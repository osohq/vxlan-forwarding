use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    process::exit,
    sync::{
        mpsc::{RecvTimeoutError, Sender},
        Arc,
    },
    thread,
    time::Duration,
};

use dashmap::DashMap;
use pnet::{
    datalink::{self, interfaces, Channel, FanoutOption},
    packet::{
        ethernet::{EtherTypes, EthernetPacket},
        ipv4::Ipv4Packet,
        tcp::{TcpFlags, TcpPacket},
        udp::UdpPacket,
        vxlan::VxlanPacket,
        Packet,
    },
};
use r2d2::PooledConnection;

type ConnectionKey = (u16, u16);
type ConnectionMap = DashMap<ConnectionKey, r2d2::PooledConnection<TcpConnector>>;

struct Connection {
    stream: TcpStream,
    closed: bool,
    packets: BTreeMap<u32, Vec<u8>>,
    next_seq: u32,
    fin_seq: Option<u32>,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            closed: false,
            packets: Default::default(),
            next_seq: 1,
            fin_seq: None,
        }
    }

    fn set_seq(&mut self, seq: u32) {
        self.next_seq = seq;
        self.check_pending();
    }

    fn push_data(&mut self, seq: u32, data: &[u8]) {
        if seq == self.next_seq {
            // eagerly send the data
            tracing::trace!(%seq, "this is already the next packet");
            if let Err(e) = self.stream.write_all(data) {
                // just close the connection and abandon it
                self.closed = true;
                tracing::error!(%e, "Failed to write data");
                return;
            };
            self.next_seq += data.len() as u32;
        } else {
            tracing::trace!(%seq, "Storing data for later");
            self.packets.insert(seq, data.to_vec());
        }
        self.check_pending();
    }

    fn set_fin(&mut self, seq: u32) {
        self.fin_seq = Some(seq);
        self.check_pending();
    }

    fn check_pending(&mut self) {
        tracing::trace!(?self.packets, %self.next_seq, "Checking pending data");
        while let Some(data) = self.packets.remove(&self.next_seq) {
            tracing::trace!(data=%String::from_utf8_lossy(&data), "Sending data");
            if let Err(e) = self.stream.write_all(&data) {
                // just close the connection and abandon it
                self.closed = true;
                tracing::error!(%e, "Failed to write data");
                return;
            };
            self.next_seq += data.len() as u32;
        }
        if let Some(fin) = self.fin_seq {
            if fin == self.next_seq {
                tracing::trace!("finished sending data");
                // self.closed = true;
            }
        }
    }
}

struct TcpConnector {
    addr: SocketAddr,
}

impl r2d2::ManageConnection for TcpConnector {
    type Connection = Connection;

    type Error = std::io::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let start = std::time::Instant::now();
        let stream = TcpStream::connect(self.addr)?;
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        tracing::debug!(duration_ms=%start.elapsed().as_millis(), "Add new connection");
        Ok(Connection::new(stream))
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let mut buf = [0u8; 1];
        match conn.stream.peek(&mut buf) {
            Ok(0) => {
                // connection closed
                conn.closed = true;
                tracing::debug!("Connection closed");
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "Connection closed",
                ))
            }
            Ok(n) => {
                tracing::trace!(?n, "Read bytes from connection");
                Ok(())
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // no data to read
                tracing::trace!("No data to read");
                Ok(())
            }
            Err(e) => {
                // failed to read from connection
                tracing::error!(%e, "Failed to read from connection");
                Err(e)
            }
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.closed
    }
}

struct ConnectionManager {
    conn_map: ConnectionMap,
    pool: r2d2::Pool<TcpConnector>,
    events: Sender<(ConnectionKey, bool)>,
}

impl ConnectionManager {
    fn new(forward_addr: SocketAddr, events: Sender<(ConnectionKey, bool)>) -> Self {
        let conn_map: ConnectionMap = ConnectionMap::with_capacity_and_shard_amount(512, 32);
        let pool = r2d2::Pool::builder()
            .max_size(128)
            .min_idle(Some(4))
            .connection_timeout(Duration::from_secs(30))
            .build(TcpConnector { addr: forward_addr })
            .expect("Failed to create connection pool");
        Self {
            pool,
            conn_map,
            events,
        }
    }

    fn get_or_insert_connection(
        &self,
        key: ConnectionKey,
    ) -> Result<dashmap::mapref::one::RefMut<'_, ConnectionKey, PooledConnection<TcpConnector>>, ()>
    {
        let mut res = Err(());

        for _ in 0..3 {
            res = self.conn_map
            .entry(key)
            .or_try_insert_with(|| {
                let start = std::time::Instant::now();
                tracing::trace!(?key, "Getting connection from pool");
                self.pool
                .get()
                .map_err(|e| {
                    tracing::error!(%e, "Failed to get connection from pool");
                })
                .map(|c| {
                    tracing::debug!(duration_ms=%start.elapsed().as_millis(), "Got connection from pool");
                    let _res = self.events.send((key, false));
                    c
                })
            });
            if res.is_ok() {
                break;
            }
        }
        res
    }

    fn finished(&self, key: &ConnectionKey) {
        if let Some(_) = self.conn_map.remove(key) {
            let _res = self.events.send((*key, true));
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = std::env::args().collect::<Vec<String>>();

    let [_, interface, expected_vni, source_port, forward_addr] = &args[..] else {
        println!(
            "Usage: {} <interface> <vni> <source_port> <forward_addr>",
            args[0]
        );
        exit(1);
    };
    // let [_, interface, source_port, forward_addr] =
    //     ["packetdump", "dummy0", "8000", "127.0.0.1:8081"];
    let source_port: u16 = source_port.parse().expect("failed to parse source port");
    let expected_vni: u32 = expected_vni.parse().expect("failed to parse VNI");

    let forward_addr = forward_addr
        .to_socket_addrs()
        .expect("failed to parse address")
        .next()
        .expect("did not find any address");

    // Get a vector with all network interfaces found
    let all_interfaces = interfaces();

    // Search for the default interface - the one that is
    // up, not loopback and has an IP.
    let interface = all_interfaces.iter().find(|e| {
        tracing::trace!("Interface: {:?}", e);

        &e.name == interface
    });

    let interface = match interface {
        Some(interface) => {
            tracing::info!("Found interface with [{}].", interface.name);

            interface
        }
        None => {
            tracing::error!("Could not find the specified interface.");

            exit(1);
        }
    };

    if !interface.is_up() {
        println!("The interface is down.");
        exit(1);
    }

    tracing::info!("Starting up...");
    let num_threads = num_cpus::get();

    let (tx, rx) = std::sync::mpsc::channel();

    let conn_manager = Arc::new(ConnectionManager::new(forward_addr, tx));

    let channel_config = datalink::Config {
        write_buffer_size: 8096,
        read_buffer_size: 8096,
        channel_type: datalink::ChannelType::Layer2,
        // See: https://man7.org/linux/man-pages/man7/packet.7.html
        linux_fanout: Some(FanoutOption {
            group_id: 123,
            fanout_type: datalink::FanoutType::LB,
            defrag: true,
            rollover: true,
        }),
        ..Default::default()
    };

    let mut threads = vec![];

    let cm = conn_manager.clone();
    let event_thread = thread::spawn(move || {
        let mut connections = BTreeMap::<ConnectionKey, std::time::Instant>::new();
        loop {
            match rx.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok((key, finished)) => {
                    if finished {
                        tracing::info!("{key:?} finished");
                        if let Some(end) = connections.remove(&key) {
                            tracing::info!(duration_ms=%end.elapsed().as_millis(), "{key:?} removed");
                        }
                    } else {
                        tracing::info!("{key:?} started");
                        connections.insert(key, std::time::Instant::now());
                    }
                    continue;
                }
                Err(e) => {
                    if e != RecvTimeoutError::Timeout {
                        tracing::error!(%e, "Failed to receive event");
                    }
                }
            }

            let now = std::time::Instant::now();
            for (key, start) in connections.iter() {
                if now.duration_since(*start) > Duration::from_secs(3) {
                    tracing::info!("Connection timed out: {:?}", key);
                    cm.finished(key);
                }
            }
        }
    });

    threads.push(event_thread);

    // see: https://github.com/libpnet/libpnet/blob/main/examples/fanout.rs
    for i in 0..num_threads {
        let interface = interface.clone();
        let conn_manager = conn_manager.clone();

        let thread = thread::Builder::new()
            .name(format!("thread{i}"))
            .spawn(move || {
                // Create a channel to receive on
                let (_, mut rx) = match datalink::channel(&interface, channel_config) {
                    Ok(Channel::Ethernet(tx, rx)) => (tx, rx),
                    Ok(_) => panic!("packetdump: unhandled channel type"),
                    Err(e) => panic!("packetdump: unable to create channel: {}", e),
                };

                tracing::info_span!("thread", %i).in_scope(|| loop {
                    match rx.next() {
                        Ok(packet) => {
                            forward_packet(&conn_manager, source_port, expected_vni, packet);
                        }
                        Err(e) => {
                            tracing::error!("An error occurred while reading: {e}");
                        }
                    }
                });
            })
            .unwrap();
        tracing::info!("Spawned thread {i}");
        threads.push(thread);
        // give a break between spawning threads since dashmap seems to struggle
        std::thread::sleep(Duration::from_millis(100));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn forward_packet(
    conn_manager: &ConnectionManager,
    source_port: u16,
    expected_vni: u32,
    packet: &[u8],
) -> Option<()> {
    let packet = EthernetPacket::new(packet)?;
    if packet.get_ethertype() != EtherTypes::Ipv4 {
        tracing::trace!("not an IPv4 packet");
        return None;
    }
    let Some(ipv4) = Ipv4Packet::new(packet.payload()) else {
        tracing::error!("failed to parse IPv4 packet");
        return None;
    };
    let Some(udp) = UdpPacket::new(ipv4.payload()) else {
        tracing::debug!("Not a UDP packet");
        return None;
    };
    let udp_source = udp.get_source();
    let Some(vxlan) = VxlanPacket::new(udp.payload()) else {
        tracing::debug!("Not a VXLAN packet");
        return None;
    };
    let vni = vxlan.get_vni();
    if vni != expected_vni {
        tracing::trace!(%vni, "Invalid VNI");
        return None;
    }
    let Some(ethernet) = EthernetPacket::new(vxlan.payload()) else {
        tracing::warn!("Not an Ethernet packet");
        return None;
    };
    if ethernet.get_ethertype() != EtherTypes::Ipv4 {
        tracing::warn!("Not an IPv4 packet");
        return None;
    }
    let Some(ipv4) = Ipv4Packet::new(ethernet.payload()) else {
        tracing::warn!("Not an IPv4 packet");
        return None;
    };
    let Some(tcp) = TcpPacket::new(ipv4.payload()) else {
        tracing::warn!("Not a TCP packet");
        return None;
    };
    if tcp.get_destination() != source_port {
        tracing::trace!("Not a packet for port {source_port}");
        return None;
    }
    let tcp_source = tcp.get_source();
    let key = (udp_source, tcp_source);

    let _span = tracing::info_span!("forward_packet", ?key).entered();

    let payload = tcp.payload();

    tracing::trace!(
        // ?tcp,
        flags=%tcp.get_flags(),
        seq=%tcp.get_sequence(), payload_len = %payload.len(), "Got packet");

    // we only care about interesting packets
    // SYN (so we know where it starts)
    // FIN (so we know where it ends)
    // non-empty payload
    let skip = tcp.get_flags() & (TcpFlags::SYN | TcpFlags::FIN) == 0 && payload.is_empty();
    if skip {
        tracing::trace!("Skipping empty packet");
        return None;
    }

    let Some(mut conn) = conn_manager.get_or_insert_connection(key).ok() else {
        tracing::error!("Failed to get connection");
        return None;
    };

    if tcp.get_flags() & TcpFlags::SYN != 0 {
        tracing::debug!("Connection start");
        conn.set_seq(tcp.get_sequence() + 1);
    }

    if tcp.get_flags() & TcpFlags::FIN != 0 {
        tracing::debug!("Proxied connection finished");
        conn.set_fin(tcp.get_sequence());
    }

    if !payload.is_empty() {
        tracing::trace!(payload=%String::from_utf8_lossy(&payload), "Forwarding payload");
        conn.push_data(tcp.get_sequence(), payload);
    }

    // read as many bytes off the socket as we can
    let mut buf = Vec::new();
    match conn.stream.read_to_end(&mut buf) {
        Ok(0) => {
            // connection closed
            tracing::debug!("Connection closed");
            // signals to the pool that the connection is no longer valid
            conn.closed = true;
        }
        Ok(n) => {
            tracing::trace!(?n, "Read bytes from connection");
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // no data to read
            tracing::trace!("No data to read");
        }
        Err(e) => {
            // failed to read from connection
            tracing::error!(%e, "Failed to read from connection");
        }
    }

    if conn.closed {
        drop(conn);
        conn_manager.finished(&key);
    }

    Some(())
}
