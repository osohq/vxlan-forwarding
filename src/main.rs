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

type ConnectionKey = (u16, u16);
type ConnectionMap = DashMap<ConnectionKey, Connection>;

const MAX_PENDING_PACKETS: &str = "MAX_PENDING_PACKETS";
const MAX_PENDING_PACKETS_DEFAULT: usize = 1;
const MAX_RETRIES: &str = "MAX_RETRIES";
const MAX_RETRIES_DEFAULT: usize = 2;

struct Connection {
    stream: TcpStream,
    closed: bool,
    packets: BTreeMap<u32, Vec<u8>>,
    next_seq: u32,
    fin_seq: Option<u32>,
    max_pending_packets: usize,
    max_retries: usize,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            closed: false,
            packets: Default::default(),
            next_seq: 1,
            fin_seq: None,
            max_pending_packets: std::env::var(MAX_PENDING_PACKETS)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(MAX_PENDING_PACKETS_DEFAULT),
            max_retries: std::env::var(MAX_RETRIES)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(MAX_RETRIES_DEFAULT),
        }
    }

    fn set_seq(&mut self, seq: u32) {
        self.next_seq = seq;
        self.check_pending();
    }

    // NOTE(sverch): This has no delay when it's retrying, it just tries to
    // send as hard as possible until the data goes through. We are using a
    // non blocking TCP connection, so this is what we need to do for the case
    // where we would have to block. Since this is per connection, no one else
    // should be blocked by this, but this does consume extra resources. The
    // retries are tunable via an environment variable.
    fn write_all_with_retries(&mut self, data: &[u8]) -> std::io::Result<()> {
        let mut remaining_retries = self.max_retries;
        loop {
            match self.stream.write_all(data) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if remaining_retries == 0 {
                        tracing::error!(%e, "Failed to write data, no retries remaining");
                        return Err(e);
                    }
                    tracing::error!(%e, %remaining_retries, "Failed to write data, retrying");
                    remaining_retries -= 1;
                    continue;
                }
                r => {
                    return r;
                }
            }
        }
    }

    fn push_data(&mut self, seq: u32, data: &[u8]) {
        if seq == self.next_seq {
            // eagerly send the data
            tracing::trace!(%seq, "this is already the next packet");
            if let Err(e) = self.write_all_with_retries(data) {
                // just close the connection and abandon it
                self.closed = true;
                tracing::error!(%e, "Failed to eagerly write data");
                return;
            };
            // Every byte has its own sequence number, so bump this by the
            // length of the data we just sent.
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

        if self.packets.len() > self.max_pending_packets {
            tracing::debug!("More than the configured maximum packets are pending -- going to start sending data");
            self.next_seq = self.packets.keys().next().copied().unwrap_or(self.next_seq);
        }

        while let Some(data) = self.packets.remove(&self.next_seq) {
            tracing::trace!(data=%String::from_utf8_lossy(&data), "Sending data");
            if let Err(e) = self.write_all_with_retries(&data) {
                // just close the connection and abandon it
                self.closed = true;
                tracing::error!(%e, "Failed to write buffered data");
                return;
            };
            // Every byte has its own sequence number, so bump this by the
            // length of the data we just sent.
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

struct ConnectionManager {
    conn_map: ConnectionMap,
    // pool: r2d2::Pool<TcpConnector>,
    events: Sender<(ConnectionKey, bool)>,
    addr: SocketAddr,
}

impl ConnectionManager {
    fn new(forward_addr: SocketAddr, events: Sender<(ConnectionKey, bool)>) -> Self {
        let conn_map: ConnectionMap = ConnectionMap::with_capacity_and_shard_amount(512, 32);
        // let pool = r2d2::Pool::builder()
        //     .max_size(128)
        //     .min_idle(Some(4))
        //     .connection_timeout(Duration::from_secs(30))
        //     .build(TcpConnector {
        //         addr: forward_addr.clone(),
        //     })
        //     .expect("Failed to create connection pool");
        Self {
            // pool,
            conn_map,
            events,
            addr: forward_addr,
        }
    }

    fn get_or_insert_connection(
        &self,
        key: ConnectionKey,
    ) -> Result<dashmap::mapref::one::RefMut<'_, ConnectionKey, Connection>, ()> {
        let mut res = Err(());

        // check if the connection is live
        self.check_connection(&key);

        for _ in 0..3 {
            res = self.conn_map.entry(key).or_try_insert_with(|| {
                let start = std::time::Instant::now();
                tracing::trace!(?key, "Initialize new connection");
                let stream = TcpStream::connect(self.addr).map_err(|e| {
                    tracing::error!(%e, "Failed to connect to forward address");
                })?;
                stream.set_nonblocking(true).map_err(|e| {
                    tracing::error!(%e, "Failed to set non-blocking mode");
                })?;
                stream.set_nodelay(true).map_err(|e| {
                    tracing::error!(%e, "Failed to set no delay");
                })?;
                tracing::debug!(duration_ms=%start.elapsed().as_millis(), "Add new connection");
                if let Err(e) = self.events.send((key, false)) {
                    tracing::debug!(%e, "unexpected send error");
                }
                Ok(Connection::new(stream))
                // tracing::trace!(?key, "Getting connection from pool");
                // self.pool
                // .get()
                // .map_err(|e| {
                //     tracing::error!(%e, "Failed to get connection from pool");
                // })
                // .map(|c| {
                //     tracing::debug!(duration_ms=%start.elapsed().as_millis(), "Got connection from pool");
                //     let _res = self.events.send((key, false));
                //     c
                // })
            });

            if res.is_ok() {
                break;
            }
        }
        res
    }

    fn check_connection(&self, key: &ConnectionKey) {
        let Some(mut conn) = self.conn_map.get_mut(key) else {
            return;
        };

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
                conn.closed = true;
            }
        }

        if conn.closed {
            drop(conn);
            self.finished(key);
        }
    }

    fn finished(&self, key: &ConnectionKey) {
        if self.conn_map.remove(key).is_some() {
            let _res = self.events.send((*key, true));
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = std::env::args().collect::<Vec<String>>();

    let [_, interface, expected_vni, original_destination_port, forward_addr] = &args[..] else {
        println!(
            "Usage: {} <interface> <vni> <original_destination_port> <forward_addr>",
            args[0]
        );
        exit(1);
    };
    // let [_, interface, original_destination_port, forward_addr] =
    //     ["packetdump", "dummy0", "8000", "127.0.0.1:8081"];
    let original_destination_port: u16 = original_destination_port
        .parse()
        .expect("failed to parse original destination port");
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
    let num_threads = std::env::var("NUM_THREADS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(num_cpus::get);

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
                    tracing::info!("Check connection liveness: {:?}", key);
                    cm.check_connection(key);
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
                    Err(e) => panic!("packetdump: unable to create channel: {e}"),
                };

                tracing::info_span!("thread", %i).in_scope(|| loop {
                    match rx.next() {
                        Ok(packet) => {
                            forward_packet(
                                &conn_manager,
                                original_destination_port,
                                expected_vni,
                                packet,
                            );
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
    original_destination_port: u16,
    expected_vni: u32,
    packet: &[u8],
) -> Option<()> {
    // First, unwrap the packet. See
    // https://docs.aws.amazon.com/vpc/latest/mirroring/traffic-mirroring-packet-formats.html
    // for the packet format.

    // AWS Traffic Mirroring Sends us this information as a UDP stream over
    // ethernet, and we are using a raw socket so we have to unwrap
    // everything. First, unwrap Ethernet, IP, and UDP, to get to the VXLAN
    // packet.
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

    // From:
    // https://docs.aws.amazon.com/vpc/latest/mirroring/traffic-mirroring-packet-formats.html
    //
    // > Source port — The port is determined by a 5-tuple hash of the
    // > original L2 packet, for ICMP, TCP, and UDP flows. ...
    //
    // So this means that this port number is actually the result of hashing
    // the following fields:
    //
    // ```
    // (
    //  tcp_source_port,
    //  tcp_source_ip,
    //  tcp_destination_port,
    //  tcp_destination_ip,
    //  protocol_number
    // )
    // ```
    //
    // This seems to be a convention in VXLAN, since the fact that UDP is a
    // connectionless protocol means the receiver is often not using this
    // value, so someone decided to put this extra information in here to
    // distinguish different traffic flows. It's useful for us to separate the
    // original client connections, so fetch that value here.
    let udp_source = udp.get_source();

    // Unwrap the VXLAN packet. This represents a virtual LAN, which
    // effectively means from this point on up this is a "normal" ethernet
    // stream, except the traffic is a copy of the traffic that was received
    // on the original interface.
    let Some(vxlan) = VxlanPacket::new(udp.payload()) else {
        tracing::debug!("Not a VXLAN packet");
        return None;
    };

    // VXLAN can be used to create many virtual networks using the same
    // physical network devices, and the VNI is used to distinguish them. We
    // must set a VNI when we create the traffic mirror session, which will
    // cause the packets that are sent over the mirrored connection to have
    // that VNI. Skip any packets with an unexpected VNI.
    let vni = vxlan.get_vni();
    if vni != expected_vni {
        // NOTE(sverch): Making this a warning, because this was likely a
        // misconfiguration based on how we use this today.
        tracing::warn!(%vni, "Received vxlan packet with unexpected VNI");
        return None;
    }

    // Unwrap the original packet
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
    if tcp.get_destination() != original_destination_port {
        tracing::trace!("Not a packet for port {original_destination_port}");
        return None;
    }

    // Get the tcp source port, for extra specificity on the connection.
    // NOTE(sverch): This might have been here originally out of confusion as
    // to which "source port" was a result of the 5 tuple hash, but it doesn't
    // seem worth taking it out right now in case that breaks something.
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
        tracing::trace!(payload=%String::from_utf8_lossy(payload), "Forwarding payload");
        conn.push_data(tcp.get_sequence(), payload);
    }

    Some(())
}
