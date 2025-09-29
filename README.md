# VXLAN To HTTP Forwarder

This service can receive mirror traffic over a VXLAN and forward all traffic
to a given HTTP endpoint.

This is specifically used for [AWS Traffic
Mirroring](https://docs.aws.amazon.com/vpc/latest/mirroring/traffic-mirroring-packet-formats.html),
which sends us packets in this format. When you configure the traffic mirror
session, make sure the VNI matches the VNI that you pass to this service.

## Usage

```txt
Usage: vxlan-to-http <interface> <vni> <original_destination_port> <forward_addr>
```

Listens on a given network `<interface>` for mirror traffic. Only traffic
traffic with the given `<vni>` and sent to `<original_destination_port>` on
the original service that this is mirrored from will be forwarded (so
`<original_destination_port>` for example may be the port your service is
listening on behind the load balancer). Traffic will be forwarded to
`<forward_addr>`.

Set `MAX_PENDING_PACKETS` to configure how many packets to buffer when we
receive packets out of order. Since the mirror transfer protocol is UDP, we
have no way to get missing data if packets are dropped, so this probably
shouldn't be very high.

Set `MAX_RETRIES` to configure how many times to retry on `EWOULDBLOCK` before
giving up, since we use a non blocking TCP connection.
