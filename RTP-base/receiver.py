import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def make_ack(seq_num):
    ack = PacketHeader(
        type=3,
        seq_num=seq_num,
        length=0,
        checksum=0
    )

    ack.checksum = compute_checksum(ack)
    return bytes(ack)


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected_seq = 1
    buffer = {}
    connection_active = False

    while True:
        pkt, address = s.recvfrom(2048)

        if len(pkt) < 16:
            continue

        pkt_header = PacketHeader(pkt[:16])

        # ===== START =====
        if pkt_header.type == 0:
            # Only START a connection if one is not already active.
            # START packets received mid-connection are ignored.
            if connection_active:
                continue

            connection_active = True
            expected_seq = 1
            buffer.clear()

            s.sendto(make_ack(1), address)

        # Ignore everything except START until a connection exists.
        elif not connection_active:
            continue

        # ===== DATA =====
        elif pkt_header.type == 2:
            msg = pkt[16 : 16 + pkt_header.length]

            received_checksum = pkt_header.checksum
            pkt_header.checksum = 0

            computed_checksum = compute_checksum(
                pkt_header / msg
            )

            # Corrupted DATA -> drop, no ACK
            if received_checksum != computed_checksum:
                continue

            seq = pkt_header.seq_num

            # Packet inside receive window
            if expected_seq <= seq < expected_seq + window_size:
                if seq not in buffer:
                    buffer[seq] = msg

                # Deliver all consecutive buffered packets
                while expected_seq in buffer:
                    sys.stdout.buffer.write(
                        buffer[expected_seq]
                    )
                    sys.stdout.buffer.flush()

                    del buffer[expected_seq]
                    expected_seq += 1

            # Old duplicate packet: don't write again.
            # Packet too far ahead: drop.
            #
            # In both cases we still send cumulative ACK below.

            s.sendto(
                make_ack(expected_seq),
                address
            )

        # ===== END =====
        elif pkt_header.type == 1:
            if pkt_header.seq_num == expected_seq:
                s.sendto(
                    make_ack(expected_seq + 1),
                    address
                )
                break

            # Still missing DATA before END
            s.sendto(
                make_ack(expected_seq),
                address
            )

    s.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "receiver_ip",
        help="The IP address of the host that receiver is running on"
    )

    parser.add_argument(
        "receiver_port",
        type=int,
        help="The port number on which receiver is listening"
    )

    parser.add_argument(
        "window_size",
        type=int,
        help="Maximum number of outstanding packets"
    )

    args = parser.parse_args()

    receiver(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()
