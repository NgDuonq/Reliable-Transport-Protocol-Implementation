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
            # Ignore START during an active connection
            if connection_active:
                continue

            connection_active = True
            expected_seq = 1
            buffer.clear()

            s.sendto(
                make_ack(1),
                address
            )

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

            if received_checksum != computed_checksum:
                continue

            seq = pkt_header.seq_num

            # Packet inside receive window
            if expected_seq <= seq < expected_seq + window_size:
                if seq not in buffer:
                    buffer[seq] = msg

                # Individual ACK
                s.sendto(
                    make_ack(seq),
                    address
                )

                # Deliver contiguous packets in order
                while expected_seq in buffer:
                    sys.stdout.buffer.write(
                        buffer[expected_seq]
                    )
                    sys.stdout.buffer.flush()

                    del buffer[expected_seq]
                    expected_seq += 1

            # Duplicate old packet
            elif seq < expected_seq:
                s.sendto(
                    make_ack(seq),
                    address
                )

            # Too far ahead -> drop

        # ===== END =====
        elif pkt_header.type == 1:
            if pkt_header.seq_num == expected_seq:
                s.sendto(
                    make_ack(pkt_header.seq_num),
                    address
                )
                break

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
