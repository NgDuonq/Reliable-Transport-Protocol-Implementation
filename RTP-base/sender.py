import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum


TIMEOUT = 0.5
MAX_PAYLOAD = 1456


def valid_ack(pkt):
    if len(pkt) < 16:
        return None

    ack = PacketHeader(pkt[:16])

    received_checksum = ack.checksum
    ack.checksum = 0

    computed_checksum = compute_checksum(ack)

    if received_checksum != computed_checksum:
        return None

    if ack.type != 3:
        return None

    return ack


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_address = (receiver_ip, receiver_port)

    # ===== START =====
    start = PacketHeader(
        type=0,
        seq_num=0,
        length=0,
        checksum=0
    )

    start.checksum = compute_checksum(start)
    s.sendto(bytes(start), receiver_address)

    # START ACK does not need loss/delay handling per spec
    pkt, address = s.recvfrom(2048)
    ack = valid_ack(pkt)

    if ack is not None:
        print("received START ACK:", ack.seq_num)

    # ===== DATA =====
    base = 1
    next_seq = 1

    packets = {}
    eof = False

    # Time at which current window started / last advanced
    timer_start = None

    while not eof or base < next_seq:

        sent_new_packet = False

        # Fill available space in window
        while not eof and next_seq < base + window_size:
            msg = sys.stdin.buffer.read(MAX_PAYLOAD)

            if not msg:
                eof = True
                break

            data_header = PacketHeader(
                type=2,
                seq_num=next_seq,
                length=len(msg),
                checksum=0
            )

            data_header.checksum = compute_checksum(
                data_header / msg
            )

            raw_packet = bytes(data_header / msg)
            packets[next_seq] = raw_packet

            s.sendto(
                raw_packet,
                receiver_address
            )

            print("sent DATA:", next_seq)

            next_seq += 1
            sent_new_packet = True

        if eof and base == next_seq:
            break

        # Start timer when first outstanding window is sent
        if timer_start is None and base < next_seq:
            timer_start = time.monotonic()

        # Determine how much of the 500 ms remains
        elapsed = time.monotonic() - timer_start
        remaining = TIMEOUT - elapsed

        # Timer already expired
        if remaining <= 0:
            print(
                "timeout, retransmitting window:",
                base,
                "to",
                next_seq - 1
            )

            for seq in range(base, next_seq):
                s.sendto(
                    packets[seq],
                    receiver_address
                )

            timer_start = time.monotonic()
            continue

        s.settimeout(remaining)

        try:
            pkt, address = s.recvfrom(2048)
            ack = valid_ack(pkt)

            if ack is None:
                continue

            ack_num = ack.seq_num
            print("received ACK:", ack_num)

            # Cumulative ACK advances the window only if
            # it acknowledges something new.
            if base < ack_num <= next_seq:
                for seq in range(base, ack_num):
                    packets.pop(seq, None)

                base = ack_num

                # Window actually advanced -> reset timer
                if base < next_seq:
                    timer_start = time.monotonic()
                else:
                    timer_start = None

            # Duplicate / stale ACK:
            # do NOT reset timer

        except socket.timeout:
            print(
                "timeout, retransmitting window:",
                base,
                "to",
                next_seq - 1
            )

            for seq in range(base, next_seq):
                s.sendto(
                    packets[seq],
                    receiver_address
                )

            timer_start = time.monotonic()

    # ===== END =====
    end_seq = next_seq

    end_header = PacketHeader(
        type=1,
        seq_num=end_seq,
        length=0,
        checksum=0
    )

    end_header.checksum = compute_checksum(end_header)

    s.sendto(
        bytes(end_header),
        receiver_address
    )

    # END must wait at most 500 ms
    s.settimeout(TIMEOUT)

    try:
        pkt, address = s.recvfrom(2048)
        ack = valid_ack(pkt)

        if (
            ack is not None
            and ack.seq_num == end_seq + 1
        ):
            print("received END ACK:", ack.seq_num)

    except socket.timeout:
        print("END ACK timeout, exiting")

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

    sender(
        args.receiver_ip,
        args.receiver_port,
        args.window_size
    )


if __name__ == "__main__":
    main()
