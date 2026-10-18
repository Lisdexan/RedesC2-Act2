import random
import socket
import sys
import time

import slidingWindowCC as swcc
from CongestionControl import CongestionControl
from socketUDP import SocketUDP


class SocketTCP:
    SEQ_ACK_BYTES = 4
    HEADER_SIZE = 1 + SEQ_ACK_BYTES + SEQ_ACK_BYTES  # flags + seq + ack

    MAX_DATA_SIZE = 16
    LENGTH_BYTES = 4
    BUFFER_SIZE = HEADER_SIZE + MAX_DATA_SIZE

    SYN_FLAG = 0b001
    ACK_FLAG = 0b010
    FIN_FLAG = 0b100

    TIMEOUT = 2  # con netem de 0.5s de delay, 2s alcanza
    DEBUG = False

    GBN_WINDOW_SIZE = 8

    CC_MSS = 8  # tambien es el tamaño de los trozos en go back n
    CC_DEBUG = False

    HANDSHAKE_TRIES = 3
    HANDSHAKE_GRACE = 1.5

    # seq cuenta bytes. connect: x -> x+1 -> x+2. datos: ACK trae seq + n.
    # ack_num no se usa

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(SocketTCP.TIMEOUT)

        self.dest_address = None
        self._accepted = set()  # SYN ya atendidos, para no crear conexiones de mas
        self.seq_num = 0

        # estado de recv
        self.msg_remaining = 0
        self.leftover = b""

        # parte 3
        self.congestion_controler = CongestionControl(SocketTCP.CC_MSS)
        self.congestion_control_on = False

    def set_congestion_control(self, on):
        self.congestion_control_on = bool(on)

    def _cc_log(self, event, data_window):
        if not (SocketTCP.CC_DEBUG and self.congestion_control_on):
            return
        cc = self.congestion_controler
        seqs = [data_window.get_sequence_number(i) for i in range(data_window.window_size)
                if data_window.get_data(i) is not None]
        if len(seqs) > 8:
            seqs = f"{seqs[:3]} ... {seqs[-2:]} ({len(seqs)} elementos)"
        print(f"[cc] {event:<14} estado={cc.current_state:<20} cwnd={cc.get_cwnd():7.2f} "
              f"({cc.get_MSS_in_cwnd()} MSS)  ssthresh={cc.get_ssthresh()}  "
              f"window_size={data_window.window_size}  ventana(seq)={seqs}", file=sys.stderr)

    @staticmethod
    def _log(*args):
        if SocketTCP.DEBUG:
            print("[debug]", *args, file=sys.stderr)

    @staticmethod
    def create_segment(seq_num, ack_num=0, syn=False, ack=False, fin=False, data=b""):
        flags = 0
        if syn:
            flags |= SocketTCP.SYN_FLAG
        if ack:
            flags |= SocketTCP.ACK_FLAG
        if fin:
            flags |= SocketTCP.FIN_FLAG

        header = (
            flags.to_bytes(1, "big")
            + seq_num.to_bytes(SocketTCP.SEQ_ACK_BYTES, "big")
            + ack_num.to_bytes(SocketTCP.SEQ_ACK_BYTES, "big")
        )
        return header + data

    @staticmethod
    def parse_segment(segment):
        flags = segment[0]
        seq_num = int.from_bytes(segment[1:5], "big")
        ack_num = int.from_bytes(segment[5:9], "big")
        data = segment[SocketTCP.HEADER_SIZE:]

        return {
            "syn": bool(flags & SocketTCP.SYN_FLAG),
            "ack": bool(flags & SocketTCP.ACK_FLAG),
            "fin": bool(flags & SocketTCP.FIN_FLAG),
            "seq_num": seq_num,
            "ack_num": ack_num,
            "data": data,
        }

    # bind / connect / accept
    def bind(self, address):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(address)

    def connect(self, address):
        x = random.randint(0, 100)
        syn = SocketTCP.create_segment(seq_num=x, syn=True)

        # SYN, esperar SYN+ACK
        new_address = None
        for attempt in range(1, SocketTCP.HANDSHAKE_TRIES + 1):
            self.sock.sendto(syn, address)
            try:
                segment, addr = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                SocketTCP._log(f"connect: timeout esperando SYN+ACK (intento {attempt}), reenvío SYN")
                continue
            info = SocketTCP.parse_segment(segment)
            if info["syn"] and info["ack"] and info["seq_num"] == x + 1:
                new_address = addr
                break
        if new_address is None:
            raise ConnectionError("connect: no hubo respuesta del servidor (SYN+ACK)")

        self.dest_address = new_address
        self.seq_num = x + 2

        # ACK final
        ack = SocketTCP.create_segment(seq_num=self.seq_num, ack=True)
        self.sock.sendto(ack, self.dest_address)

        # si se pierde el ACK el server reenvia el SYN+ACK, asi que espero un poco
        # mas que un timeout y reenvio el ACK cada vez que llegue uno repetido
        self.sock.settimeout(SocketTCP.TIMEOUT * SocketTCP.HANDSHAKE_GRACE)
        while True:
            try:
                segment, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                break
            info = SocketTCP.parse_segment(segment)
            if info["syn"] and info["ack"] and info["seq_num"] == x + 1:
                SocketTCP._log("connect: SYN+ACK repetido (se perdió mi ACK), reenvío ACK")
                self.sock.sendto(ack, self.dest_address)
        self.sock.settimeout(SocketTCP.TIMEOUT)

    def accept(self):
        # espera un SYN nuevo sin limite
        while True:
            try:
                segment, client_address = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                continue
            info = SocketTCP.parse_segment(segment)
            if info["syn"] and (client_address, info["seq_num"]) not in self._accepted:
                break
        x = info["seq_num"]
        self._accepted.add((client_address, x))

        new_conn = SocketTCP()
        new_conn.bind((self.sock.getsockname()[0], 0))
        new_address = new_conn.sock.getsockname()
        new_conn.dest_address = client_address

        # SYN+ACK, esperar ACK
        syn_ack = SocketTCP.create_segment(seq_num=x + 1, syn=True, ack=True)
        for attempt in range(1, SocketTCP.HANDSHAKE_TRIES + 1):
            new_conn.sock.sendto(syn_ack, client_address)
            try:
                segment, _ = new_conn.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                SocketTCP._log(f"accept: timeout esperando ACK (intento {attempt}), reenvío SYN+ACK")
                continue
            info2 = SocketTCP.parse_segment(segment)
            if info2["ack"] and not info2["syn"] and info2["seq_num"] == x + 2:
                new_conn.seq_num = x + 2
                return new_conn, new_address

        new_conn.sock.close()
        raise ConnectionError("accept: no llegó el ACK final del handshake")

    # send / recv
    def send(self, message, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            self.send_using_stop_and_wait(message)
        elif mode == "go_back_n":
            self.send_using_go_back_n(message)

    def recv(self, buff_size, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            return self.recv_using_stop_and_wait(buff_size)
        elif mode == "go_back_n":
            return self.recv_using_go_back_n(buff_size)

    # Stop & Wait
    def _send_and_wait_ack(self, data):
        segment = SocketTCP.create_segment(seq_num=self.seq_num, data=data)
        expected_ack = self.seq_num + len(data)

        while True:
            self.sock.sendto(segment, self.dest_address)
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                SocketTCP._log(f"timeout esperando ACK seq={expected_ack}, retransmito seq={self.seq_num}")
                continue

            info = SocketTCP.parse_segment(raw)
            if info["ack"] and info["seq_num"] == expected_ack:
                self.seq_num = expected_ack
                return
            SocketTCP._log(f"ACK inesperado seq={info['seq_num']} (esperaba {expected_ack}), ignoro")

    def send_using_stop_and_wait(self, message):
        # primero el largo, despues los trozos
        self._send_and_wait_ack(len(message).to_bytes(SocketTCP.LENGTH_BYTES, "big"))

        for i in range(0, len(message), SocketTCP.MAX_DATA_SIZE):
            self._send_and_wait_ack(message[i:i + SocketTCP.MAX_DATA_SIZE])

    def _send_ack(self):
        self.sock.sendto(SocketTCP.create_segment(seq_num=self.seq_num, ack=True), self.dest_address)

    def _recv_segment_data(self):
        while True:
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                continue

            info = SocketTCP.parse_segment(raw)
            if info["syn"] or info["fin"] or info["ack"] or len(info["data"]) == 0:
                continue

            if info["seq_num"] == self.seq_num:
                self.seq_num += len(info["data"])
                self._send_ack()
                return info["data"]

            # duplicado: reenvio el ACK sin entregar los datos otra vez
            SocketTCP._log(f"duplicado seq={info['seq_num']} (espero {self.seq_num}), reenvío ACK")
            self._send_ack()

    def recv_using_stop_and_wait(self, buff_size):
        # mensaje nuevo: el primer segmento trae el largo
        if self.msg_remaining == 0 and len(self.leftover) == 0:
            self.msg_remaining = int.from_bytes(self._recv_segment_data(), "big")

        target = min(buff_size, len(self.leftover) + self.msg_remaining)

        while len(self.leftover) < target:
            data = self._recv_segment_data()
            self.leftover += data
            self.msg_remaining -= len(data)

        # lo que sobra queda para el proximo recv
        out, self.leftover = self.leftover[:target], self.leftover[target:]
        return out

    # Go-Back N
    def send_using_go_back_n(self, message):
        cc = self.congestion_controler
        use_cc = self.congestion_control_on

        data_list = [len(message).to_bytes(SocketTCP.LENGTH_BYTES, "big")] + \
                    [message[i:i + cc.MSS] for i in range(0, len(message), cc.MSS)]
        final_seq = self.seq_num + sum(len(d) for d in data_list)

        window_size = cc.get_MSS_in_cwnd() if use_cc else SocketTCP.GBN_WINDOW_SIZE
        data_window = swcc.SlidingWindowCC(window_size, data_list, self.seq_num)
        self._cc_log("inicio", data_window)

        # reuso el socket dentro de un SocketUDP para tener los timers
        udp = SocketUDP()
        udp.socket_udp.close()
        udp.socket_udp = self.sock
        self.sock.setblocking(False)
        udp.settimeout(SocketTCP.TIMEOUT)

        def send_element(i):
            data = data_window.get_data(i)
            if data is None:
                return
            seg = SocketTCP.create_segment(seq_num=data_window.get_sequence_number(i), data=data)
            udp.sendto(seg, self.dest_address, timer_index=0)

        try:
            for i in range(window_size):
                send_element(i)

            while data_window.get_data(0) is not None:
                try:
                    raw, _ = udp.recvfrom(SocketTCP.BUFFER_SIZE)
                except TimeoutError:
                    SocketTCP._log(f"timeout, reenvío ventana desde seq={data_window.get_sequence_number(0)}")
                    udp.stop_timer(0)  # un timer vencido no se puede reusar
                    if use_cc:
                        cc.event_timeout()
                        window_size = cc.get_MSS_in_cwnd()
                        data_window.update_window_size(window_size)
                        self._cc_log("TIMEOUT", data_window)
                    for i in range(window_size):
                        send_element(i)
                    continue

                info = SocketTCP.parse_segment(raw)
                if not info["ack"]:
                    continue

                # ACK acumulativo. si la ventana se achico justo antes, el ACK puede
                # pasarse de la ventana, por eso el while externo
                old_size = window_size
                moved = 0
                while True:
                    k = 0
                    while (k < window_size and data_window.get_data(k) is not None and
                           data_window.get_sequence_number(k) + len(data_window.get_data(k)) <= info["seq_num"]):
                        k += 1
                    if k == 0:
                        break
                    data_window.move_window(k)
                    moved += k
                if moved == 0:
                    continue  # ACK repetido o viejo

                if use_cc:
                    cc.event_ack_received()
                    window_size = cc.get_MSS_in_cwnd()
                    data_window.update_window_size(window_size)
                    self._cc_log("ACK", data_window)

                # elementos que entraron a la ventana (por avanzar o por crecer)
                new_idx = [i for i in range(max(0, old_size - moved), window_size)
                           if data_window.get_data(i) is not None]

                # si hay elementos nuevos el sendto parte un timer nuevo,
                # si no, dejo correr el que ya estaba
                if new_idx or data_window.get_data(0) is None:
                    udp.stop_timer(0)
                for i in new_idx:
                    send_element(i)
        finally:
            if udp.timer_list[0] is not None:
                udp.stop_timer(0)
            self.sock.settimeout(SocketTCP.TIMEOUT)

        self.seq_num = final_seq

    def recv_using_go_back_n(self, buff_size):
        # el receptor de stop & wait sirve igual
        return self.recv_using_stop_and_wait(buff_size)

    # close / recv_close
    def close(self):
        s = self.seq_num
        fin = SocketTCP.create_segment(seq_num=s, fin=True)

        # FIN, esperar FIN+ACK (3 intentos)
        got_fin_ack = False
        for attempt in range(1, 4):
            self.sock.sendto(fin, self.dest_address)
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                SocketTCP._log(f"close: timeout {attempt}/3 esperando FIN+ACK, reenvío FIN")
                continue
            info = SocketTCP.parse_segment(raw)
            if info["fin"] and info["ack"] and info["seq_num"] == s + 1:
                got_fin_ack = True
                break
            SocketTCP._log("close: segmento inesperado, lo ignoro")

        if got_fin_ack:
            # ACK final, 3 veces con un timeout entre cada una
            self.seq_num = s + 2
            ack = SocketTCP.create_segment(seq_num=self.seq_num, ack=True)
            for k in range(3):
                try:
                    self.sock.sendto(ack, self.dest_address)
                except OSError:
                    pass
                if k < 2:
                    time.sleep(SocketTCP.TIMEOUT)
        else:
            SocketTCP._log("close: sin respuesta tras 3 timeouts, asumo que la contraparte cerró")

        self.sock.close()

    def recv_close(self):
        # esperar FIN
        while True:
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                continue
            info = SocketTCP.parse_segment(raw)
            if info["fin"] and info["seq_num"] == self.seq_num:
                break
            if info["data"]:
                # dato retransmitido, se perdio mi ACK
                SocketTCP._log("recv_close: dato duplicado antes del FIN, reenvío ACK")
                self._send_ack()

        s = self.seq_num

        fin_ack = SocketTCP.create_segment(seq_num=s + 1, fin=True, ack=True)
        self.sock.sendto(fin_ack, self.dest_address)

        # esperar el ACK final (hasta 3 timeouts)
        timeouts = 0
        while timeouts < 3:
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                timeouts += 1
                SocketTCP._log(f"recv_close: timeout {timeouts}/3 esperando ACK final")
                continue
            info = SocketTCP.parse_segment(raw)
            if info["ack"] and not info["fin"] and info["seq_num"] == s + 2:
                break
            if info["fin"] and not info["ack"] and info["seq_num"] == s:
                SocketTCP._log("recv_close: FIN repetido (se perdió mi FIN+ACK), lo reenvío")
                self.sock.sendto(fin_ack, self.dest_address)
        else:
            SocketTCP._log("recv_close: sin ACK tras 3 timeouts, asumo que la contraparte cerró")

        self.seq_num = s + 2
        self.sock.close()
