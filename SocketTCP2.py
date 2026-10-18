import random
import socket
import sys
import time

import slidingWindowCC as swcc
from CongestionControl import CongestionControl
from socketUDP import SocketUDP


class SocketTCP:
    SEQ_ACK_BYTES = 4  # tamaño en bytes de seq_num y ack_num
    HEADER_SIZE = 1 + SEQ_ACK_BYTES + SEQ_ACK_BYTES  # flags + seq_num + ack_num = 9

    MAX_DATA_SIZE = 16
    LENGTH_BYTES = 4  # el message_length viaja como entero binario de 4 bytes
    BUFFER_SIZE = HEADER_SIZE + MAX_DATA_SIZE  # tamaño fijo del socket UDP interno

    SYN_FLAG = 0b001
    ACK_FLAG = 0b010
    FIN_FLAG = 0b100

    TIMEOUT = 2    # segundos (con netem delay 0.5s ida y vuelta = 1s, 2s alcanza)
    DEBUG = False  # ponlo en True para ver retransmisiones

    GBN_WINDOW_SIZE = 8  # ventana fija de Go-Back N (Parte 2, sin control de congestión)

    CC_MSS = 8           # MSS (bytes) del control de congestión; también el tamaño de los trozos en Go-Back N
    CC_DEBUG = False     # ponlo en True para ver cómo evoluciona la ventana de congestión (Parte 3)

    HANDSHAKE_TRIES = 3     # intentos máximos de connect / accept
    HANDSHAKE_GRACE = 1.5   # connect espera GRACE*TIMEOUT tras mandar el ACK final (ver connect)

    # Numeración (la del diagrama): un solo contador compartido, que cuenta bytes.
    #   connect: SYN seq=x  ->  SYN+ACK seq=x+1  ->  ACK seq=x+2
    #   datos:   segmento seq=S con n bytes  ->  ACK con seq=S+n  (siguiente número esperado)
    # El campo ack_num del header no se usa (va en 0).

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(SocketTCP.TIMEOUT)

        self.dest_address = None
        self._accepted = set()  # (address, x) de SYN ya atendidos: evita conexiones fantasma por SYN duplicados
        self.seq_num = 0  # próximo número de secuencia (a enviar o a esperar)

        # estado de recv: bytes del mensaje actual que aún NO han llegado
        # y bytes que ya llegaron pero no se han entregado (por buff_size)
        self.msg_remaining = 0
        self.leftover = b""

        # Parte 3: control de congestión
        self.congestion_controler = CongestionControl(SocketTCP.CC_MSS)
        self.congestion_control_on = False

    def set_congestion_control(self, on):
        """Setter de congestion_control_on: True activa el control de congestión en Go-Back N."""
        self.congestion_control_on = bool(on)

    def _cc_log(self, event, data_window):
        """Debug de la ventana de congestión (solo si CC_DEBUG y el control de congestión está activo)."""
        if not (SocketTCP.CC_DEBUG and self.congestion_control_on):
            return
        cc = self.congestion_controler
        seqs = [data_window.get_sequence_number(i) for i in range(data_window.window_size)
                if data_window.get_data(i) is not None]
        if len(seqs) > 8:  # ventana grande: mostramos solo los extremos
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

    # ------------------------------------------------------------------
    # bind / connect / accept -> 3-way handshake (todavía sin manejo de pérdidas)
    # ------------------------------------------------------------------
    def bind(self, address):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(address)

    def connect(self, address):
        x = random.randint(0, 100)
        syn = SocketTCP.create_segment(seq_num=x, syn=True)

        # 1) SYN, seq=x  ->  2) esperamos SYN+ACK, seq=x+1  (Stop & Wait, máx. HANDSHAKE_TRIES)
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
                new_address = addr  # "Guardamos Address"
                break
        if new_address is None:
            raise ConnectionError("connect: no hubo respuesta del servidor (SYN+ACK)")

        self.dest_address = new_address
        self.seq_num = x + 2

        # 3) ACK, seq=x+2
        ack = SocketTCP.create_segment(seq_num=self.seq_num, ack=True)
        self.sock.sendto(ack, self.dest_address)

        # Caso borde: si ese ACK se pierde, el servidor reenvía el SYN+ACK tras un timeout.
        # Esperamos un poco MÁS que un timeout (GRACE*TIMEOUT: el SYN+ACK reenviado demora
        # además el tiempo de viaje) y, cada vez que llegue un SYN+ACK repetido, reenviamos el ACK.
        self.sock.settimeout(SocketTCP.TIMEOUT * SocketTCP.HANDSHAKE_GRACE)
        while True:
            try:
                segment, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                break  # nadie reclamó el ACK -> el servidor lo recibió
            info = SocketTCP.parse_segment(segment)
            if info["syn"] and info["ack"] and info["seq_num"] == x + 1:
                SocketTCP._log("connect: SYN+ACK repetido (se perdió mi ACK), reenvío ACK")
                self.sock.sendto(ack, self.dest_address)
        self.sock.settimeout(SocketTCP.TIMEOUT)

    def accept(self):
        # esperamos un SYN "para siempre" (el timeout solo es para send/recv)
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

        new_conn = SocketTCP()                   # "new socket()"
        new_conn.bind((self.sock.getsockname()[0], 0))
        new_address = new_conn.sock.getsockname()
        new_conn.dest_address = client_address

        # 2) SYN+ACK, seq=x+1  ->  3) esperamos ACK, seq=x+2  (Stop & Wait, máx. HANDSHAKE_TRIES)
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

    # ------------------------------------------------------------------
    # send / recv -> despachan según el modo ARQ
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Stop & Wait
    # ------------------------------------------------------------------
    def _send_and_wait_ack(self, data):
        """Envía un segmento con `data` y espera su ACK; retransmite si hay timeout."""
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
        # 1er segmento: largo del mensaje (4 bytes), luego los trozos de <= 16 bytes
        self._send_and_wait_ack(len(message).to_bytes(SocketTCP.LENGTH_BYTES, "big"))

        for i in range(0, len(message), SocketTCP.MAX_DATA_SIZE):
            self._send_and_wait_ack(message[i:i + SocketTCP.MAX_DATA_SIZE])

    def _send_ack(self):
        # el ACK lleva en seq el siguiente número que espero
        self.sock.sendto(SocketTCP.create_segment(seq_num=self.seq_num, ack=True), self.dest_address)

    def _recv_segment_data(self):
        """Espera el siguiente segmento en orden, lo ACKea y devuelve sus datos."""
        while True:
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                continue  # el receptor solo espera; el que retransmite es el emisor

            info = SocketTCP.parse_segment(raw)
            if info["syn"] or info["fin"] or info["ack"] or len(info["data"]) == 0:
                continue  # por ahora ignoramos lo que no sea un segmento de datos

            if info["seq_num"] == self.seq_num:
                self.seq_num += len(info["data"])
                self._send_ack()
                return info["data"]

            # duplicado (se perdió el ACK): reenvío el ACK actual y no entrego los datos otra vez
            SocketTCP._log(f"duplicado seq={info['seq_num']} (espero {self.seq_num}), reenvío ACK")
            self._send_ack()

    def recv_using_stop_and_wait(self, buff_size):
        # ¿empieza un mensaje nuevo? -> el primer segmento trae message_length
        if self.msg_remaining == 0 and len(self.leftover) == 0:
            self.msg_remaining = int.from_bytes(self._recv_segment_data(), "big")

        # cuántos bytes vamos a devolver: min(lo que queda del mensaje, buff_size)
        target = min(buff_size, len(self.leftover) + self.msg_remaining)

        while len(self.leftover) < target:
            data = self._recv_segment_data()
            self.leftover += data
            self.msg_remaining -= len(data)

        # devolvemos exactamente `target`; lo que sobre queda guardado para el próximo recv
        out, self.leftover = self.leftover[:target], self.leftover[target:]
        return out

    # ------------------------------------------------------------------
    # Go-Back N
    # ------------------------------------------------------------------
    def send_using_go_back_n(self, message):
        cc = self.congestion_controler
        use_cc = self.congestion_control_on

        # los trozos son de tamaño MSS (8 bytes), con o sin control de congestión
        data_list = [len(message).to_bytes(SocketTCP.LENGTH_BYTES, "big")] + \
                    [message[i:i + cc.MSS] for i in range(0, len(message), cc.MSS)]
        final_seq = self.seq_num + sum(len(d) for d in data_list)

        # tamaño inicial: lo manda el control de congestión (cwnd = 1 MSS -> window_size = 1)
        # o, si está apagado, la ventana fija de siempre
        window_size = cc.get_MSS_in_cwnd() if use_cc else SocketTCP.GBN_WINDOW_SIZE
        data_window = swcc.SlidingWindowCC(window_size, data_list, self.seq_num)
        self._cc_log("inicio", data_window)

        # envolvemos el socket existente en un SocketUDP (no bloqueante, con timers)
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
                    udp.stop_timer(0)  # un Timer vencido no se puede reusar
                    if use_cc:
                        cc.event_timeout()
                        window_size = cc.get_MSS_in_cwnd()      # cwnd = MSS -> window_size = 1
                        data_window.update_window_size(window_size)
                        self._cc_log("TIMEOUT", data_window)
                    for i in range(window_size):
                        send_element(i)
                    continue

                info = SocketTCP.parse_segment(raw)
                if not info["ack"]:
                    continue

                # ACK acumulativo: movemos la ventana mientras el primer elemento quede confirmado.
                # Caso borde: si window_size bajó justo después de enviar la ventana anterior, el ACK
                # puede ser mayor que todo lo que hay en la ventana; entonces k == window_size, la
                # ventana avanza completa y el while repite hasta que el ACK caiga dentro de ella.
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
                    continue  # ACK repetido o viejo (no cuenta como ACK nuevo para la congestión)

                if use_cc:
                    cc.event_ack_received()
                    window_size = cc.get_MSS_in_cwnd()
                    data_window.update_window_size(window_size)  # crece o se achica según cwnd
                    self._cc_log("ACK", data_window)

                # elementos nuevos en la ventana: los que entraron al avanzar (posiciones desde
                # old_size - moved) y los que entraron al crecer (hasta window_size). Van consecutivos.
                new_idx = [i for i in range(max(0, old_size - moved), window_size)
                           if data_window.get_data(i) is not None]

                # si entran segmentos nuevos, su sendto arranca un timer nuevo;
                # si no entra nada pero quedan sin ACK, dejamos correr el timer actual
                if new_idx or data_window.get_data(0) is None:
                    udp.stop_timer(0)
                for i in new_idx:
                    send_element(i)
        finally:
            if udp.timer_list[0] is not None:
                udp.stop_timer(0)
            self.sock.settimeout(SocketTCP.TIMEOUT)  # dejamos el socket como lo espera el resto de la clase

        self.seq_num = final_seq

    def recv_using_go_back_n(self, buff_size):
        # el receptor de Stop & Wait ya sirve para Go-Back N: acepta solo el seq esperado
        # y ante cualquier otro reenvía el ACK actual (acumulativo)
        return self.recv_using_stop_and_wait(buff_size)

    # ------------------------------------------------------------------
    # close / recv_close -> fin de conexión con tolerancia a pérdidas
    #
    #   Host A  close()                         Host B  recv_close()
    #   ------------------------------------------------------------
    #   FIN,     seq=S      ------------------->
    #                       <-------------------  FIN+ACK, seq=S+1
    #   ACK,     seq=S+2    ------------------->   (A lo manda 3 veces)
    #
    # close():      hasta 3 timeouts esperando el FIN+ACK (reenvía el FIN en cada uno);
    #               si no llega, asume que la contraparte ya cerró y cierra igual.
    #               Si llega, manda el ACK final 3 veces esperando un timeout entre envíos.
    # recv_close(): espera el FIN; responde FIN+ACK; espera hasta 3 timeouts por el ACK final
    #               (si llega otro FIN, reenvía el FIN+ACK) y cierra pase lo que pase.
    # ------------------------------------------------------------------
    def close(self):
        """Lado Host A: inicia el cierre."""
        s = self.seq_num
        fin = SocketTCP.create_segment(seq_num=s, fin=True)

        # 1) FIN, seq=S  ->  2) esperamos FIN+ACK, seq=S+1 (hasta 3 timeouts)
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
            # 3) ACK, seq=S+2, enviado 3 veces con un timeout de espera entre envíos
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
        """Lado Host B: espera el FIN, responde FIN+ACK, espera el ACK final y cierra."""
        # 1) esperamos FIN, seq=S (S = el seq que esperamos)
        while True:
            try:
                raw, _ = self.sock.recvfrom(SocketTCP.BUFFER_SIZE)
            except socket.timeout:
                continue
            info = SocketTCP.parse_segment(raw)
            if info["fin"] and info["seq_num"] == self.seq_num:
                break
            if info["data"]:
                # el emisor retransmite su último dato (se perdió mi ACK): lo confirmo de nuevo
                SocketTCP._log("recv_close: dato duplicado antes del FIN, reenvío ACK")
                self._send_ack()

        s = self.seq_num

        # 2) FIN+ACK, seq=S+1
        fin_ack = SocketTCP.create_segment(seq_num=s + 1, fin=True, ack=True)
        self.sock.sendto(fin_ack, self.dest_address)

        # 3) esperamos el ACK, seq=S+2, hasta 3 timeouts
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