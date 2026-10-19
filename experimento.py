import sys, time, threading
import SocketTCP2
from SocketTCP2 import SocketTCP

SocketTCP.TIMEOUT = 0.5
SocketTCP.DEBUG = False
SocketTCP.CC_DEBUG = False

RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
BASE_PORT = 9000
NBYTES = int(sys.argv[2]) if len(sys.argv) > 2 else None
mensaje = open("archivo.txt", "rb").read()
if NBYTES:
    mensaje = mensaje[:NBYTES]
print(f"Mensaje: {len(mensaje)} bytes (~{len(mensaje)//SocketTCP.CC_MSS + 1} segmentos sin pérdida)", flush=True)


def una_corrida(use_cc, puerto):
    resultado = {}

    def servidor():
        try:
            s = SocketTCP()
            s.bind(("localhost", puerto))
            conn, _ = s.accept()
            resultado["data"] = conn.recv(10**8, mode="go_back_n")
            conn.recv_close()
        except Exception as e:
            # si el servidor muere (p.ej. handshake perdido) destrabo al cliente
            resultado["error"] = e
            if "client" in resultado:
                try:
                    resultado["client"].sock.close()
                except Exception:
                    pass

    th = threading.Thread(target=servidor, daemon=True)
    th.start()
    time.sleep(0.3)

    c = SocketTCP()
    resultado["client"] = c
    c.set_congestion_control(use_cc)
    c.connect(("localhost", puerto))
    t0 = time.time()
    try:
        c.send(mensaje, mode="go_back_n")
    except Exception:
        if "error" in resultado:
            raise RuntimeError(f"servidor falló: {resultado['error']}")
        raise
    tiempo = time.time() - t0
    nseg = c.number_of_sent_segments
    c.close()
    th.join(timeout=30)

    if "error" in resultado:
        raise RuntimeError(f"servidor falló: {resultado['error']}")
    return tiempo, nseg, resultado.get("data") == mensaje


def correr_variante(use_cc, puerto_inicial):
    filas = []
    puerto = puerto_inicial
    while len(filas) < RUNS:
        try:
            r = una_corrida(use_cc, puerto)
        except Exception as e:  # p.ej. handshake fallido por la perdida
            print(f"  (run falló: {e!r}, reintento)", flush=True)
            r = None
        puerto += 1
        if r is None:
            continue
        filas.append(r)
        print(f"  run {len(filas)}: {r[0]:.2f} s, {r[1]} segmentos, íntegro={r[2]}", flush=True)
    return filas


print(f"== CON control de congestión ({RUNS} runs) ==", flush=True)
cc = correr_variante(True, BASE_PORT)
print(f"== SIN control de congestión, ventana fija {SocketTCP.GBN_WINDOW_SIZE} ({RUNS} runs) ==", flush=True)
sin = correr_variante(False, BASE_PORT + 100)

print("\n=== TABLA (pegar en el Typst) ===")
for i in range(RUNS):
    print(f"[{i+1}], [{cc[i][0]:.2f}], [{cc[i][1]}], [{sin[i][0]:.2f}], [{sin[i][1]}],")
prom = lambda xs: sum(xs) / len(xs)
print(f"[*Promedio*], [{prom([r[0] for r in cc]):.2f}], [{prom([r[1] for r in cc]):.0f}], "
      f"[{prom([r[0] for r in sin]):.2f}], [{prom([r[1] for r in sin]):.0f}],")
