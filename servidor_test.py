import os, sys, inspect
import SocketTCP2
from SocketTCP2 import SocketTCP

CARPETA = os.path.dirname(os.path.abspath(__file__))
print("[info] SocketTCP2 viene de:", SocketTCP2.__file__)
if "mode" not in inspect.signature(SocketTCP.send).parameters:
    print("[ERROR] Ese SocketTCP2.py es la versión VIEJA (send no tiene 'mode'). Pega el código nuevo ahí.")
    sys.exit()

SocketTCP.TIMEOUT = 0.5
SocketTCP.DEBUG = True

s = SocketTCP()
s.bind(("localhost", 8000))
print("[server] esperando accept...", flush=True)
conn, _ = s.accept()
print("[server] conexión aceptada, esperando datos...", flush=True)

data = conn.recv(10**8, mode="go_back_n")
ruta = os.path.join(CARPETA, "recibido.txt")
with open(ruta, "wb") as f:
    f.write(data)
original = open(os.path.join(CARPETA, "archivo.txt"), "rb").read()
print("OK, llegó íntegro" if data == original else "ERROR, no coincide", len(data), "bytes", flush=True)
print("[server] guardado en:", ruta, flush=True)

conn.recv_close()
print("[server] cerrado", flush=True)