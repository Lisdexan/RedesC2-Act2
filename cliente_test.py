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
SocketTCP.CC_DEBUG = True
mensaje = open(os.path.join(CARPETA, "archivo.txt"), "rb").read()

c = SocketTCP()
c.set_congestion_control(True)
print("[client] conectando...", flush=True)
c.connect(("localhost", 8000))
print(f"[client] conectado, enviando {len(mensaje)} bytes...", flush=True)
c.send(mensaje, mode="go_back_n")
print("[client] envío terminado, cerrando...", flush=True)
c.close()
print("[client] cerrado", flush=True)