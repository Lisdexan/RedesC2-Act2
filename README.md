# Redes C2
## Actividad 2
Para ejecutar el proyecto se debe runear desde una virtual box, linux.
Usando dos terminales en la carpeta con los comandos:`python3 servidor_test.py` y luego `python3 cliente_test.py`.

El cliente envía archivo.txt usando Go-Back N con control de congestión, y el servidor lo guarda en recibido.txt. Si todo funciona, el servidor imprime "OK, llegó íntegro".

Para probar con pérdidas se usa netem: `sudo tc qdisc add dev lo root netem loss 20.0% delay 0.5s`, y al terminar se desactiva con `sudo tc qdisc del dev lo root netem`.

Se necesita tener SocketTCP2.py, CongestionControl.py, slidingWindowCC.py, socketUDP.py y archivo.txt en la misma carpeta donde se encuentran servidor_test.py y cliente_test.py

También se pueden hacer múltiples runs para poder comparar con congestión y sin congestión. usando `python3 experimento.py <runs> <bytes del msje>` como experimento.py 2 400, haria 2 runs con el mensaje solo tomando los primeros 400 bytes (2 con congestión y 2 sin congestión.)
