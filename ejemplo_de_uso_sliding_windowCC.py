import slidingWindowCC as swcc  # para poder llamar así a la clase, guárdela en un archivo llamado slidingWindowCC.py

window_size = 3
initial_seq = 5
message = "Esta es una prueba de sliding window"
message_length = str(len(message.encode()))

# mensaje "Esta es una prueba de sliding window." separado en grupos de 4 caracteres.
data_list = [message_length.encode()] + [message[i+4:i+8].encode() for i in range(-4, len(message)-4, 4)]
print(f"Creamos un objeto SlidingWindow con: \n - window_size = {window_size}\n - data_list = {data_list}\n - initial_seq = {initial_seq}\n")

# creamos un objeto SlidingWindow
data_window = swcc.SlidingWindowCC(window_size, data_list, initial_seq)

# Podemos imprimir la ventana inicial
print("Ventana inicial:")
print(data_window, "\n")
# y nos muestra lo siguiente:
# +------+---------+---------+---------+
# | Data | b'36'   | b'Esta' | b' es ' |
# +------+---------+---------+---------+
# | Seq  | 5       | 7       | 11      |
# +------+---------+---------+---------+

# Avanzamos la ventana en 2 espacios y luego otros 3
data_window.move_window(2)
data_window.move_window(3)
print("Ventana luego de llamar a data_window.move_window(2) y data_window.move_window(3):")
print(data_window, "\n")

# si avanzamos lo suficiente la ventana se acaban los datos
data_window.move_window(1)
data_window.move_window(3)
if data_window.get_sequence_number(2) == None and data_window.get_data(2) == None:
    print("El último elemento de la ventana es igual a None:")
    print(data_window, "\n")

# También podemos crear ventanas vacías de la siguiente forma:
print("Creamos una nueva ventana vacía:")
empty_window = swcc.SlidingWindowCC(window_size, [], initial_seq)
print(empty_window, "\n")

# y podemos añadir datos a esta ventana
add_data = b"Hola"
seq = initial_seq + 2
window_index = 2
empty_window.put_data(add_data, seq, window_index)
print(f"Añadimos data={add_data} con seq={seq} en el índice={window_index}:")
print(empty_window)