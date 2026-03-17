# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function


import socket
import select
import threading


def listen(server, on_read):
    sock, addr = server.accept()

    timeout = 10
    try:
        read_ready, _, _ = select.select([sock], [], [], timeout)
        while len(read_ready):
            data = sock.recv(8192)
            if on_read(sock, data):
                read_ready, _, _ = select.select([sock], [], [], 0)
            else:
                read_ready = []
    except (socket.error, select.error, OSError, ValueError):
        pass
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except (socket.error, OSError, ValueError):
        pass
    sock.close()


def make_socket_server(port, on_read=None):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('', port))
    server.listen(1)
    t = threading.Thread(
        target=listen,
        args=(server, on_read)
    )
    t.start()
    return server
