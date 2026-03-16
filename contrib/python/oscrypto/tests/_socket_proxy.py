# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function


import socket
import select
import threading


_sockets = {}
_socket_lock = threading.Lock()


def proxy(src, dst, callback=None):
    timeout = 10
    try:
        read_ready, _, _ = select.select([src], [], [], timeout)
        while len(read_ready):
            if callback:
                callback(src, dst)
            else:
                dst.send(src.recv(8192))
            read_ready, _, _ = select.select([src], [], [], timeout)
    except (socket.error, select.error, OSError, ValueError):
        pass
    try:
        src.shutdown(socket.SHUT_RDWR)
    except (socket.error, OSError, ValueError):
        pass
    src.close()
    try:
        dst.shutdown(socket.SHUT_RDWR)
    except (socket.error, OSError, ValueError):
        pass
    dst.close()


def listen(server, ip, port, send_callback, recv_callback):
    lsock, laddr = server.accept()
    rsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rsock.connect((ip, port))
    with _socket_lock:
        _sockets[threading.current_thread().ident] = {
            'lsock': lsock,
            'rsock': rsock
        }
    t1 = threading.Thread(target=proxy, args=(rsock, lsock, recv_callback))
    t2 = threading.Thread(target=proxy, args=(lsock, rsock, send_callback))
    t1.start()
    t2.start()


def make_socket_proxy(ip, port, send_callback=None, recv_callback=None):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('', 8080))
    server.listen(1)
    t = threading.Thread(
        target=listen,
        args=(server, ip, port, send_callback, recv_callback)
    )
    t.start()
    sock = socket.create_connection(('localhost', 8080))
    sock.settimeout(1)
    t.join()
    with _socket_lock:
        data = _sockets[t.ident]
        return (sock, data['lsock'], data['rsock'], server)
