import random
import socket

ATTEMPTS = 25
# range 10000-10199 is reserved for Skynet on Sandbox machines
MIN_PORT = 10200
MAX_PORT = 25000


def is_port_open(host, port):
    _socket = socket.socket(socket.AF_INET)
    return _socket.connect_ex((host, port)) != 0


def find_free_port(range_start=MIN_PORT, range_end=MAX_PORT, attempts=ATTEMPTS):
    """
    Finds free port

    :param range_start: start of range
    :param range_end: end of range
    :param attempts: number of tries to find free port

    :return: some open port in a given range
    """
    ports = [random.randint(range_start, range_end) for _ in range(attempts)]
    while ports:
        port = ports.pop()
        if is_port_open('', port):
            return port
    raise RuntimeError('Could not find free port in range = ' + str((range_start, range_end)))
