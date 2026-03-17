import pytest
pytest.importorskip('zmq')

from partd.zmq import Server, keys_to_flush, File, Client
from partd import core, Dict
from threading import Thread
from time import sleep
from contextlib import contextmanager
import pickle

import os
import shutil


def test_server():
    s = Server()
    try:
        s.start()
        s.append({'x': b'abc', 'y': b'1234'})
        s.append({'x': b'def', 'y': b'5678'})

        assert s.get(['x']) == [b'abcdef']
        assert s.get(['x', 'y']) == [b'abcdef', b'12345678']

        assert s.get(['x']) == [b'abcdef']
    finally:
        s.close()


def dont_test_flow_control():
    path = 'bar'
    if os.path.exists('bar'):
        shutil.rmtree('bar')
    s = Server('bar', available_memory=1, n_outstanding_writes=3, start=False)
    p = Client(s.address)
    try:
        listen_thread = Thread(target=s.listen)
        listen_thread.start()
        """ Don't start these threads
        self._write_to_disk_thread = Thread(target=self._write_to_disk)
        self._write_to_disk_thread.start()
        self._free_frozen_sockets_thread = Thread(target=self._free_frozen_sockets)
        self._free_frozen_sockets_thread.start()
        """
        p.append({'x': b'12345'})
        sleep(0.1)
        assert s._out_disk_buffer.qsize() == 1
        p.append({'x': b'12345'})
        p.append({'x': b'12345'})
        sleep(0.1)
        assert s._out_disk_buffer.qsize() == 3

        held_append = Thread(target=p.append, args=({'x': b'123'},))
        held_append.start()

        sleep(0.1)
        assert held_append.is_alive()  # held!

        assert not s._frozen_sockets.empty()

        write_to_disk_thread = Thread(target=s._write_to_disk)
        write_to_disk_thread.start()
        free_frozen_sockets_thread = Thread(target=s._free_frozen_sockets)
        free_frozen_sockets_thread.start()

        sleep(0.2)
        assert not held_append.is_alive()
        assert s._frozen_sockets.empty()
    finally:
        s.close()


@contextmanager
def partd_server(**kwargs):
    with Server(**kwargs) as server:
        with Client(server.address) as p:
            yield (p, server)


def test_partd_object():
    with partd_server() as (p, server):
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']


def test_delete():
    with partd_server() as (p, server):
        p.append({'x': b'Hello'})
        assert p.get('x') == b'Hello'
        p.delete(['x'])
        assert p.get('x') == b''


def test_iset():
    with partd_server() as (p, server):
        p.iset('x', b'111')
        p.iset('x', b'111')
        assert p.get('x') == b'111'


def test_tuple_keys():
    with partd_server() as (p, server):
        p.append({('x', 'y'): b'123'})
        assert p.get(('x', 'y')) == b'123'


def test_serialization():
    with partd_server() as (p, server):
        p.append({'x': b'123'})
        q = pickle.loads(pickle.dumps(p))
        assert q.get('x') == b'123'


def test_drop():
    with partd_server() as (p, server):
        p.append({'x': b'123'})
        p.drop()
        assert p.get('x') == b''


def dont_test_server_autocreation():
    with Client() as p:
        p.append({'x': b'123'})
        assert p.get('x') == b'123'
