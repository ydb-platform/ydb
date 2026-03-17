from partd import File
from partd.core import token, escape_filename, filename
from partd import core
import os
import shutil
from contextlib import contextmanager


def test_partd():
    path = 'tmp.partd'

    with File(path) as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})
        assert os.path.exists(p.filename('x'))
        assert os.path.exists(p.filename('y'))

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)

    assert not os.path.exists(path)


def test_key_tuple():
    with File('foo') as p:
        p.append({('a', 'b'): b'123'})
        assert os.path.exists(os.path.join(p.path, 'a', 'b'))


def test_ensure():
    with File('foo') as p:
        p.iset('x', b'123')
        p.iset('x', b'123')
        p.iset('x', b'123')

        assert p.get('x') == b'123'


def test_filenames():
    assert token('hello') == 'hello'
    assert token(('hello', 'world')) == os.path.join('hello', 'world')
    assert escape_filename(os.path.join('a', 'b')) == os.path.join('a', 'b')
    assert filename('dir', ('a', 'b')) == os.path.join('dir', 'a', 'b')
