from partd.file import File
from partd.encode import Encode

import zlib
import shutil
import os


def test_partd():
    with Encode(zlib.compress, zlib.decompress, b''.join) as p:
        p.append({'x': b'Hello', 'y': b'abc'})
        p.append({'x': b'World!', 'y': b'def'})

        result = p.get(['y', 'x'])
        assert result == [b'abcdef', b'HelloWorld!']

        assert p.get('z') == b''

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['x'], lock=False)


def test_ensure():
    with Encode(zlib.compress, zlib.decompress, b''.join) as p:
        p.iset('x', b'123')
        p.iset('x', b'123')
        p.iset('x', b'123')
        assert p.get('x') == b'123'
