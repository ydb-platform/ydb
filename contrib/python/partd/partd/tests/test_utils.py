from partd.utils import frame, framesplit
import struct


def test_frame():
    assert frame(b'Hello') == struct.pack('Q', 5) + b'Hello'


def test_framesplit():
    L = [b'Hello', b'World!', b'123']
    assert list(framesplit(b''.join(map(frame, L)))) == L
