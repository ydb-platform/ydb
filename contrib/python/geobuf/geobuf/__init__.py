from .encode import Encoder
from .decode import Decoder

__version__ = '2.0.1'


def encode(*args):
    return Encoder().encode(*args)


def decode(*args):
    return Decoder().decode(*args)
