from __future__ import unicode_literals
from __future__ import division

from os import urandom


def algorithm_generate(random_bytes):
    return bytearray(urandom(random_bytes))
