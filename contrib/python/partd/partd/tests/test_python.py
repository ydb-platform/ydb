from partd.python import dumps, loads


import os
import shutil
from math import sin


def test_pack_unpack():
    data = [1, 2, b'Hello', 'Hello']
    assert loads(dumps(data)) == data

    data = [1, 2, sin]
    assert loads(dumps(data)) == data
