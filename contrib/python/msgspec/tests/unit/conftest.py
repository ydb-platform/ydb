import math
import random
import string
import struct

import pytest


class Rand:
    """Random source, pulled out into fixture with repr so the seed is
    displayed on failing tests"""

    def __init__(self, seed=0):
        self.seed = seed or random.randint(0, 2**32 - 1)
        self.rand = random.Random(self.seed)

    def __repr__(self):
        return f"Rand({self.seed})"

    def str(self, n, m=0):
        """
        str(n) -> random string of length `n`.
        str(n, m) -> random string between lengths `n` & `m`
        """
        if m:
            n = self.rand.randint(n, m)
        return "".join(self.rand.choices(string.ascii_letters, k=n))

    def bytes(self, n):
        """random bytes of length `n`"""
        return self.rand.getrandbits(8 * n).to_bytes(n, "little")

    def float(self):
        """random finite float"""
        while True:
            dbytes = self.rand.getrandbits(64).to_bytes(8, "big")
            x = struct.unpack("!d", dbytes)[0]
            if math.isfinite(x):
                return x

    def shuffle(self, obj):
        """random shuffle"""
        self.rand.shuffle(obj)


@pytest.fixture
def rand():
    yield Rand()


@pytest.fixture(scope="session")
def package_dir(pytestconfig):
    return pytestconfig.rootpath.joinpath("src", "msgspec")
