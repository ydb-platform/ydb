from sys import maxsize

from nanoid import generate, non_secure_generate
from nanoid.resources import alphabet


def test_flat_distribution():
    count = 100 * 1000
    length = 5
    alphabet = 'abcdefghijklmnopqrstuvwxyz'

    chars = {}
    for _ in range(count):
        id = generate(alphabet, length)
        for j in range(len(id)):
            char = id[j]
            if not chars.get(char):
                chars[char] = 0
            chars[char] += 1

    assert len(chars.keys()) == len(alphabet)

    max = 0
    min = maxsize
    for k in chars:
        distribution = (chars[k] * len(alphabet)) / float((count * length))
        if distribution > max:
            max = distribution
        if distribution < min:
            min = distribution
    assert max - min <= 0.05


def test_generates_url_friendly_id():
    for _ in range(10):
        id = generate()
        assert len(id) == 21
        for j in range(len(id)):
            assert id[j] in alphabet


def test_has_no_collisions():
    count = 100 * 1000
    used = {}
    for _ in range(count):
        id = generate()
        assert id is not None
        used[id] = True


def test_has_options():
    assert generate('a', 5) == 'aaaaa'


def test_non_secure_ids():
    for i in range(10000):
        nanoid = non_secure_generate()
        assert len(nanoid) == 21


def test_non_secure_short_ids():
    for i in range(10000):
        nanoid = non_secure_generate("12345a", 3)
        assert len(nanoid) == 3


def test_short_secure_ids():
    for i in range(10000):
        nanoid = generate("12345a", 3)
        assert len(nanoid) == 3
