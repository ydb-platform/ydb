from sys import maxsize

from nanoid import non_secure_generate
from nanoid.resources import alphabet


def test_changes_id_length():
    assert len(non_secure_generate(size=10)) == 10


def test_generates_url_friendly_id():
    for _ in range(10):
        id = non_secure_generate()
        assert len(id) == 21
        for j in range(len(id)):
            assert alphabet.find(id[j]) != -1


def test_has_flat_distribution():
    count = 100 * 1000
    length = len(non_secure_generate())

    chars = {}
    for _ in range(count):
        id = non_secure_generate()
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


def test_has_no_collisions():
    count = 100 * 1000
    used = {}
    for _ in range(count):
        id = non_secure_generate()
        assert id is not None
        used[id] = True
