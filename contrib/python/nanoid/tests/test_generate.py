from sys import maxsize
from nanoid import generate


def test_has_flat_distribution():
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


def test_has_no_collisions():
    count = 100 * 1000
    used = {}
    for _ in range(count):
        id = generate()
        assert id is not None
        used[id] = True


def test_has_options():
    count = 100 * 1000
    for _ in range(count):
        assert generate('a', 5) == 'aaaaa'
        assert len(generate(alphabet="12345a", size=3)) == 3
