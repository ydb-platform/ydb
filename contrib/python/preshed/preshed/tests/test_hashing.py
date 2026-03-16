import pytest

from preshed.maps import PreshMap
import random


def test_insert():
    h = PreshMap()
    assert h[1] is None
    h[1] = 5
    assert h[1] == 5
    h[2] = 6
    assert h[1] == 5
    assert h[2] == 6

def test_resize():
    h = PreshMap(4)
    h[4] = 12
    for i in range(10, 100):
        value = int(i * (random.random() + 1))
        h[i] = value
    assert h[4] == 12


def test_zero_key():
    h = PreshMap()
    h[0] = 6
    h[5] = 12
    assert h[0] == 6
    assert h[5] == 12

    for i in range(500, 1000):
        h[i] = i * random.random()
    assert h[0] == 6
    assert h[5] == 12


def test_iter():
    key_sum = 0
    val_sum = 0
    h = PreshMap()
    for i in range(56, 24, -3):
        h[i] = i * 2
        key_sum += i
        val_sum += i * 2
    for key, value in h.items():
        key_sum -= key
        val_sum -= value
    assert key_sum == 0
    assert val_sum == 0


def test_one_and_empty():
    # See Issue #21
    table = PreshMap()
    for i in range(100, 110):
        table[i] = i
        del table[i]
    assert table[0] == None


def test_many_and_empty():
    # See Issue #21
    table = PreshMap()
    for i in range(100, 110):
        table[i] = i
    for i in range(100, 110):
        del table[i]
    assert table[0] == None


def test_zero_values():
    table = PreshMap()
    table[10] = 0
    assert table[10] == 0
    assert table[11] is None
