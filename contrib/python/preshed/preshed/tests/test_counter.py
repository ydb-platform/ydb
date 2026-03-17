from __future__ import division
import pytest

from preshed.counter import PreshCounter


def test_count():
    counter = PreshCounter()
    assert counter[12] == 0
    counter.inc(12, 1)
    assert counter[12] == 1
    counter.inc(14, 10)
    counter.inc(9, 10)
    counter.inc(12, 4)
    assert counter[12] == 5
    assert counter[14] == 10
    assert counter[9] == 10


def test_unsmooth_prob():
    counter = PreshCounter()
    assert counter.prob(12) == 0.0
    counter.inc(12, 1)
    assert counter.prob(12) == 1.0
    counter.inc(14, 10)
    assert counter.prob(14) == 10 / 11
    assert counter.prob(12) == 1.0 / 11

def test_smooth_prob():
    p = PreshCounter()
    # 1 10
    # 2 6
    # 3 4
    # 5 2
    # 8 1
    for i in range(10):
        p.inc(100-i, 1) # 10 items of freq 1
    for i in range(6):
        p.inc(90 - i, 2) # 6 items of freq 2
    for i in range(4):
        p.inc(80 - i, 3) # 4 items of freq 3
    for i in range(2):
        p.inc(70 - i, 5) # 2 items of freq 5
    for i in range(1):
        p.inc(60 - i, 8) # 1 item of freq 8

    assert p.total == (10 * 1) + (6 * 2) + (4 * 3) + (2 * 5) + (1 * 8)

    assert p.prob(100) == 1.0 / p.total
    assert p.prob(200) == 0.0
    assert p.prob(60) == 8.0 / p.total

    p.smooth()

    assert p.smoother(1) < 1.0
    assert p.smoother(8) < 8.0
    assert p.prob(1000) < p.prob(100)

    for event, count in reversed(sorted(p, key=lambda it: it[1])):
        assert p.smoother(count) < count


import os
def test_large_freqs():
    if 'TEST_FILE_LOC' in os.environ:
        loc = os.environ['TEST_FILE_LOC']
    else:
        return None
    counts = PreshCounter()
    for i, line in enumerate(open(loc)):
        line = line.strip()
        if not line:
            continue
        freq = int(line.split()[0])
        counts.inc(i+1, freq)
    oov = i+2
    assert counts.prob(oov) == 0.0
    assert counts.prob(1) < 0.1
    counts.smooth()
    assert counts.prob(oov) > 0
    assert counts.prob(oov) < counts.prob(i)
