from __future__ import division
import pytest
import pickle

from preshed.bloom import BloomFilter

def test_contains():
    bf = BloomFilter()
    assert 23 not in bf
    bf.add(23)
    assert 23 in bf

    bf.add(5)
    bf.add(42)
    bf.add(1002)
    assert 5 in bf
    assert 42 in bf
    assert 1002 in bf

def test_no_false_negatives():
    bf = BloomFilter(size=100, hash_funcs=2)
    for ii in range(0,1000,20):
        bf.add(ii)

    for ii in range(0,1000,20):
        assert ii in bf

def test_from_error():
    bf = BloomFilter.from_error_rate(1000)
    for ii in range(0,1000,20):
        bf.add(ii)

    for ii in range(0,1000,20):
        assert ii in bf

def test_to_from_bytes():
    bf = BloomFilter(size=100, hash_funcs=2)
    for ii in range(0,1000,20):
        bf.add(ii)
    data = bf.to_bytes()
    bf2 = BloomFilter()
    for ii in range(0,1000,20):
        assert ii not in bf2
    bf2.from_bytes(data)
    for ii in range(0,1000,20):
        assert ii in bf2
    assert bf2.to_bytes() == data

def test_bloom_pickle():
    bf = BloomFilter(size=100, hash_funcs=2)
    for ii in range(0,1000,20):
        bf.add(ii)
    data = pickle.dumps(bf)
    bf2 = pickle.loads(data)
    for ii in range(0,1000,20):
        assert ii in bf2
