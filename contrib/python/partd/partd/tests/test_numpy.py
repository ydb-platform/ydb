import pytest
np = pytest.importorskip('numpy')  # noqa

import pickle

import partd
from partd.numpy import Numpy


def test_numpy():
    dt = np.dtype([('a', 'i4'), ('b', 'i2'), ('c', 'f8')])
    with Numpy() as p:
        p.append({'a': np.array([10, 20, 30], dtype=dt['a']),
                  'b': np.array([ 1,  2,  3], dtype=dt['b']),
                  'c': np.array([.1, .2, .3], dtype=dt['c'])})
        p.append({'a': np.array([70, 80, 90], dtype=dt['a']),
                  'b': np.array([ 7,  8,  9], dtype=dt['b']),
                  'c': np.array([.7, .8, .9], dtype=dt['c'])})

        result = p.get(['a', 'c'])
        assert (result[0] == np.array([10, 20, 30, 70, 80, 90],dtype=dt['a'])).all()
        assert (result[1] == np.array([.1, .2, .3, .7, .8, .9],dtype=dt['c'])).all()

        with p.lock:  # uh oh, possible deadlock
            result = p.get(['a'], lock=False)


def test_nested():
    with Numpy() as p:
        p.append({'x': np.array([1, 2, 3]),
                 ('y', 1): np.array([4, 5, 6]),
                 ('z', 'a', 3): np.array([.1, .2, .3])})
        assert (p.get(('z', 'a', 3)) == np.array([.1, .2, .3])).all()


def test_serialization():
    with Numpy() as p:
        p.append({'x': np.array([1, 2, 3])})
        q = pickle.loads(pickle.dumps(p))
        assert (q.get('x') == [1, 2, 3]).all()


array_of_lists = np.empty(3, dtype='O')
array_of_lists[:] = [[1, 2], [3, 4], [5, 6]]


@pytest.mark.parametrize('x', [np.array(['Alice', 'Bob', 'Charlie'], dtype='O'),
                               array_of_lists])
def test_object_dtype(x):
    with Numpy() as p:
        p.append({'x': x})
        p.append({'x': x})
        assert isinstance(p.get('x'), np.ndarray)
        assert (p.get('x') == np.concatenate([x, x])).all()


def test_datetime_types():
    x = np.array(['2014-01-01T12:00:00'], dtype='M8[us]')
    y = np.array(['2014-01-01T12:00:00'], dtype='M8[s]')
    with Numpy() as p:
        p.append({'x': x, 'y': y})
        assert p.get('x').dtype == x.dtype
        assert p.get('y').dtype == y.dtype


def test_non_utf8_bytes():
    a = np.array([b'\xc3\x28', b'\xa0\xa1', b'\xe2\x28\xa1', b'\xe2\x82\x28',
                  b'\xf0\x28\x8c\xbc'], dtype='O')
    s = partd.numpy.serialize(a)
    assert (partd.numpy.deserialize(s, 'O') == a).all()
