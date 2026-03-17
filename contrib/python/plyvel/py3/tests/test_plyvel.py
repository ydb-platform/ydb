# encoding: UTF-8

from __future__ import unicode_literals

import itertools
import os
import random
import shutil
import stat
import sys
import tempfile
import threading
import time

import pytest

import plyvel

if sys.version_info < (3, 0):
    from future_builtins import zip
    range = xrange  # noqa: F821 (python 2 only)


#
# Fixtures
#

@pytest.fixture
def db_dir(request):
    name = tempfile.mkdtemp()

    def finalize():
        shutil.rmtree(name)

    request.addfinalizer(finalize)
    return name


@pytest.fixture
def db(request):
    name = tempfile.mkdtemp()
    db = plyvel.DB(name, create_if_missing=True, error_if_exists=True)

    def finalize():
        db.close()
        shutil.rmtree(name)

    request.addfinalizer(finalize)
    return db


#
# Actual tests
#

def test_version():
    v = plyvel.__leveldb_version__
    assert v.startswith('1.')


def test_open_read_only_dir(db_dir):
    # Opening a DB in a read-only dir should not work
    os.chmod(db_dir, stat.S_IRUSR | stat.S_IXUSR)
    with pytest.raises(plyvel.IOError):
        plyvel.DB(db_dir)


def test_open_no_create(db_dir):
    with pytest.raises(plyvel.Error):
        plyvel.DB(db_dir, create_if_missing=False)


def test_open_fresh(db_dir):
    db = plyvel.DB(db_dir, create_if_missing=True)
    db.close()
    with pytest.raises(plyvel.Error):
        plyvel.DB(db_dir, error_if_exists=True)


def test_open_no_compression(db_dir):
    plyvel.DB(db_dir, compression=None, create_if_missing=True)


def test_open_many_options(db_dir):
    plyvel.DB(
        db_dir, create_if_missing=True, error_if_exists=False,
        paranoid_checks=True, write_buffer_size=16 * 1024 * 1024,
        max_open_files=512, lru_cache_size=64 * 1024 * 1024,
        block_size=2 * 1024, block_restart_interval=32,
        compression='snappy', bloom_filter_bits=10)


def test_invalid_open(db_dir):
    with pytest.raises(TypeError):
        plyvel.DB(123)

    with pytest.raises(TypeError):
        plyvel.DB(db_dir, write_buffer_size='invalid')

    with pytest.raises(TypeError):
        plyvel.DB(db_dir, lru_cache_size='invalid')

    with pytest.raises(ValueError):
        plyvel.DB(db_dir, compression='invalid', create_if_missing=True)


# XXX: letter casing of encoding names differs between Python 2 and 3
@pytest.mark.skipif(sys.getfilesystemencoding().lower() != 'utf-8',
                    reason="requires UTF-8 file system encoding")
def test_open_unicode_name(db_dir):
    db_dir = os.path.join(db_dir, 'úñîçøđê_name')
    os.makedirs(db_dir)
    plyvel.DB(db_dir, create_if_missing=True)


def test_open_close(db_dir):
    # Create a database with options that result in additional object
    # allocation (e.g. LRU cache).
    db = plyvel.DB(
        db_dir,
        create_if_missing=True,
        lru_cache_size=1024 * 1024,
        bloom_filter_bits=10)
    db.put(b'key', b'value')
    wb = db.write_batch()
    sn = db.snapshot()
    it = db.iterator()
    snapshot_it = sn.iterator()

    # Close the database
    db.close()
    assert db.closed

    # Expect runtime errors for operations on the database,
    with pytest.raises(RuntimeError):
        db.get(b'key')
    with pytest.raises(RuntimeError):
        db.put(b'key', b'value')
    with pytest.raises(RuntimeError):
        db.delete(b'key')

    # ... on write batches,
    with pytest.raises(RuntimeError):
        wb.put(b'key', b'value')

    # ... on snapshots,
    pytest.raises(RuntimeError, db.snapshot)
    with pytest.raises(RuntimeError):
        sn.get(b'key')

    # ... on iterators,
    with pytest.raises(RuntimeError):
        next(it)

    # ... and on snapshot iterators,
    with pytest.raises(RuntimeError):
        next(snapshot_it)


def test_large_lru_cache(db_dir):
    # Use a 2 GB size (does not fit in a 32-bit signed int)
    plyvel.DB(db_dir, create_if_missing=True, lru_cache_size=2 * 1024**3)


def test_put(db):
    db.put(b'foo', b'bar')
    db.put(b'foo', b'bar', sync=False)
    db.put(b'foo', b'bar', sync=True)

    for i in range(1000):
        key = ('key-%d' % i).encode('ascii')
        value = ('value-%d' % i).encode('ascii')
        db.put(key, value)

    pytest.raises(TypeError, db.put, b'foo', 12)
    pytest.raises(TypeError, db.put, 12, 'foo')


def test_get(db):
    key = b'the-key'
    value = b'the-value'
    assert db.get(key) is None
    db.put(key, value)
    assert db.get(key) == value
    assert db.get(key, verify_checksums=True) == value
    assert db.get(key, verify_checksums=False) == value
    assert db.get(key, verify_checksums=None) == value
    assert db.get(key, fill_cache=True) == value
    assert db.get(key, fill_cache=False, verify_checksums=None) == value

    key2 = b'key-that-does-not-exist'
    value2 = b'default-value'
    assert db.get(key2) is None
    assert db.get(key2, value2) == value2
    assert db.get(key2, default=value2) == value2

    pytest.raises(TypeError, db.get, 1)
    pytest.raises(TypeError, db.get, 'key')
    pytest.raises(TypeError, db.get, None)
    pytest.raises(TypeError, db.get, b'foo', b'default', True)


def test_delete(db):
    # Put and delete a key
    key = b'key-that-will-be-deleted'
    db.put(key, b'')
    assert db.get(key) is not None
    db.delete(key)
    assert db.get(key) is None

    # The .delete() method also takes write options
    db.put(key, b'')
    db.delete(key, sync=True)


def test_null_bytes(db):
    key = b'key\x00\x01'
    value = b'\x00\x00\x01'
    db.put(key, value)
    assert db.get(key) == value
    db.delete(key)
    assert db.get(key) is None


def test_bytes_like(db):
    b = b'bar'

    value = bytearray(b)
    db.put(b'quux', value)
    assert db.get(b'quux') == value

    value = memoryview(b)
    db.put(b'foo', value)
    assert db.get(b'foo') == value


def test_write_batch(db):
    # Prepare a batch with some data
    batch = db.write_batch()
    for i in range(1000):
        batch.put(('batch-key-%d' % i).encode('ascii'), b'value')

    # Delete a key that was also set in the same (pending) batch
    batch.delete(b'batch-key-2')

    # The DB should not have any data before the batch is written
    assert db.get(b'batch-key-1') is None

    # ...but it should have data afterwards
    batch.write()
    assert db.get(b'batch-key-1') is not None
    assert db.get(b'batch-key-2') is None

    # Batches can be cleared
    batch = db.write_batch()
    batch.put(b'this-is-never-saved', b'')
    batch.clear()
    batch.write()
    assert db.get(b'this-is-never-saved') is None

    # Batches take write options
    batch = db.write_batch(sync=True)
    batch.put(b'batch-key-sync', b'')
    batch.write()


def test_write_batch_context_manager(db):
    key = b'batch-key'
    assert db.get(key) is None
    with db.write_batch() as wb:
        wb.put(key, b'')
    assert db.get(key) is not None

    # Data should also be written when an exception is raised
    key = b'batch-key-exception'
    assert db.get(key) is None
    with pytest.raises(ValueError):
        with db.write_batch() as wb:
            wb.put(key, b'')
            raise ValueError()
    assert db.get(key) is not None


def test_write_batch_transaction(db):
    with pytest.raises(ValueError):
        with db.write_batch(transaction=True) as wb:
            wb.put(b'key', b'value')
            raise ValueError()

    assert list(db.iterator()) == []


def test_write_batch_bytes_like(db):
    with db.write_batch() as wb:
        wb.put(b'a', bytearray(b'foo'))
        wb.put(b'b', memoryview(b'bar'))
    assert db.get(b'a') == b'foo'
    assert db.get(b'b') == b'bar'


def test_iteration(db):
    entries = []
    for i in range(100):
        entry = (
            ('%03d' % i).encode('ascii'),
            ('%03d' % i).encode('ascii'))
        entries.append(entry)

    for k, v in entries:
        db.put(k, v)

    for entry, expected in zip(entries, db):
        assert entry == expected


def test_iterator_closing(db):
    db.put(b'k', b'v')
    it = db.iterator()
    next(it)
    it.close()
    pytest.raises(RuntimeError, next, it)
    pytest.raises(RuntimeError, it.seek_to_stop)

    with db.iterator() as it:
        next(it)

    pytest.raises(RuntimeError, next, it)


def test_iterator_return(db):
    db.put(b'key', b'value')

    for key, value in db:
        assert b'key' == key
        assert b'value' == value

    for key, value in db.iterator():
        assert b'key' == key
        assert b'value' == value

    for key in db.iterator(include_value=False):
        assert b'key' == key

    for value in db.iterator(include_key=False):
        assert b'value' == value

    for ret in db.iterator(include_key=False, include_value=False):
        assert ret is None


def assert_iterator_behaviour(db, iter_kwargs, expected_values):
    first, second, third = expected_values
    is_forward = not iter_kwargs.get('reverse', False)

    # Simple iteration
    it = db.iterator(**iter_kwargs)
    assert next(it) == first
    assert next(it) == second
    assert next(it) == third
    with pytest.raises(StopIteration):
        next(it)
    with pytest.raises(StopIteration):
        # second time may not cause a segfault
        next(it)

    # Single steps, both next() and .prev()
    it = db.iterator(**iter_kwargs)
    assert next(it) == first
    assert it.prev() == first
    assert next(it) == first
    assert it.prev() == first
    with pytest.raises(StopIteration):
        it.prev()
    assert next(it) == first
    assert next(it) == second
    assert next(it) == third
    with pytest.raises(StopIteration):
        next(it)
    assert it.prev() == third
    assert it.prev() == second

    # End of iterator
    it = db.iterator(**iter_kwargs)
    if is_forward:
        it.seek_to_stop()
    else:
        it.seek_to_start()
    with pytest.raises(StopIteration):
        next(it)
    assert it.prev() == third

    # Begin of iterator
    it = db.iterator(**iter_kwargs)
    if is_forward:
        it.seek_to_start()
    else:
        it.seek_to_stop()
    with pytest.raises(StopIteration):
        it.prev()
    assert next(it) == first


def test_forward_iteration(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')

    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(include_key=False),
        expected_values=(b'1', b'2', b'3'))


def test_reverse_iteration(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')

    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(reverse=True, include_key=False),
        expected_values=(b'3', b'2', b'1'))


def test_range_iteration(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')
    db.put(b'4', b'4')
    db.put(b'5', b'5')

    actual = list(db.iterator(start=b'2', include_value=False))
    expected = [b'2', b'3', b'4', b'5']
    assert actual == expected

    actual = list(db.iterator(stop=b'3', include_value=False))
    expected = [b'1', b'2']
    assert actual == expected

    actual = list(db.iterator(start=b'0', stop=b'3', include_value=False))
    expected = [b'1', b'2']
    assert actual == expected

    assert list(db.iterator(start=b'3', stop=b'0')) == []

    # Only start (inclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'3', include_key=False),
        expected_values=(b'3', b'4', b'5'))

    # Only start (exclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'2', include_key=False,
                         include_start=False),
        expected_values=(b'3', b'4', b'5'))

    # Only stop (exclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(stop=b'4', include_key=False),
        expected_values=(b'1', b'2', b'3'))

    # Only stop (inclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(stop=b'3', include_key=False, include_stop=True),
        expected_values=(b'1', b'2', b'3'))

    # Both start and stop
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'2', stop=b'5', include_key=False),
        expected_values=(b'2', b'3', b'4'))


def test_reverse_range_iteration(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')
    db.put(b'4', b'4')
    db.put(b'5', b'5')

    assert list(db.iterator(start=b'3', stop=b'0', reverse=True)) == []

    # Only start (inclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'3', reverse=True, include_value=False),
        expected_values=(b'5', b'4', b'3'))

    # Only start (exclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'2', reverse=True, include_value=False,
                         include_start=False),
        expected_values=(b'5', b'4', b'3'))

    # Only stop (exclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(stop=b'4', reverse=True, include_value=False),
        expected_values=(b'3', b'2', b'1'))

    # Only stop (inclusive)
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(stop=b'3', reverse=True, include_value=False,
                         include_stop=True),
        expected_values=(b'3', b'2', b'1'))

    # Both start and stop
    assert_iterator_behaviour(
        db,
        iter_kwargs=dict(start=b'1', stop=b'4', reverse=True,
                         include_value=False),
        expected_values=(b'3', b'2', b'1'))


def test_out_of_range_iterations(db):
    db.put(b'1', b'1')
    db.put(b'3', b'3')
    db.put(b'4', b'4')
    db.put(b'5', b'5')
    db.put(b'7', b'7')

    def t(expected, **kwargs):
        kwargs['include_value'] = False
        assert b''.join((db.iterator(**kwargs))) == expected

    # Out of range start key
    t(b'3457', start=b'2')
    t(b'3457', start=b'2', include_start=False)

    # Out of range stop key
    t(b'5431', stop=b'6', reverse=True)
    t(b'5431', stop=b'6', include_stop=True, reverse=True)

    # Out of range prefix
    t(b'', prefix=b'0', include_start=True)
    t(b'', prefix=b'0', include_start=False)
    t(b'', prefix=b'8', include_stop=True, reverse=True)
    t(b'', prefix=b'8', include_stop=False, reverse=True)


def test_range_empty_database(db):
    it = db.iterator()
    it.seek_to_start()  # no-op (don't crash)
    it.seek_to_stop()  # no-op (don't crash)

    it = db.iterator()
    with pytest.raises(StopIteration):
        next(it)

    it = db.iterator()
    with pytest.raises(StopIteration):
        it.prev()
    with pytest.raises(StopIteration):
        next(it)


def test_iterator_single_entry(db):
    key = b'key'
    value = b'value'
    db.put(key, value)

    it = db.iterator(include_value=False)
    assert next(it) == key
    assert it.prev() == key
    assert next(it) == key
    assert it.prev() == key
    with pytest.raises(StopIteration):
        it.prev()
    assert next(it) == key
    with pytest.raises(StopIteration):
        next(it)


def test_iterator_seeking(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')
    db.put(b'4', b'4')
    db.put(b'5', b'5')

    it = db.iterator(include_value=False)
    it.seek_to_start()
    with pytest.raises(StopIteration):
        it.prev()
    assert next(it) == b'1'
    it.seek_to_start()
    assert next(it) == b'1'
    it.seek_to_stop()
    with pytest.raises(StopIteration):
        next(it)
    assert it.prev() == b'5'

    # Seek to a specific key
    it.seek(b'2')
    assert next(it) == b'2'
    assert next(it) == b'3'
    assert list(it) == [b'4', b'5']
    it.seek(b'2')
    assert it.prev() == b'1'

    # Seek to keys that sort between/before/after existing keys
    it.seek(b'123')
    assert next(it) == b'2'
    it.seek(b'6')
    with pytest.raises(StopIteration):
        next(it)
    it.seek(b'0')
    with pytest.raises(StopIteration):
        it.prev()
    assert next(it) == b'1'
    it.seek(b'4')
    it.seek(b'3')
    assert next(it) == b'3'

    # Seek in a reverse iterator
    it = db.iterator(include_value=False, reverse=True)
    it.seek(b'6')
    assert next(it) == b'5'
    assert next(it) == b'4'
    it.seek(b'1')
    with pytest.raises(StopIteration):
        next(it)
    assert it.prev() == b'1'

    # Seek in iterator with start key
    it = db.iterator(start=b'2', include_value=False)
    assert next(it) == b'2'
    it.seek(b'2')
    assert next(it) == b'2'
    it.seek(b'0')
    assert next(it) == b'2'
    it.seek_to_start()
    assert next(it) == b'2'

    # Seek in iterator with stop key
    it = db.iterator(stop=b'3', include_value=False)
    assert next(it) == b'1'
    it.seek(b'2')
    assert next(it) == b'2'
    it.seek(b'5')
    with pytest.raises(StopIteration):
        next(it)
    it.seek(b'5')
    assert it.prev() == b'2'
    it.seek_to_stop()
    with pytest.raises(StopIteration):
        next(it)
    it.seek_to_stop()
    assert it.prev() == b'2'

    # Seek in iterator with both start and stop keys
    it = db.iterator(start=b'2', stop=b'5', include_value=False)
    it.seek(b'0')
    assert next(it) == b'2'
    it.seek(b'5')
    with pytest.raises(StopIteration):
        next(it)
    it.seek(b'5')
    assert it.prev() == b'4'

    # Seek in reverse iterator with start and stop key
    it = db.iterator(
        reverse=True, start=b'2', stop=b'4', include_value=False)
    it.seek(b'5')
    assert next(it) == b'3'
    it.seek(b'1')
    assert it.prev() == b'2'
    it.seek_to_start()
    with pytest.raises(StopIteration):
        next(it)
    it.seek_to_stop()
    assert next(it) == b'3'


def test_iterator_boundaries(db):
    db.put(b'1', b'1')
    db.put(b'2', b'2')
    db.put(b'3', b'3')
    db.put(b'4', b'4')
    db.put(b'5', b'5')

    def t(expected, **kwargs):
        kwargs.update(include_value=False)
        actual = b''.join(db.iterator(**kwargs))
        assert actual == expected

    t(b'12345')
    t(b'345', start=b'2', include_start=False)
    t(b'1234', stop=b'5', include_start=False)
    t(b'12345', stop=b'5', include_stop=True)
    t(b'2345', start=b'2', stop=b'5', include_stop=True)
    t(b'2345', start=b'2', stop=b'5', include_stop=True)
    t(b'345', start=b'3', include_stop=False)
    t(b'45', start=b'3', include_start=False, include_stop=False)


def test_iterator_prefix(db):
    keys = [
        b'a1', b'a2', b'a3', b'aa4', b'aa5',
        b'b1', b'b2', b'b3', b'b4', b'b5',
        b'c1', b'c\xff', b'c\x00',
        b'\xff\xff', b'\xff\xffa', b'\xff\xff\xff',
    ]
    for key in keys:
        db.put(key, b'')

    pytest.raises(TypeError, db.iterator, prefix=b'abc', start=b'a')
    pytest.raises(TypeError, db.iterator, prefix=b'abc', stop=b'a')

    def t(*args, **kwargs):
        # Positional arguments are the expected ones, keyword
        # arguments are passed to db.iterator()
        kwargs.update(include_value=False)
        it = db.iterator(**kwargs)
        assert list(it) == list(args)

    t(*sorted(keys), prefix=b'')
    t(prefix=b'd')
    t(b'b1', prefix=b'b1')
    t(b'a1', b'a2', b'a3', b'aa4', b'aa5', prefix=b'a')
    t(b'aa4', b'aa5', prefix=b'aa')
    t(b'\xff\xff', b'\xff\xffa', b'\xff\xff\xff', prefix=b'\xff\xff')

    # The include_start and include_stop make no sense, so should
    # not affect the behaviour
    t(b'a1', b'a2', b'a3', b'aa4', b'aa5',
      prefix=b'a', include_start=False, include_stop=True)


def test_snapshot(db):
    db.put(b'a', b'a')
    db.put(b'b', b'b')

    # Snapshot should have existing values, but not changed values
    snapshot = db.snapshot()
    assert snapshot.get(b'a') == b'a'
    assert list(snapshot.iterator(include_value=False)) == [b'a', b'b']
    assert snapshot.get(b'c') is None
    db.delete(b'a')
    db.put(b'c', b'c')
    assert snapshot.get(b'c') is None
    assert snapshot.get(b'c', b'd') == b'd'
    assert snapshot.get(b'c', default=b'd') == b'd'
    assert list(snapshot.iterator(include_value=False)) == [b'a', b'b']

    # New snapshot should reflect latest state
    snapshot = db.snapshot()
    assert snapshot.get(b'c') == b'c'
    assert list(snapshot.iterator(include_value=False)) == [b'b', b'c']

    # Snapshots are directly iterable, just like DB
    assert list(k for k, v in snapshot) == [b'b', b'c']


def test_snapshot_closing(db):
    # Snapshots can be closed explicitly
    snapshot = db.snapshot()
    snapshot.close()
    with pytest.raises(RuntimeError):
        snapshot.get(b'a')

    snapshot.close()  # no-op


def test_snapshot_closing_database(db):
    # Closing the db should also render the snapshot unusable
    snapshot = db.snapshot()
    db.close()
    with pytest.raises(RuntimeError):
        snapshot.get(b'a')


def test_snapshot_closing_context_manager(db):
    # Context manager
    db.put(b'a', b'a')
    snapshot = db.snapshot()
    with snapshot:
        assert snapshot.get(b'a') == b'a'
    with pytest.raises(RuntimeError):
        snapshot.get(b'a')


def test_property(db):
    with pytest.raises(TypeError):
        db.get_property()

    with pytest.raises(TypeError):
        db.get_property(42)

    assert db.get_property(b'does-not-exist') is None

    properties = [
        b'leveldb.stats',
        b'leveldb.sstables',
        b'leveldb.num-files-at-level0',
    ]
    for prop in properties:
        assert isinstance(db.get_property(prop), bytes)


def test_compaction(db):
    db.compact_range()
    db.compact_range(start=b'a', stop=b'b')
    db.compact_range(start=b'a')
    db.compact_range(stop=b'b')


def test_approximate_sizes(db_dir):
    # Write some data to a fresh database
    db = plyvel.DB(db_dir, create_if_missing=True, error_if_exists=True)
    value = b'a' * 100
    with db.write_batch() as wb:
        for i in range(1000):
            key = bytes(i) * 100
            wb.put(key, value)

    # Close and reopen the database
    db.close()
    del wb, db
    db = plyvel.DB(db_dir, create_if_missing=False)

    with pytest.raises(TypeError):
        db.approximate_size(1, 2)

    with pytest.raises(TypeError):
        db.approximate_sizes(None)

    with pytest.raises(TypeError):
        db.approximate_sizes((1, 2))

    # Test single range
    assert db.approximate_size(b'1', b'2') >= 0

    # Test multiple ranges
    assert db.approximate_sizes() == []
    assert db.approximate_sizes((b'1', b'2'))[0] >= 0

    ranges = [
        (b'1', b'3'),
        (b'', b'\xff'),
    ]
    assert len(db.approximate_sizes(*ranges)) == len(ranges)


def test_repair_db(db_dir):
    db = plyvel.DB(db_dir, create_if_missing=True)
    db.put(b'foo', b'bar')
    db.close()
    del db

    plyvel.repair_db(db_dir)
    db = plyvel.DB(db_dir)
    assert db.get(b'foo') == b'bar'


def test_destroy_db(db_dir):
    db_dir = os.path.join(db_dir, 'subdir')
    db = plyvel.DB(db_dir, create_if_missing=True)
    db.put(b'foo', b'bar')
    db.close()
    del db

    plyvel.destroy_db(db_dir)
    assert not os.path.lexists(db_dir)


def test_threading(db):
    randint = random.randint

    N_THREADS_PER_FUNC = 5

    def bulk_insert(db):
        name = threading.current_thread().name
        v = name.encode('ascii') * randint(300, 700)
        for n in range(randint(1000, 8000)):
            rev = '{0:x}'.format(n)[::-1]
            k = '{0}: {1}'.format(rev, name).encode('ascii')
            db.put(k, v)

    def iterate_full(db):
        for i in range(randint(4, 7)):
            for key, value in db.iterator(reverse=True):
                pass

    def iterate_short(db):
        for i in range(randint(200, 700)):
            it = db.iterator()
            list(itertools.islice(it, randint(50, 100)))

    def close_db(db):
        time.sleep(1)
        db.close()

    funcs = [
        bulk_insert,
        iterate_full,
        iterate_short,

        # XXX: This this will usually cause a segfault since
        # unexpectedly closing a database may crash threads using
        # iterators:
        # close_db,
    ]

    threads = []
    for func in funcs:
        for n in range(N_THREADS_PER_FUNC):
            t = threading.Thread(target=func, args=(db,))
            t.start()
            threads.append(t)

    for t in threads:
        t.join()


def test_invalid_comparator(db_dir):
    with pytest.raises(ValueError):
        plyvel.DB(db_dir, comparator=None, comparator_name=b'invalid')

    with pytest.raises(TypeError):
        plyvel.DB(db_dir, comparator=lambda x, y: 1, comparator_name=12)

    with pytest.raises(TypeError):
        plyvel.DB(db_dir, comparator=b'not-a-callable',
                  comparator_name=b'invalid')


def test_comparator(db_dir):
    def comparator(a, b):
        a = a.lower()
        b = b.lower()
        if a < b:
            return -1
        if a > b:
            return 1
        else:
            return 0

    comparator_name = b"CaseInsensitiveComparator"

    db = plyvel.DB(
        db_dir,
        create_if_missing=True,
        comparator=comparator,
        comparator_name=comparator_name)

    keys = [
        b'aaa',
        b'BBB',
        b'ccc',
    ]

    with db.write_batch() as wb:
        for key in keys:
            wb.put(key, b'')

    expected = sorted(keys, key=lambda s: s.lower())
    actual = list(db.iterator(include_value=False))
    assert actual == expected


def test_prefixed_db(db):
    for prefix in (b'a', b'b'):
        for i in range(1000):
            key = prefix + '{0:03d}'.format(i).encode('ascii')
            db.put(key, b'')

    db_a = db.prefixed_db(b'a')
    db_b = db.prefixed_db(b'b')

    # Access original db
    assert db_a.db is db

    # Basic operations
    key = b'123'
    assert db_a.get(key) is not None
    db_a.put(key, b'foo')
    assert db_a.get(key) == b'foo'
    db_a.delete(key)
    assert db_a.get(key) is None
    assert db_a.get(key, b'v') == b'v'
    assert db_a.get(key, default=b'v') == b'v'
    db_a.put(key, b'foo')
    assert db.get(b'a123') == b'foo'

    # Iterators
    assert len(list(db_a)) == 1000
    it = db_a.iterator(include_value=False)
    assert next(it) == b'000'
    assert next(it) == b'001'
    assert len(list(it)) == 998
    it = db_a.iterator(start=b'900')
    assert len(list(it)) == 100
    it = db_a.iterator(stop=b'012', include_stop=False)
    assert len(list(it)) == 12
    it = db_a.iterator(stop=b'012', include_stop=True)
    assert len(list(it)) == 13
    it = db_a.iterator(include_stop=True)
    assert len(list(it)) == 1000
    it = db_a.iterator(prefix=b'10')
    assert len(list(it)) == 10
    it = db_a.iterator(include_value=False)
    it.seek(b'500')
    assert len(list(it)) == 500
    it.seek_to_start()
    assert len(list(it)) == 1000
    it.seek_to_stop()
    assert it.prev() == b'999'
    it = db_b.iterator()
    it.seek_to_start()
    with pytest.raises(StopIteration):
        it.prev()
    it.seek_to_stop()
    with pytest.raises(StopIteration):
        next(it)

    # Snapshots
    sn_a = db_a.snapshot()
    assert sn_a.get(b'042') == b''
    db_a.put(b'042', b'new')
    assert sn_a.get(b'042') == b''
    assert db_a.get(b'042') == b'new'
    assert db_a.get(b'foo', b'x') == b'x'
    assert db_a.get(b'foo', default=b'x') == b'x'

    # Snapshot iterators
    sn_a.iterator()
    it = sn_a.iterator(
        start=b'900', include_start=False, include_value=False)
    assert next(it) == b'901'
    assert len(list(it)) == 98

    # Write batches
    wb = db_a.write_batch()
    wb.put(b'0002', b'foo')
    wb.delete(b'0003')
    wb.write()
    assert db_a.get(b'0002') == b'foo'
    assert db_a.get(b'0003') is None

    # Delete all data in db_a
    for key in db_a.iterator(include_value=False):
        db_a.delete(key)
    assert len(list(db_a)) == 0

    # The complete db and the 'b' prefix should remain untouched
    assert len(list(db)) == 1000
    assert len(list(db_b)) == 1000

    # Prefixed prefixed databases (recursive)
    db_b12 = db_b.prefixed_db(b'12')
    it = db_b12.iterator(include_value=False)
    assert next(it) == b'0'
    assert len(list(it)) == 9


def test_raw_iterator(db):
    for i in range(1000):
        key = value = '{0:03d}'.format(i).encode('ascii')
        db.put(key, value)

    it = db.raw_iterator()
    it.seek_to_first()
    assert it.key() == b'000'
    assert it.value() == b'000'
    it.next()
    assert it.key() == b'001'
    assert it.value() == b'001'
    it.next()
    assert it.key() == b'002'
    it.next()
    it.next()
    assert it.key() == b'004'
    it.prev()
    assert it.key() == b'003'

    it.seek_to_first()
    it.prev()
    with pytest.raises(plyvel.IteratorInvalidError):
        it.value()
    assert not it.valid()

    it.seek(b'005')
    assert it.key() == b'005'

    it.seek(b'006abc')
    assert it.key() == b'007'

    it.seek_to_last()
    assert it.key() == b'999'

    it.next()
    with pytest.raises(plyvel.IteratorInvalidError):
        it.key()


def test_raw_iterator_empty_db(db):
    it = db.raw_iterator()
    assert not it.valid()

    it.seek_to_first()
    assert not it.valid()

    it.seek_to_last()
    assert not it.valid()

    with pytest.raises(plyvel.IteratorInvalidError):
        it.key()


def test_raw_iterator_snapshot(db):
    sn = db.snapshot()
    db.put(b'001', b'')
    it = sn.raw_iterator()
    it.seek_to_first()
    assert not it.valid()
    with pytest.raises(plyvel.IteratorInvalidError):
        it.key()


def test_raw_iterator_closing(db):
    it = db.raw_iterator()
    it.close()
    with pytest.raises(RuntimeError):
        it.next()

    with pytest.raises(RuntimeError):
        with db.raw_iterator() as it:
            pass
        it.valid()


def test_access_DB_name_attr(db_dir):
    db = plyvel.DB(db_dir, create_if_missing=True)
    assert db.name == db_dir


def test_try_changing_DB_name_attr(db_dir):
    db = plyvel.DB(db_dir, create_if_missing=True)
    with pytest.raises(AttributeError):
        db.name = 'a'
