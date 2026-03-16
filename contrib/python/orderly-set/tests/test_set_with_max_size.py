import pytest
from orderly_set import RoughMaxSizeSet  # adjust import path as needed


@pytest.fixture
def cache():
    # small size for testing
    return RoughMaxSizeSet(max_size=5, eviction_batch=2)

class TestRoughMaxSizeSet:
    def test_add_new_item_returns_true(self, cache):
        assert cache.add('a') is True
        assert 'a' in cache
        assert len(cache) == 1

    def test_add_existing_item_returns_false(self, cache):
        cache.add('x')
        assert cache.add('x') is False
        assert len(cache) == 1

    def test_no_eviction_below_threshold(self, cache):
        # fill up to max_size + batch, but not over
        for i in range( cache.max_size + cache.eviction_batch ):
            cache.add(i)
        assert len(cache) == cache.max_size + cache.eviction_batch

    def test_eviction_when_exceeding_threshold(self, cache):
        # add one more than threshold
        total = cache.max_size + cache.eviction_batch + 1
        for i in range(total):
            cache.add(i)
        # should trim back to max_size
        assert len(cache) == cache.max_size
        # oldest should be gone
        assert 0 not in cache
        # newest max_size items should remain
        remaining = set(list(range(total))[ -cache.max_size : ])  # last max_size integers
        assert set(cache) == remaining

    def test_len_and_iteration(self, cache):
        items = ['a', 'b', 'c']
        for x in items:
            cache.add(x)
        assert len(cache) == len(items)
        assert set(cache) == set(items)

    def test_repr_shows_order_and_size(self, cache):
        for x in ['x', 'y', 'z']:
            cache.add(x)
        rep = repr(cache)
        assert "['x', 'y', 'z']" in rep
        assert f"max_size={cache.max_size}" in rep

    def test_custom_eviction_batch(self):
        custom = RoughMaxSizeSet(max_size=3, eviction_batch=1)
        for i in [10, 20, 30, 40]:
            custom.add(i)
        # size stays at 4 (max_size + eviction_batch), no eviction yet
        assert len(custom) == 4
        assert 10 in custom

    def test_reinsertion_of_evicted_item(self, cache):
        # force eviction
        for i in range(cache.max_size + cache.eviction_batch + 1):
            cache.add(i)
        # 0 was evicted; re-adding should work
        assert cache.add(0) is True
        assert 0 in cache
        assert len(cache) == cache.max_size + 1
