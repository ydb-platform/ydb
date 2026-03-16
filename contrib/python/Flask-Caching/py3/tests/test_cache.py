import random
import time

import pytest

from flask_caching import Cache

try:
    import redis  # noqa

    HAS_NOT_REDIS = False
except ImportError:
    HAS_NOT_REDIS = True


def test_cache_set(app, cache):
    cache.set("hi", "hello")

    assert cache.get("hi") == "hello"


def test_cache_has(app, cache):
    cache.add("hi", "hello")
    assert cache.has("hi")


def test_cache_add(app, cache):
    cache.add("hi", "hello")
    assert cache.get("hi") == "hello"

    cache.add("hi", "foobar")
    assert cache.get("hi") == "hello"


def test_cache_delete(app, cache):
    cache.set("hi", "hello")
    cache.delete("hi")
    assert cache.get("hi") is None


def test_cache_delete_many(app, cache):
    cache.set("hi", "hello")
    cache.delete_many("ho", "hi")
    assert cache.get("hi") is not None


@pytest.mark.skipif(HAS_NOT_REDIS, reason="requires Redis")
def test_cache_unlink(app, redis_server):
    cache = Cache(config={"CACHE_TYPE": "redis"})
    cache.init_app(app)
    cache.set("biggerkey", "test" * 100)
    cache.unlink("biggerkey")
    assert cache.get("biggerkey") is None

    cache.set("biggerkey1", "test" * 100)
    cache.set("biggerkey2", "test" * 100)
    cache.unlink("biggerkey1", "biggerkey2")
    assert cache.get("biggerkey1") is None
    assert cache.get("biggerkey2") is None


def test_cache_unlink_if_not(app):
    cache = Cache(config={"CACHE_TYPE": "simple"})
    cache.init_app(app)
    cache.set("biggerkey", "test" * 100)
    cache.unlink("biggerkey")
    assert cache.get("biggerkey") is None

    cache.set("biggerkey1", "test" * 100)
    cache.set("biggerkey2", "test" * 100)
    cache.unlink("biggerkey1", "biggerkey2")
    assert cache.get("biggerkey1") is None
    assert cache.get("biggerkey2") is None


def test_cache_delete_many_ignored(app):
    cache = Cache(config={"CACHE_TYPE": "simple", "CACHE_IGNORE_ERRORS": True})
    cache.init_app(app)

    cache.set("hi", "hello")
    assert cache.get("hi") == "hello"
    cache.delete_many("ho", "hi")
    assert cache.get("hi") is None


def test_cache_cached_function(app, cache):
    with app.test_request_context():

        @cache.cached(1, key_prefix="MyBits")
        def get_random_bits():
            return [random.randrange(0, 2) for i in range(50)]

        my_list = get_random_bits()
        his_list = get_random_bits()

        assert my_list == his_list

        time.sleep(2)

        his_list = get_random_bits()

        assert my_list != his_list


def test_cache_cached_function_with_source_check_enabled(app, cache):
    with app.test_request_context():

        @cache.cached(key_prefix="MyBits", source_check=True)
        def get_random_bits():
            return [random.randrange(0, 2) for i in range(50)]

        first_attempt = get_random_bits()
        second_attempt = get_random_bits()

        assert second_attempt == first_attempt

        # ... change the source  to see if the return value changes when called
        @cache.cached(key_prefix="MyBits", source_check=True)
        def get_random_bits():
            return {"val": [random.randrange(0, 2) for i in range(50)]}

        third_attempt = get_random_bits()

        assert third_attempt != first_attempt
        # We changed the return data type so we do a check to be sure
        assert isinstance(third_attempt, dict)

        # ... change the source back to what it was original and the data should
        # be the same
        @cache.cached(key_prefix="MyBits", source_check=True)
        def get_random_bits():
            return [random.randrange(0, 2) for i in range(50)]

        forth_attempt = get_random_bits()

        assert forth_attempt == first_attempt


def test_cache_cached_function_with_source_check_disabled(app, cache):
    with app.test_request_context():

        @cache.cached(key_prefix="MyBits", source_check=False)
        def get_random_bits():
            return [random.randrange(0, 2) for i in range(50)]

        first_attempt = get_random_bits()
        second_attempt = get_random_bits()

        assert second_attempt == first_attempt

        # ... change the source  to see if the return value changes when called
        @cache.cached(key_prefix="MyBits", source_check=False)
        def get_random_bits():
            return {"val": [random.randrange(0, 2) for i in range(50)]}

        third_attempt = get_random_bits()

        assert third_attempt == first_attempt


def test_cache_accepts_multiple_ciphers(app, cache, hash_method):
    with app.test_request_context():

        @cache.cached(1, key_prefix="MyBits", hash_method=hash_method)
        def get_random_bits():
            return [random.randrange(0, 2) for i in range(50)]

        my_list = get_random_bits()
        his_list = get_random_bits()

        assert my_list == his_list

        time.sleep(2)

        his_list = get_random_bits()

        assert my_list != his_list


def test_cached_none(app, cache):
    with app.test_request_context():
        from collections import Counter

        call_counter = Counter()

        @cache.cached(cache_none=True)
        def cache_none(param):
            call_counter[param] += 1

            return None

        cache_none(1)

        assert call_counter[1] == 1
        assert cache_none(1) is None
        assert call_counter[1] == 1

        cache.clear()

        cache_none(1)
        assert call_counter[1] == 2


def test_cached_doesnt_cache_none(app, cache):
    """Asserting that when cache_none is False, we always
    assume a None value returned from .get() means the key is not found
    """
    with app.test_request_context():
        from collections import Counter

        call_counter = Counter()

        @cache.cached()
        def cache_none(param):
            call_counter[param] += 1

            return None

        cache_none(1)

        # The cached function should have been called
        assert call_counter[1] == 1

        # Next time we call the function, the value should be coming from the cache…
        # But the value is None and so we treat it as uncached.
        assert cache_none(1) is None

        # …thus, the call counter should increment to 2
        assert call_counter[1] == 2

        cache.clear()

        cache_none(1)
        assert call_counter[1] == 3


def test_cache_forced_update(app, cache):
    from collections import Counter

    with app.test_request_context():
        need_update = False
        call_counter = Counter()

        @cache.cached(1, forced_update=lambda: need_update)
        def cached_function(param):
            call_counter[param] += 1

            return 1

        cached_function(1)
        assert call_counter[1] == 1

        assert cached_function(1) == 1
        assert call_counter[1] == 1

        need_update = True

        assert cached_function(1) == 1
        assert call_counter[1] == 2


def test_cache_forced_update_params(app, cache):
    from collections import Counter

    with app.test_request_context():
        cached_call_counter = Counter()
        call_counter = Counter()
        call_params = {}

        def need_update(param):
            """This helper function returns True if it has been called with
            the same params for more than 2 times
            """

            call_counter[param] += 1
            call_params[call_counter[param] - 1] = (param,)

            return call_counter[param] > 2

        @cache.cached(1, forced_update=need_update)
        def cached_function(param):
            cached_call_counter[param] += 1

            return 1

        assert cached_function(1) == 1
        # need_update should have been called once
        assert call_counter[1] == 1
        # the parameters used to call need_update should be the same as the
        # parameters used to call cached_function
        assert call_params[0] == (1,)
        # the cached function should have been called once
        assert cached_call_counter[1] == 1

        assert cached_function(1) == 1
        # need_update should have been called twice by now as forced_update
        # should be called regardless of the arguments
        assert call_counter[1] == 2
        # the parameters used to call need_update should be the same as the
        # parameters used to call cached_function
        assert call_params[1] == (1,)
        # this time the forced_update should have returned False, so
        # cached_function should not have been called again
        assert cached_call_counter[1] == 1

        assert cached_function(1) == 1
        # need_update should have been called thrice by now as forced_update
        # should be called regardless of the arguments
        assert call_counter[1] == 3
        # the parameters used to call need_update should be the same as the
        # parameters used to call cached_function
        assert call_params[1] == (1,)
        # this time the forced_update should have returned True, so
        # cached_function should have been called again
        assert cached_call_counter[1] == 2


def test_generator(app, cache):
    """test function return generator"""
    with app.test_request_context():

        @cache.cached()
        def gen():
            return (str(time.time()) for i in range(2))

        time_str = gen()
        time.sleep(1)
        assert gen() == time_str

        @cache.cached()
        def gen_yield():
            yield str(time.time())
            yield str(time.time())

        time_str = gen_yield()
        time.sleep(1)
        assert gen_yield() == time_str
