import pickle

from yarl import URL

# serialize


def test_pickle() -> None:
    u1 = URL("picklepickle")
    hash(u1)
    v = pickle.dumps(u1)
    u2 = pickle.loads(v)
    assert u1._cache
    assert not u2._cache
    assert hash(u1) == hash(u2)


def test_default_style_state() -> None:
    u = object.__new__(URL)
    val = ("set_state", "set_state", "set_state", "set_state", "set_state")
    u.__setstate__((None, {"_val": val}))
    assert u._val == val
    assert hash(u) != 1


def test_empty_url_is_not_cached() -> None:
    u = URL.__new__(URL)
    val = ("set_state", "set_state", "set_state", "set_state", "set_state")
    u.__setstate__((None, {"_val": val}))
    assert u._val == val
    assert hash(u) != 1


def test_pickle_does_not_pollute_cache() -> None:
    """Verify the unpickling does not pollute the cache.

    Since unpickle will call URL.__new__ with default
    args, we need to make sure that default args never
    end up in the pre_encoded_url or encode_url cache.
    """
    u1 = URL.__new__(URL)
    u1._scheme = "this"
    u1._netloc = "never.appears.any.where.else.in.tests"
    u1._path = ""
    u1._query = ""
    u1._fragment = ""
    hash(u1)
    v = pickle.dumps(u1)
    u2: URL = pickle.loads(v)
    assert u1._cache
    assert hash(u1) == hash(u2)
    assert u2._scheme == "this"
    assert u2._netloc == "never.appears.any.where.else.in.tests"
    assert u2._path == ""
    assert u2._query == ""
    assert u2._fragment == ""
    # Verify unpickling did not the cache wrong scheme
    # for empty args.
    assert URL().scheme == ""
    assert URL("").scheme == ""
