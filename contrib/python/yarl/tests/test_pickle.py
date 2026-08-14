import pickle
from urllib.parse import SplitResult

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


def test_pickle_legacy_splitresult_state() -> None:
    """Pickles produced by older yarl releases embedded a ``SplitResult``.

    Loading such bytes must still rebuild the URL so users upgrading from
    pre-fix versions do not lose access to their stored data.
    """
    val = ("http", "example.com", "/p", "q=1", "frag")
    legacy_state = (tuple.__new__(SplitResult, val),)
    u = URL.__new__(URL)
    u.__setstate__(legacy_state)
    assert u._scheme == "http"
    assert u._netloc == "example.com"
    assert u._path == "/p"
    assert u._query == "q=1"
    assert u._fragment == "frag"


def test_pickle_getstate_returns_plain_tuple() -> None:
    """Regression test for gh-1632.

    Python 3.15 added a ``SplitResult.__getstate__`` that touches instance
    attributes set by ``__init__``. yarl previously embedded a ``SplitResult``
    built via ``tuple.__new__``, bypassing init, which made pickling crash.
    ``__getstate__`` must return a plain ``tuple`` so pickling never invokes
    ``SplitResult.__getstate__``.
    """
    u = URL("http://example.com/path?q=1#frag")
    state = u.__getstate__()
    assert len(state) == 1
    assert type(state[0]) is tuple
    assert state[0] == ("http", "example.com", "/path", "q=1", "frag")
