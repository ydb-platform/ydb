from yarl import URL

# comparison and hashing


def test_ne_str() -> None:
    url = URL("http://example.com/")
    assert url != "http://example.com/"


def test_eq() -> None:
    url = URL("http://example.com/")
    assert url == URL("http://example.com/")


def test_hash() -> None:
    assert hash(URL("http://example.com/")) == hash(URL("http://example.com/"))


def test_hash_double_call() -> None:
    url = URL("http://example.com/")
    assert hash(url) == hash(url)


def test_le_less() -> None:
    url1 = URL("http://example1.com/")
    url2 = URL("http://example2.com/")

    assert url1 <= url2


def test_le_eq() -> None:
    url1 = URL("http://example.com/")
    url2 = URL("http://example.com/")

    assert url1 <= url2


def test_le_not_implemented() -> None:
    url = URL("http://example1.com/")

    assert url.__le__(123) is NotImplemented


def test_lt() -> None:
    url1 = URL("http://example1.com/")
    url2 = URL("http://example2.com/")

    assert url1 < url2


def test_lt_not_implemented() -> None:
    url = URL("http://example1.com/")

    assert url.__lt__(123) is NotImplemented


def test_ge_more() -> None:
    url1 = URL("http://example1.com/")
    url2 = URL("http://example2.com/")

    assert url2 >= url1


def test_ge_eq() -> None:
    url1 = URL("http://example.com/")
    url2 = URL("http://example.com/")

    assert url2 >= url1


def test_ge_not_implemented() -> None:
    url = URL("http://example1.com/")

    assert url.__ge__(123) is NotImplemented


def test_gt() -> None:
    url1 = URL("http://example1.com/")
    url2 = URL("http://example2.com/")

    assert url2 > url1


def test_gt_not_implemented() -> None:
    url = URL("http://example1.com/")

    assert url.__gt__(123) is NotImplemented
