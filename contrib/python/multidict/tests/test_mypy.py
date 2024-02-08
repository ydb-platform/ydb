# This file doesn't use test parametrization because mypy doesn't nothing about it.
# Concrete types are required

import multidict


def test_classes_not_abstract() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    multidict.MultiDictProxy(d1)
    multidict.CIMultiDictProxy(d2)


def test_getitem() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("a")

    assert d1["a"] == "b"
    assert d2["a"] == "b"
    assert d3["a"] == "b"
    assert d4["a"] == "b"

    assert d1[key] == "b"
    assert d2[key] == "b"
    assert d3[key] == "b"
    assert d4[key] == "b"


def test_get() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("a")

    assert d1.get("a") == "b"
    assert d2.get("a") == "b"
    assert d3.get("a") == "b"
    assert d4.get("a") == "b"

    assert d1.get(key) == "b"
    assert d2.get(key) == "b"
    assert d3.get(key) == "b"
    assert d4.get(key) == "b"


def test_get_default() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("b")

    assert d1.get("b", "d") == "d"
    assert d2.get("b", "d") == "d"
    assert d3.get("b", "d") == "d"
    assert d4.get("b", "d") == "d"

    assert d1.get(key, "d") == "d"
    assert d2.get(key, "d") == "d"
    assert d3.get(key, "d") == "d"
    assert d4.get(key, "d") == "d"


def test_getone() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("a")

    assert d1.getone("a") == "b"
    assert d2.getone("a") == "b"
    assert d3.getone("a") == "b"
    assert d4.getone("a") == "b"

    assert d1.getone(key) == "b"
    assert d2.getone(key) == "b"
    assert d3.getone(key) == "b"
    assert d4.getone(key) == "b"


def test_getone_default() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("b")

    assert d1.getone("b", 1) == 1
    assert d2.getone("b", 1) == 1
    assert d3.getone("b", 1) == 1
    assert d4.getone("b", 1) == 1

    assert d1.getone(key, 1) == 1
    assert d2.getone(key, 1) == 1
    assert d3.getone(key, 1) == 1
    assert d4.getone(key, 1) == 1


def test_getall() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("a")

    assert d1.getall("a") == ["b"]
    assert d2.getall("a") == ["b"]
    assert d3.getall("a") == ["b"]
    assert d4.getall("a") == ["b"]

    assert d1.getall(key) == ["b"]
    assert d2.getall(key) == ["b"]
    assert d3.getall(key) == ["b"]
    assert d4.getall(key) == ["b"]


def test_getall_default() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    key = multidict.istr("b")

    assert d1.getall("b", 1) == 1
    assert d2.getall("b", 1) == 1
    assert d3.getall("b", 1) == 1
    assert d4.getall("b", 1) == 1

    assert d1.getall(key, 1) == 1
    assert d2.getall(key, 1) == 1
    assert d3.getall(key, 1) == 1
    assert d4.getall(key, 1) == 1


def test_copy() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    assert d1.copy() == d1
    assert d2.copy() == d2
    assert d3.copy() == d1
    assert d4.copy() == d2


def test_iter() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    d3 = multidict.MultiDictProxy(d1)
    d4 = multidict.CIMultiDictProxy(d2)

    for i in d1:
        i.lower()  # str-specific class
    for i in d2:
        i.lower()  # str-specific class
    for i in d3:
        i.lower()  # str-specific class
    for i in d4:
        i.lower()  # str-specific class


def test_setitem() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    key = multidict.istr("a")

    d1["a"] = "b"
    d2["a"] = "b"

    d1[key] = "b"
    d2[key] = "b"

def test_delitem() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    del d1["a"]
    del d2["a"]

    key = multidict.istr("a")

    d3: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d4: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    del d3[key]
    del d4[key]


def test_additem() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    key = multidict.istr("a")

    d1.add("a", "b")
    d2.add("a", "b")

    d1.add(key, "b")
    d2.add(key, "b")


def test_extend_mapping() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    key = multidict.istr("a")

    d1.extend({"a": "b"})
    d2.extend({"a": "b"})

    d1.extend({key: "b"})
    d2.extend({key: "b"})


def test_update_mapping() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    key = multidict.istr("a")

    d1.update({"a": "b"})
    d2.update({"a": "b"})

    d1.update({key: "b"})
    d2.update({key: "b"})

def test_popone() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    assert d1.popone("a") == "b"
    assert d2.popone("a") == "b"

    key = multidict.istr("a")
    d1 = multidict.MultiDict({"a": "b"})
    d2 = multidict.CIMultiDict({"a": "b"})

    assert d1.popone(key) == "b"
    assert d2.popone(key) == "b"


def test_popall() -> None:
    d1: multidict.MultiDict[str] = multidict.MultiDict({"a": "b"})
    d2: multidict.CIMultiDict[str] = multidict.CIMultiDict({"a": "b"})

    assert d1.popall("a") == ["b"]
    assert d2.popall("a") == ["b"]

    key = multidict.istr("a")
    d1 = multidict.MultiDict({"a": "b"})
    d2 = multidict.CIMultiDict({"a": "b"})

    assert d1.popall(key) == ["b"]
    assert d2.popall(key) == ["b"]
