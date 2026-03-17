import re


def test_Any():
    from uc_micro.properties import Any

    assert re.search(Any.regex.REGEX, "A")
    assert not re.search(Any.regex.REGEX, "")


def test_Cc():
    from uc_micro.categories import Cc

    assert re.search(Cc.REGEX, "\r")
    assert not re.search(Cc.REGEX, "A")


def test_Cf():
    from uc_micro.categories import Cf

    assert re.search(Cf.REGEX, "\xAD")
    assert not re.search(Cf.REGEX, "A")


def test_P():
    from uc_micro.categories import P

    assert re.search(P.REGEX, ",")
    assert not re.search(P.REGEX, "A")


def test_Z():
    from uc_micro.categories import Z

    assert re.search(Z.REGEX, " ")
    assert re.search(Z.REGEX, "\u2028")
    assert re.search(Z.REGEX, "\u2029")
    assert not re.search(Z.REGEX, "A")
