"""
Tests for the AttrMap class.
"""


def test_repr():
    """
    repr(AttrMap)
    """
    from attrdict.mapping import AttrMap

    assert repr(AttrMap()) == "AttrMap({})"
    assert repr(AttrMap({'foo': 'bar'})) == "AttrMap({'foo': 'bar'})"
    assert repr(AttrMap({1: {'foo': 'bar'}})) == "AttrMap({1: {'foo': 'bar'}})"
    assert repr(AttrMap({1: AttrMap({'foo': 'bar'})})) == "AttrMap({1: AttrMap({'foo': 'bar'})})"
