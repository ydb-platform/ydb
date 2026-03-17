from babel.messages.pofile import _NormalizedString


def test_normalized_string():
    ab1 = _NormalizedString('a', 'b ')
    ab2 = _NormalizedString('a', ' b')
    ac1 = _NormalizedString('a', 'c')
    ac2 = _NormalizedString('  a', 'c  ')
    z = _NormalizedString()
    assert ab1 == ab2 and ac1 == ac2  # __eq__
    assert ab1 < ac1  # __lt__
    assert ac1 > ab2  # __gt__
    assert ac1 >= ac2  # __ge__
    assert ab1 <= ab2  # __le__
    assert ab1 != ac1  # __ne__
    assert not z  # __nonzero__ / __bool__
    assert sorted([ab1, ab2, ac1])  # the sort order is not stable so we can't really check it, just that we can sort
