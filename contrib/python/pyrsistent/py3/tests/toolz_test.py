from pyrsistent import get_in, m, v


def test_get_in():
    # This is not an extensive test. The doctest covers that fairly good though.
    get_in(m(a=v(1, 2, 3)), ['m', 1]) == 2
