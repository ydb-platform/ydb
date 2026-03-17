import h3.api.basic_str as h3


def test1():
    assert h3.geo_to_h3(37.7752702151959, -122.418307270836, 9) == '8928308280fffff'


def test5():
    expected = {
        '89283082873ffff',
        '89283082877ffff',
        '8928308283bffff',
        '89283082807ffff',
        '8928308280bffff',
        '8928308280fffff',
        '89283082803ffff'
    }

    out = h3.k_ring('8928308280fffff', 1)
    assert out == expected
