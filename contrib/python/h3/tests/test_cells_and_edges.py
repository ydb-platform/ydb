import h3
import pytest

from h3 import (
    H3ValueError,
    H3CellError,
    H3ResolutionError,
    H3EdgeError,
    H3DistanceError,
)


def approx2(a, b):
    if len(a) != len(b):
        return False

    return all(
        x == pytest.approx(y)
        for x, y in zip(a, b)
    )


def test1():
    assert h3.geo_to_h3(37.7752702151959, -122.418307270836, 9) == '8928308280fffff'


def test2():
    h = '8928308280fffff'
    expected = (37.77670234943567, -122.41845932318311)

    assert h3.h3_to_geo(h) == pytest.approx(expected)


def test3():
    expected = (
        (37.775197782893386, -122.41719971841658),
        (37.77688044840226, -122.41612835779264),
        (37.778385004930925, -122.4173879761762),
        (37.77820687262238, -122.41971895414807),
        (37.77652420699321, -122.42079024541877),
        (37.775019673792606, -122.4195306280734),
    )

    out = h3.h3_to_geo_boundary('8928308280fffff')
    assert approx2(out, expected)


def test4():
    expected = (
        (-122.41719971841658, 37.775197782893386),
        (-122.41612835779264, 37.77688044840226),
        (-122.4173879761762, 37.778385004930925),
        (-122.41971895414807, 37.77820687262238),
        (-122.42079024541877, 37.77652420699321),
        (-122.4195306280734, 37.775019673792606),
        (-122.41719971841658, 37.775197782893386)
    )

    out = h3.h3_to_geo_boundary('8928308280fffff', geo_json=True)
    assert approx2(out, expected)


def test_k_ring_distance():
    with pytest.raises(H3DistanceError):
        h3.k_ring('8928308280fffff', -10)


def test_hex_ring_distance():
    with pytest.raises(H3DistanceError):
        h3.hex_ring('8928308280fffff', -10)


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


def test6():
    expected = {'8928308280fffff'}
    out = h3.hex_ring('8928308280fffff', 0)
    assert out == expected


def test7():
    expected = {
        '89283082803ffff',
        '89283082807ffff',
        '8928308280bffff',
        '8928308283bffff',
        '89283082873ffff',
        '89283082877ffff'
    }

    out = h3.hex_ring('8928308280fffff', 1)
    assert out == expected


def test8():
    assert h3.h3_is_valid('89283082803ffff')
    assert not h3.h3_is_valid('abc')

    # looks like it might be valid, but it isn't!
    h_bad = '8a28308280fffff'
    assert not h3.h3_is_valid(h_bad)

    # other methods should validate and raise exception if bad input
    with pytest.raises(H3CellError):
        h3.h3_get_resolution(h_bad)


def test9():
    assert h3.h3_get_resolution('8928308280fffff') == 9
    assert h3.h3_get_resolution('8a28308280f7fff') == 10


def test_parent():
    h = '8928308280fffff'

    assert h3.h3_to_parent(h, 7) == '872830828ffffff'
    assert h3.h3_to_parent(h, 8) == '8828308281fffff'
    assert h3.h3_to_parent(h, 9) == h

    with pytest.raises(H3ResolutionError):
        h3.h3_to_parent(h, 10)


def test_children():
    h = '8928308280fffff'

    # one above should raise an exception
    with pytest.raises(H3ResolutionError):
        h3.h3_to_children(h, 8)

    # same resolution is set of just cell itself
    out = h3.h3_to_children(h, 9)
    assert out == {h}

    # one below should give children
    expected = {
        '8a28308280c7fff',
        '8a28308280cffff',
        '8a28308280d7fff',
        '8a28308280dffff',
        '8a28308280e7fff',
        '8a28308280effff',
        '8a28308280f7fff'
    }
    out = h3.h3_to_children(h, 10)
    assert out == expected

    # finest resolution cell should return error for children
    h = '8f04ccb2c45e225'
    with pytest.raises(H3ResolutionError):
        h3.h3_to_children(h)


def test_center_child():
    h = '8928308280fffff'

    # one above should raise an exception
    with pytest.raises(H3ResolutionError):
        h3.h3_to_center_child(h, 8)

    # same resolution should be same cell
    assert h3.h3_to_center_child(h, 9) == h

    # one below should give direct child
    expected = '8a28308280c7fff'
    assert h3.h3_to_center_child(h, 10) == expected

    # finest resolution hex should return error for child
    h = '8f04ccb2c45e225'
    with pytest.raises(H3ResolutionError):
        h3.h3_to_center_child(h)


def test_distance():
    h = '8a28308280c7fff'
    assert h3.h3_distance(h, h) == 0

    n = h3.hex_ring(h, 1).pop()
    assert h3.h3_distance(h, n) == 1

    n = h3.hex_ring(h, 2).pop()
    assert h3.h3_distance(h, n) == 2


def test_distance_error():
    h1 = '8353b0fffffffff'
    h2 = '835804fffffffff'

    with pytest.raises(H3ValueError):
        h3.h3_distance(h1, h2)


def test_compact(in_test=False):

    # lat/lngs for State of Maine
    maine = [
        (45.137451890638886, -67.13734351262877),
        (44.8097, -66.96466),
        (44.3252, -68.03252),
        (43.98, -69.06),
        (43.68405, -70.11617),
        (43.090083319667144, -70.64573401557249),
        (43.08003225358635, -70.75102474636725),
        (43.21973948828747, -70.79761105007827),
        (43.36789581966826, -70.98176001655037),
        (43.46633942318431, -70.94416541205806),
        (45.3052400000002, -71.08482),
        (45.46022288673396, -70.6600225491012),
        (45.914794623389355, -70.30495378282376),
        (46.69317088478567, -70.00014034695016),
        (47.44777598732787, -69.23708614772835),
        (47.184794623394396, -68.90478084987546),
        (47.35462921812177, -68.23430497910454),
        (47.066248887716995, -67.79035274928509),
        (45.702585354182816, -67.79141211614706),
        (45.137451890638886, -67.13734351262877)
    ]

    res = 5

    h_uncomp = h3.polyfill_polygon(maine, res)
    h_comp = h3.compact(h_uncomp)

    expected = {'852b114ffffffff', '852b189bfffffff', '852b1163fffffff', '842ba9bffffffff', '842bad3ffffffff', '852ba9cffffffff', '842badbffffffff', '852b1e8bfffffff', '852a346ffffffff', '842b1e3ffffffff', '852b116ffffffff', '842b185ffffffff', '852b1bdbfffffff', '852bad47fffffff', '852ba9c3fffffff', '852b106bfffffff', '852a30d3fffffff', '842b1edffffffff', '852b12a7fffffff', '852b1027fffffff', '842baddffffffff', '852a349bfffffff', '852b1227fffffff', '852a3473fffffff', '852b117bfffffff', '842ba99ffffffff', '852a341bfffffff', '852ba9d3fffffff', '852b1067fffffff', '852a3463fffffff', '852baca7fffffff', '852b116bfffffff', '852b1c6bfffffff', '852a3493fffffff', '852ba9dbfffffff', '852b180bfffffff', '842bad7ffffffff', '852b1063fffffff', '842ba93ffffffff', '852a3693fffffff', '852ba977fffffff', '852b1e9bfffffff', '852bad53fffffff', '852b100ffffffff', '852b102bfffffff', '852a3413fffffff', '852ba8b7fffffff', '852bad43fffffff', '852b1c6ffffffff', '852a340bfffffff', '852b103bfffffff', '852b1813fffffff', '852b12affffffff', '842a34dffffffff', '852b1873fffffff', '852b106ffffffff', '852b115bfffffff', '852baca3fffffff', '852b114bfffffff', '852b1143fffffff', '852a348bfffffff', '852a30d7fffffff', '852b181bfffffff', '842a345ffffffff', '852b1e8ffffffff', '852b1883fffffff', '852b1147fffffff', '852a3483fffffff', '852b12a3fffffff', '852a346bfffffff', '852ba9d7fffffff', '842b18dffffffff', '852b188bfffffff', '852a36a7fffffff', '852bacb3fffffff', '852b187bfffffff', '852bacb7fffffff', '842b1ebffffffff', '842b1e5ffffffff', '852ba8a7fffffff', '842bad9ffffffff', '852a36b7fffffff', '852a347bfffffff', '832b13fffffffff', '852ba9c7fffffff', '832b1afffffffff', '842ba91ffffffff', '852bad57fffffff', '852ba8affffffff', '852b1803fffffff', '842b1e7ffffffff', '852bad4ffffffff', '852b102ffffffff', '852b1077fffffff', '852b1237fffffff', '852b1153fffffff', '852a3697fffffff', '852a36b3fffffff', '842bad1ffffffff', '842b1e1ffffffff', '852b186bfffffff', '852b1023fffffff'} # noqa

    assert h_comp == expected

    if in_test:
        return h_uncomp, h_comp, res


def test_uncompact():

    h_uncomp, h_comp, res = test_compact(True)

    out = h3.uncompact(h_comp, res)

    assert out == h_uncomp


def test_num_hexagons():
    expected = {
        0: 122,
        1: 842,
        2: 5882,
        9: 4842432842,
        15: 569707381193162,
    }

    out = {
        k: h3.num_hexagons(k)
        for k in expected
    }

    assert expected == out


def test_hex_area():
    expected_in_km2 = {
        0: 4250546.848,
        1: 607220.9782,
        2: 86745.85403,
        9: 0.1053325,
        15: 9e-07,
    }

    out = {
        k: h3.hex_area(k, unit='km^2')
        for k in expected_in_km2
    }

    assert out == pytest.approx(expected_in_km2)


def test_hex_edge_length():
    expected_in_km = {
        0: 1107.712591000,
        1: 418.676005500,
        2: 158.244655800,
        9: 0.174375668,
        15: 0.000509713,
    }

    out = {
        res: h3.edge_length(res, unit='km')
        for res in expected_in_km
    }

    assert out == pytest.approx(expected_in_km)


def test_edge():
    h1 = '8928308280fffff'
    h2 = '89283082873ffff'

    assert not h3.h3_indexes_are_neighbors(h1, h1)
    assert h3.h3_indexes_are_neighbors(h1, h2)

    e = h3.get_h3_unidirectional_edge(h1, h2)

    assert e == '12928308280fffff'
    assert h3.h3_unidirectional_edge_is_valid(e)
    assert not h3.h3_is_valid(e)

    assert h3.get_origin_h3_index_from_unidirectional_edge(e) == h1
    assert h3.get_destination_h3_index_from_unidirectional_edge(e) == h2

    assert h3.get_h3_indexes_from_unidirectional_edge(e) == (h1, h2)


def test_edges_from_cell():
    h = '8928308280fffff'
    edges = h3.get_h3_unidirectional_edges_from_hexagon(h)
    destinations = {
        h3.get_destination_h3_index_from_unidirectional_edge(e)
        for e in edges
    }
    neighbors = h3.hex_ring(h, 1)

    assert neighbors == destinations


def test_edge_boundary():
    h1 = '8928308280fffff'
    h2 = '89283082873ffff'
    e = h3.get_h3_unidirectional_edge(h1, h2)

    expected = (
        (37.77688044840226, -122.41612835779266),
        (37.778385004930925, -122.41738797617619)
    )

    out = h3.get_h3_unidirectional_edge_boundary(e)

    assert out[0] == pytest.approx(expected[0])
    assert out[1] == pytest.approx(expected[1])


def test_validation():
    h = '8a28308280fffff'  # invalid cell

    with pytest.raises(H3CellError):
        h3.h3_get_base_cell(h)

    with pytest.raises(H3CellError):
        h3.h3_get_resolution(h)

    with pytest.raises(H3CellError):
        h3.h3_to_parent(h, 9)

    with pytest.raises(H3CellError):
        h3.h3_distance(h, h)

    with pytest.raises(H3CellError):
        h3.k_ring(h, 1)

    with pytest.raises(H3CellError):
        h3.hex_ring(h, 1)

    with pytest.raises(H3CellError):
        h3.h3_to_children(h, 11)

    with pytest.raises(H3CellError):
        h3.compact({h})

    with pytest.raises(H3CellError):
        h3.uncompact({h}, 10)


def test_validation2():
    h = '8928308280fffff'

    with pytest.raises(H3ResolutionError):
        h3.h3_to_children(h, 17)

    assert not h3.h3_indexes_are_neighbors(h, h)


def test_validation_geo():
    h = '8a28308280fffff'  # invalid cell

    with pytest.raises(H3CellError):
        h3.h3_to_geo(h)

    with pytest.raises(H3ResolutionError):
        h3.geo_to_h3(0, 0, 17)

    with pytest.raises(H3CellError):
        h3.h3_to_geo_boundary(h)

    with pytest.raises(H3CellError):
        h3.h3_indexes_are_neighbors(h, h)


def test_edges():
    h = '8928308280fffff'

    with pytest.raises(H3ValueError):
        h3.get_h3_unidirectional_edge(h, h)

    h2 = h3.hex_ring(h, 2).pop()
    with pytest.raises(H3ValueError):
        h3.get_h3_unidirectional_edge(h, h2)

    e_bad = '14928308280ffff1'
    assert not h3.h3_unidirectional_edge_is_valid(e_bad)

    with pytest.raises(H3EdgeError):
        h3.get_origin_h3_index_from_unidirectional_edge(e_bad)

    with pytest.raises(H3EdgeError):
        h3.get_destination_h3_index_from_unidirectional_edge(e_bad)

    with pytest.raises(H3EdgeError):
        h3.get_h3_indexes_from_unidirectional_edge(e_bad)


def test_line():
    h1 = '8928308280fffff'
    h2 = '8928308287bffff'

    out = h3.h3_line(h1, h2)

    expected = [
        '8928308280fffff',
        '89283082873ffff',
        '8928308287bffff'
    ]

    assert out == expected


def test_versions():
    from packaging.version import Version

    v = h3.versions()

    assert v['python'] == h3.__version__

    v_c = Version(v['c'])
    v_p = Version(v['python'])

    # of X.Y.Z, X and Y must match
    assert v_c.release[:2] == v_p.release[:2]


def test_str_int_convert():
    s = '8928308280fffff'
    i = h3.string_to_h3(s)

    assert h3.h3_to_string(i) == s


def test_hex2int_fail():
    h_invalid = {}

    assert not h3.h3_is_valid(h_invalid)


def test_edge_is_valid_fail():
    e_invalid = {}
    assert not h3.h3_unidirectional_edge_is_valid(e_invalid)


def test_get_pentagons():
    out = h3.get_pentagon_indexes(0)

    expected = {
        '8009fffffffffff',
        '801dfffffffffff',
        '8031fffffffffff',
        '804dfffffffffff',
        '8063fffffffffff',
        '8075fffffffffff',
        '807ffffffffffff',
        '8091fffffffffff',
        '80a7fffffffffff',
        '80c3fffffffffff',
        '80d7fffffffffff',
        '80ebfffffffffff',
    }

    assert out == expected

    out = h3.get_pentagon_indexes(5)

    expected = {
        '85080003fffffff',
        '851c0003fffffff',
        '85300003fffffff',
        '854c0003fffffff',
        '85620003fffffff',
        '85740003fffffff',
        '857e0003fffffff',
        '85900003fffffff',
        '85a60003fffffff',
        '85c20003fffffff',
        '85d60003fffffff',
        '85ea0003fffffff',
    }

    assert out == expected

    for i in range(16):
        assert len(h3.get_pentagon_indexes(i)) == 12


def test_uncompact_cell_input():
    # `uncompact` takes in a collection of cells, not a single cell.
    # Since a python string is seen as a Iterable collection,
    # inputting a single cell string can raise weird errors.

    # Ensure we get a reasonably helpful answer
    with pytest.raises(H3CellError):
        h3.uncompact('8001fffffffffff', 1)


def test_get_res0_indexes():
    out = h3.get_res0_indexes()

    assert len(out) == 122

    # subset
    pentagons = h3.get_pentagon_indexes(0)
    assert pentagons < out

    # all valid
    assert all(map(h3.h3_is_valid, out))

    # resolution
    assert all(map(
        lambda h: h3.h3_get_resolution(h) == 0,
        out
    ))

    # verify a few concrete cells
    sub = {
        '8001fffffffffff',
        '8003fffffffffff',
        '8005fffffffffff',
    }
    assert sub < out


def test_get_faces_invalid():
    h = '8a28308280fffff'  # invalid cell

    with pytest.raises(H3CellError):
        h3.h3_get_faces(h)


def test_get_faces():
    h = '804dfffffffffff'
    expected = {2, 3, 7, 8, 12}
    out = h3.h3_get_faces(h)
    assert out == expected

    h = '80c1fffffffffff'
    expected = {13}
    out = h3.h3_get_faces(h)
    assert out == expected

    h = '80bbfffffffffff'
    expected = {16, 15}
    out = h3.h3_get_faces(h)
    assert out == expected


def test_to_local_ij_error():
    h = h3.geo_to_h3(0, 0, 0)

    # error if we cross a face
    nb = h3.hex_ring(h, k=2)
    with pytest.raises(H3ValueError):
        [h3.experimental_h3_to_local_ij(h, p) for p in nb]

    # should be fine if we do not cross a face
    nb = h3.hex_ring(h, k=1)
    out = {h3.experimental_h3_to_local_ij(h, p) for p in nb}
    expected = {(-1, 0), (0, -1), (0, 1), (1, 0), (1, 1)}

    assert out == expected


def test_from_local_ij_error():
    h = h3.geo_to_h3(0, 0, 0)

    baddies = [(1, -1), (-1, 1), (-1, -1)]
    for i, j in baddies:
        with pytest.raises(H3ValueError):
            h3.experimental_local_ij_to_h3(h, i, j)

    # inverting output should give good data
    nb = h3.hex_ring(h, k=1)
    goodies = {h3.experimental_h3_to_local_ij(h, p) for p in nb}

    out = {
        h3.experimental_local_ij_to_h3(h, i, j)
        for i, j in goodies
    }

    assert out == nb


def test_to_local_ij_self():
    h = h3.geo_to_h3(0, 0, 9)
    out = h3.experimental_h3_to_local_ij(h, h)

    assert out == (-858, -2766)
