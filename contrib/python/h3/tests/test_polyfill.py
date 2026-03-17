import h3
import itertools
import pytest


def reverse(loop):
    return list(reversed(loop))


def drop_last(loop):
    return loop[:-1]


def toggle_map(func, poly):
    """ Return all permuations of `func` being applied or not
    to each element of `poly`

    returns iterable of length 2**len(poly)
    """
    mapped = (list(func(loop)) for loop in poly)

    return itertools.product(*zip(poly, mapped))


def chain_toggle_map(func, seq):
    seq = (toggle_map(func, p) for p in seq)
    seq = itertools.chain(*seq)

    return seq


def input_permutations(poly, res=5):
    g = [poly]
    g = chain_toggle_map(drop_last, g)
    g = chain_toggle_map(reverse, g)

    for p in g:
        hexes = h3.polyfill_polygon(p[0], res=res, holes=p[1:])
        yield hexes


def swap_element_order(seq):
    return [e[::-1] for e in seq]


def get_us_box_coords(order='latlng'):

    # big center chunk of the US in lat/lng order
    outer = [
        [42.68, -110.61],
        [32.17, -109.02],
        [31.57,  -94.26],
        [42.94,  -89.38],
        [42.68, -110.61]
    ]

    hole1 = [
        [39.77, -105.07],
        [34.81, -104.72],
        [34.77,  -98.39],
        [40.14,  -96.72],
        [39.77, -105.07]
    ]

    hole2 = [
        [41.37, -98.61],
        [40.04, -91.80],
        [42.32, -91.80],
        [41.37, -98.61]
    ]

    if order == 'lnglat':
        outer, hole1, hole2 = map(swap_element_order, [outer, hole1, hole2])

    return outer, hole1, hole2


def test_polyfill_polygon():

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

    # a very rough hexagonal approximation to the State of Maine
    expected = {
        '832b13fffffffff',
        '832b18fffffffff',
        '832b1afffffffff',
        '832b1efffffffff',
        '832ba9fffffffff',
        '832badfffffffff'
    }

    out = h3.polyfill_polygon(maine, 3)

    assert out == expected


def test_polyfill_polygon_order():
    lnglat, _, _ = get_us_box_coords(order='lnglat')

    out = h3.polyfill_polygon(lnglat, 5, lnglat_order=True)

    assert len(out) == 7063


# # todo: we can generate segfaults with malformed input data to polyfill
# # need to test for this and avoid segfault
# # def test_polyfill_segfault():
# #     pass


def test_polyfill_polygon_holes():

    outer, hole1, hole2 = get_us_box_coords()

    assert 7063 == len(
        h3.polyfill_polygon(outer, 5)
    )

    for res in 1, 2, 3, 4, 5:
        hexes_all = h3.polyfill_polygon(outer, res)
        hexes_holes = h3.polyfill_polygon(outer, res, [hole1, hole2])

        hexes_1 = h3.polyfill_polygon(hole1, res)
        hexes_2 = h3.polyfill_polygon(hole2, res)

        assert len(hexes_all) == len(hexes_holes) + len(hexes_1) + len(hexes_2)
        assert hexes_all == set.union(hexes_holes, hexes_1, hexes_2)


def test_polyfill_geojson():
    outer, hole1, hole2 = get_us_box_coords(order='lnglat')

    d = {
        'type': 'Polygon',
        'coordinates': [outer],
    }

    out = h3.polyfill_geojson(d, 5)

    assert len(out) == 7063


def test_polyfill():
    outer, hole1, hole2 = get_us_box_coords(order='lnglat')

    d = {
        'type': 'Polygon',
        'coordinates': [outer],
    }

    out = h3.polyfill(d, 5, geo_json_conformant=True)

    assert len(out) == 7063


def test_input_format():
    """ Test that `polyfill_polygon` can take in polygon inputs
    where the LinearRings may or may not follow the right hand rule,
    and they may or may not be closed loops (where the last element
    is equal to the first).

    Test all permutations of these rules on polygons with
    0, 1, and 2 holes. Ensure that for any polygon, each LinearRing
    may follow a different subset of rules.
    """

    poly = get_us_box_coords(order='latlng')

    assert len(poly) == 3

    # two holes
    for hexes in input_permutations(poly[:3]):
        assert len(hexes) == 5437

    # one hole
    for hexes in input_permutations(poly[:2]):
        assert len(hexes) == 5726

    # zero holes
    for hexes in input_permutations(poly[:1]):
        assert len(hexes) == 7063


def test_resolution():
    d = {
        'type': 'Polygon',
        'coordinates': [[]],
    }

    with pytest.raises(h3.H3ResolutionError):
        h3.polyfill(d, -1)

    with pytest.raises(h3.H3ResolutionError):
        h3.polyfill(d, 16)


def test_invalid_polygon():
    """
    We were previously seeing segfaults on data like
    this because we weren't raising errors inside
    some `cdef` functions.
    """

    # one
    d = {
        'type': 'Polygon',
        'coordinates': [1, 2, 3],
    }
    with pytest.raises(TypeError):
        h3.polyfill(d, 4)

    # two
    d = {
        'type': 'Polygon',
        'coordinates': [[1, 2, 3]],
    }
    with pytest.raises(TypeError):
        h3.polyfill(d, 4)

    # three
    d = {
        'type': 'Polygon',
        'coordinates': [(1, 2), (2, 2), (2, 1), (1, 2)],
    }
    with pytest.raises(TypeError):
        h3.polyfill(d, 4)
