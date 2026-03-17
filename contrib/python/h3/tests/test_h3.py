import pytest
from pytest import approx

import h3


def test_nested_import():
    """ Test that we can import `h3.h3`
    For backwards-compatibility
    """
    from h3 import h3
    assert h3.geo_to_h3(37.3615593, -122.0553238, 5) == '85283473fffffff'


def shift_circular_list(start_element, elements_list):
    # We shift the circular list so that it starts from start_element,
    start_index = elements_list.index(start_element)
    return elements_list[start_index:] + elements_list[:start_index]


def test_h3_is_valid():
    assert h3.h3_is_valid('85283473fffffff')
    assert h3.h3_is_valid('850dab63fffffff')
    assert not h3.h3_is_valid('lolwut')

    # H3 0.x Addresses are not considered valid
    assert not h3.h3_is_valid('5004295803a88')

    for res in range(16):
        assert h3.h3_is_valid(h3.geo_to_h3(37, -122, res))


def test_geo_to_h3():
    assert h3.geo_to_h3(37.3615593, -122.0553238, 5) == '85283473fffffff'


def test_h3_get_resolution():
    for res in range(16):
        h = h3.geo_to_h3(37.3615593, -122.0553238, res)
        assert h3.h3_get_resolution(h) == res


def test_silly_geo_to_h3():
    lat, lng = 37.3615593, -122.0553238

    expected0 = '85283473fffffff'
    out0 = h3.geo_to_h3(lat, lng, 5)
    assert out0 == expected0

    out1 = h3.geo_to_h3(lat + 180.0, lng + 360.0, 5)
    expected1 = '85ca2d53fffffff'
    assert out1 == expected1


def test_h3_to_geo():
    latlng = h3.h3_to_geo('85283473fffffff')
    assert latlng == approx((37.34579337536848, -121.97637597255124))


def test_h3_to_geo_boundary():
    out = h3.h3_to_geo_boundary('85283473fffffff')

    expected = [
        [37.271355866731895, -121.91508032705622],
        [37.353926450852256, -121.86222328902491],
        [37.42834118609435, -121.9235499963016],
        [37.42012867767778, -122.0377349642703],
        [37.33755608435298, -122.09042892904395],
        [37.26319797461824, -122.02910130919],
    ]

    assert len(out) == len(expected)

    for o, e in zip(out, expected):
        assert o == approx(e)


def test_h3_to_geo_boundary_geo_json():
    out = h3.h3_to_geo_boundary('85283473fffffff', True)

    expected = [
        [-121.91508032705622, 37.271355866731895],
        [-121.86222328902491, 37.353926450852256],
        [-121.9235499963016, 37.42834118609435],
        [-122.0377349642703, 37.42012867767778],
        [-122.09042892904395, 37.33755608435298],
        [-122.02910130919, 37.26319797461824],
        [-121.91508032705622, 37.271355866731895],
    ]

    assert len(out) == len(expected)

    for o, e in zip(out, expected):
        assert o == approx(e)


def test_k_ring():
    h = '8928308280fffff'
    out = h3.k_ring(h, 1)

    assert len(out) == 1 + 6

    expected = {
        '8928308280bffff',
        '89283082807ffff',
        '89283082877ffff',
        h,
        '89283082803ffff',
        '89283082873ffff',
        '8928308283bffff',
    }

    assert out == expected


def test_k_ring2():
    h = '8928308280fffff'
    out = h3.k_ring(h, 2)

    assert len(out) == 1 + 6 + 12

    expected = {
        '89283082813ffff',
        '89283082817ffff',
        '8928308281bffff',
        '89283082863ffff',
        '89283082823ffff',
        '89283082873ffff',
        '89283082877ffff',
        h,
        '8928308287bffff',
        '89283082833ffff',
        '8928308282bffff',
        '8928308283bffff',
        '89283082857ffff',
        '892830828abffff',
        '89283082847ffff',
        '89283082867ffff',
        '89283082803ffff',
        '89283082807ffff',
        '8928308280bffff',
    }

    assert out == expected


def test_k_ring_pentagon():
    h = '821c07fffffffff'  # a pentagon cell
    out = h3.k_ring(h, 1)

    assert len(out) == 1 + 5

    expected = {
        '821c2ffffffffff',
        '821c27fffffffff',
        h,
        '821c17fffffffff',
        '821c1ffffffffff',
        '821c37fffffffff',
    }

    assert out == expected


def test_k_ring_distances():
    h = '8928308280fffff'
    out = h3.k_ring_distances(h, 1)

    assert [len(x) for x in out] == [1, 6]

    expected = [
        {h},
        {
            '8928308280bffff',
            '89283082807ffff',
            '89283082877ffff',
            '89283082803ffff',
            '89283082873ffff',
            '8928308283bffff',
        }
    ]

    assert out == expected

    out = h3.k_ring_distances('870800003ffffff', 2)

    assert [len(x) for x in out] == [1, 6, 11]


def test_polyfill():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [37.813318999983238, -122.4089866999972145],
                [37.7866302000007224, -122.3805436999997056],
                [37.7198061999978478, -122.3544736999993603],
                [37.7076131999975672, -122.5123436999983966],
                [37.7835871999971715, -122.5247187000021967],
                [37.8151571999998453, -122.4798767000009008]
            ]
        ]
    }

    out = h3.polyfill(geo, 9)
    assert len(out) > 1000


def test_polyfill_bogus_geo_json():
    with pytest.raises(ValueError):
        bad_geo = {'type': 'whatwhat'}
        h3.polyfill(bad_geo, 9)


def test_polyfill_with_hole():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [37.813318999983238, -122.4089866999972145],
                [37.7866302000007224, -122.3805436999997056],
                [37.7198061999978478, -122.3544736999993603],
                [37.7076131999975672, -122.5123436999983966],
                [37.7835871999971715, -122.5247187000021967],
                [37.8151571999998453, -122.4798767000009008],
            ],
            [
                [37.7869802, -122.4471197],
                [37.7664102, -122.4590777],
                [37.7710682, -122.4137097],
            ]
        ]
    }

    out = h3.polyfill(geo, 9)
    assert len(out) > 1000


def test_polyfill_with_two_holes():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [37.813318999983238, -122.4089866999972145],
                [37.7866302000007224, -122.3805436999997056],
                [37.7198061999978478, -122.3544736999993603],
                [37.7076131999975672, -122.5123436999983966],
                [37.7835871999971715, -122.5247187000021967],
                [37.8151571999998453, -122.4798767000009008],
            ],
            [
                [37.7869802, -122.4471197],
                [37.7664102, -122.4590777],
                [37.7710682, -122.4137097],
            ],
            [
                [37.747976, -122.490025],
                [37.731550, -122.503758],
                [37.725440, -122.452603],
            ]
        ]
    }

    out = h3.polyfill(geo, 9)
    assert len(out) > 1000


def test_polyfill_geo_json_compliant():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [-122.4089866999972145, 37.813318999983238],
                [-122.3805436999997056, 37.7866302000007224],
                [-122.3544736999993603, 37.7198061999978478],
                [-122.5123436999983966, 37.7076131999975672],
                [-122.5247187000021967, 37.7835871999971715],
                [-122.4798767000009008, 37.8151571999998453],
            ]
        ]
    }

    out = h3.polyfill(geo, 9, True)
    assert len(out) > 1000


def test_polyfill_down_under():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [151.1979259, -33.8555555],
                [151.2074556, -33.8519779],
                [151.224743, -33.8579597],
                [151.2254986, -33.8582212],
                [151.235313348, -33.8564183032],
                [151.234799568, -33.8594049408],
                [151.233485084, -33.8641069037],
                [151.233181742, -33.8715791334],
                [151.223980353, -33.8876967719],
                [151.219388501, -33.8873877027],
                [151.2189209, -33.8869995],
                [151.2181177, -33.886283399999996],
                [151.2157995, -33.8851287],
                [151.2156925, -33.8852471],
                [151.2141233, -33.8851287],
                [151.2116267, -33.8847438],
                [151.2083456, -33.8834707],
                [151.2080246, -33.8827601],
                [151.2059204, -33.8816053],
                [151.2043868, -33.8827601],
                [151.2028176, -33.8838556],
                [151.2022826, -33.8839148],
                [151.2011057, -33.8842405],
                [151.1986114, -33.8842819],
                [151.1986091, -33.8842405],
                [151.1948287, -33.8773416],
                [151.1923322, -33.8740845],
                [151.1850566, -33.8697019],
                [151.1902636, -33.8625354],
                [151.1986805, -33.8612915],
                [151.1979259, -33.8555555],
            ]
        ]
    }

    out = h3.polyfill(geo, 9, True)
    assert len(out) > 10


def test_polyfill_far_east():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [142.86483764648438, 41.92578147109541],
                [142.86483764648438, 42.29965889253408],
                [143.41552734375, 42.29965889253408],
                [143.41552734375, 41.92578147109541],
                [142.86483764648438, 41.92578147109541],
            ]
        ]
    }

    out = h3.polyfill(geo, 9, True)
    assert len(out) > 10


def test_polyfill_southern_tip():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [-67.642822265625, -55.41654360858007],
                [-67.642822265625, -54.354955689554096],
                [-64.742431640625, -54.354955689554096],
                [-64.742431640625, -55.41654360858007],
                [-67.642822265625, -55.41654360858007],
            ]
        ]
    }

    out = h3.polyfill(geo, 9, True)
    assert len(out) > 10


def test_polyfill_null_island():
    geo = {
        "type": "Polygon",
        "coordinates": [
            [
                [-3.218994140625, -3.0856655287215378],
                [-3.218994140625, 3.6888551431470478],
                [3.5815429687499996, 3.6888551431470478],
                [3.5815429687499996, -3.0856655287215378],
                [-3.218994140625, -3.0856655287215378],
            ]
        ]
    }

    out = h3.polyfill(geo, 4, True)
    assert len(out) > 10


def test_h3_set_to_multi_polygon_empty():
    out = h3.h3_set_to_multi_polygon([])
    assert out == []


def test_h3_set_to_multi_polygon_single():
    h = '89283082837ffff'
    hexes = {h}

    # multi_polygon
    mp = h3.h3_set_to_multi_polygon(hexes)
    vertices = h3.h3_to_geo_boundary(h)

    # We shift the expected circular list so that it starts from
    # multi_polygon[0][0][0], since output starting from any vertex
    # would be correct as long as it's in order.
    expected_coords = shift_circular_list(
        mp[0][0][0],
        [
            vertices[2],
            vertices[3],
            vertices[4],
            vertices[5],
            vertices[0],
            vertices[1],
        ]
    )

    expected = [[expected_coords]]

    assert mp == expected


def test_h3_set_to_multi_polygon_single_geo_json():
    hexes = ['89283082837ffff']
    mp = h3.h3_set_to_multi_polygon(hexes, True)
    vertices = h3.h3_to_geo_boundary(hexes[0], True)

    # We shift the expected circular list so that it starts from
    # multi_polygon[0][0][0], since output starting from any vertex
    # would be correct as long as it's in order.
    expected_coords = shift_circular_list(
        mp[0][0][0],
        [
            vertices[2],
            vertices[3],
            vertices[4],
            vertices[5],
            vertices[0],
            vertices[1]
        ]
    )

    expected = [[expected_coords]]

    # polygon count matches expected
    assert len(mp) == 1

    # loop count matches expected
    assert len(mp[0]) == 1

    # coord count 7 matches expected according to geojson format
    assert len(mp[0][0]) == 7

    # first coord should be the same as last coord according to geojson format
    assert mp[0] == mp[-1]

    # the coord should be (lng, lat) according to geojson format
    assert mp[0][0][0][0] == approx(-122.42778275313199)
    assert mp[0][0][0][1] == approx(37.77598951883773)

    # Discard last coord for testing below, since last coord is
    # the same as the first one
    mp[0][0].pop()
    assert mp == expected


def test_h3_set_to_multi_polygon_contiguous():
    # the second hexagon shares v0 and v1 with the first
    hexes = ['89283082837ffff', '89283082833ffff']

    # multi_polygon
    mp = h3.h3_set_to_multi_polygon(hexes)
    vertices0 = h3.h3_to_geo_boundary(hexes[0])
    vertices1 = h3.h3_to_geo_boundary(hexes[1])

    # We shift the expected circular list so that it starts from
    # multi_polygon[0][0][0], since output starting from any vertex
    # would be correct as long as it's in order.
    expected_coords = shift_circular_list(
        mp[0][0][0],
        [
            vertices1[0],
            vertices1[1],
            vertices1[2],
            vertices0[1],
            vertices0[2],
            vertices0[3],
            vertices0[4],
            vertices0[5],
            vertices1[4],
            vertices1[5],
        ]
    )

    expected = [[expected_coords]]

    assert len(mp) == 1  # polygon count matches expected
    assert len(mp[0]) == 1  # loop count matches expected
    assert len(mp[0][0]) == 10  # coord count matches expected

    assert mp == expected


def test_h3_set_to_multi_polygon_non_contiguous():
    # the second hexagon does not touch the first
    hexes = {'89283082837ffff', '8928308280fffff'}
    # multi_polygon
    mp = h3.h3_set_to_multi_polygon(hexes)

    assert len(mp) == 2  # polygon count matches expected
    assert len(mp[0]) == 1  # loop count matches expected
    assert len(mp[0][0]) == 6  # coord count 1 matches expected
    assert len(mp[1][0]) == 6  # coord count 2 matches expected


def test_h3_set_to_multi_polygon_hole():
    # Six hexagons in a ring around a hole
    hexes = [
        '892830828c7ffff', '892830828d7ffff', '8928308289bffff',
        '89283082813ffff', '8928308288fffff', '89283082883ffff',
    ]
    mp = h3.h3_set_to_multi_polygon(hexes)

    assert len(mp) == 1  # polygon count matches expected
    assert len(mp[0]) == 2  # loop count matches expected
    assert len(mp[0][0]) == 6 * 3  # outer coord count matches expected
    assert len(mp[0][1]) == 6  # inner coord count matches expected


def test_h3_set_to_multi_polygon_2k_ring():
    h = '8930062838bffff'
    hexes = h3.k_ring(h, 2)
    # multi_polygon
    mp = h3.h3_set_to_multi_polygon(hexes)

    assert len(mp) == 1  # polygon count matches expected
    assert len(mp[0]) == 1  # loop count matches expected
    assert len(mp[0][0]) == 6 * (2 * 2 + 1)  # coord count matches expected

    hexes2 = {
        '89300628393ffff', '89300628383ffff', '89300628397ffff',
        '89300628067ffff', '89300628387ffff', '893006283bbffff',
        '89300628313ffff', '893006283cfffff', '89300628303ffff',
        '89300628317ffff', '8930062839bffff', h,
        '8930062806fffff', '8930062838fffff', '893006283d3ffff',
        '893006283c3ffff', '8930062831bffff', '893006283d7ffff',
        '893006283c7ffff'
    }

    mp2 = h3.h3_set_to_multi_polygon(hexes2)

    assert len(mp2) == 1  # polygon count matches expected
    assert len(mp2[0]) == 1  # loop count matches expected
    assert len(mp2[0][0]) == 6 * (2 * 2 + 1)  # coord count matches expected

    hexes3 = list(h3.k_ring(h, 6))
    hexes3.sort()
    mp3 = h3.h3_set_to_multi_polygon(hexes3)

    assert len(mp3[0]) == 1  # loop count matches expected


def test_hex_ring():
    h = '8928308280fffff'
    out = h3.hex_ring(h, 1)
    expected = {
        '8928308280bffff',
        '89283082807ffff',
        '89283082877ffff',
        '89283082803ffff',
        '89283082873ffff',
        '8928308283bffff',
    }

    assert out == expected
    assert out == h3.k_ring(h, 1) - h3.k_ring(h, 0)


def test_hex_ring2():
    h = '8928308280fffff'
    out = h3.hex_ring(h, 2)

    expected = {
        '89283082813ffff',
        '89283082817ffff',
        '8928308281bffff',
        '89283082863ffff',
        '89283082823ffff',
        '8928308287bffff',
        '89283082833ffff',
        '8928308282bffff',
        '89283082857ffff',
        '892830828abffff',
        '89283082847ffff',
        '89283082867ffff',
    }

    assert out == expected
    assert out == h3.k_ring(h, 2) - h3.k_ring(h, 1)


def test_hex_ring_pentagon():
    h = '821c07fffffffff'
    out = h3.hex_ring(h, 1)

    expected = {
        '821c17fffffffff',
        '821c1ffffffffff',
        '821c27fffffffff',
        '821c2ffffffffff',
        '821c37fffffffff',
    }

    assert out == expected


def test_compact_and_uncompact():
    geo = {
        'type': 'Polygon',
        'coordinates': [
            [
                [37.813318999983238, -122.4089866999972145],
                [37.7866302000007224, -122.3805436999997056],
                [37.7198061999978478, -122.3544736999993603],
                [37.7076131999975672, -122.5123436999983966],
                [37.7835871999971715, -122.5247187000021967],
                [37.8151571999998453, -122.4798767000009008],
            ]
        ]
    }

    hexes = h3.polyfill(geo, 9)

    compact_hexes = h3.compact(hexes)
    assert len(compact_hexes) == 209

    uncompact_hexes = h3.uncompact(compact_hexes, 9)
    assert len(uncompact_hexes) == 1253


def test_compact_and_uncompact_nothing():
    assert h3.compact([]) == set()
    assert h3.uncompact([], 9) == set()


def test_uncompact_error():
    hexagons = [h3.geo_to_h3(37, -122, 10)]

    with pytest.raises(Exception):
        h3.uncompact(hexagons, 5)


def test_compact_malformed_input():
    hexes = ['89283082813ffff'] * 13

    with pytest.raises(Exception):
        h3.compact(hexes)


def test_h3_to_parent():
    h = '89283082813ffff'
    assert h3.h3_to_parent(h, 8) == '8828308281fffff'


def test_h3_to_children():
    h = '8828308281fffff'
    children = h3.h3_to_children(h, 9)

    assert len(children) == 7


def test_hex_range():
    h = '8928308280fffff'
    out = h3.hex_range(h, 1)
    assert len(out) == 1 + 6

    expected = {
        '8928308280bffff',
        '89283082807ffff',
        h,
        '89283082877ffff',
        '89283082803ffff',
        '89283082873ffff',
        '8928308283bffff',
    }

    assert out == expected


def test_hex_range2():
    h = '8928308280fffff'
    out = h3.hex_range(h, 2)

    assert len(out) == 1 + 6 + 12

    expected = {
        '89283082813ffff',
        '89283082817ffff',
        '8928308281bffff',
        '89283082863ffff',
        '89283082823ffff',
        '89283082873ffff',
        '89283082877ffff',
        '8928308287bffff',
        '89283082833ffff',
        '8928308282bffff',
        '8928308283bffff',
        '89283082857ffff',
        '892830828abffff',
        '89283082847ffff',
        '89283082867ffff',
        '89283082803ffff',
        h,
        '89283082807ffff',
        '8928308280bffff',
    }

    assert out == expected


def test_hex_range_pentagon():
    h = '821c07fffffffff'  # a pentagon

    # should consist of `h` and it's 5 neighbors
    out = h3.hex_range(h, 1)

    expected = {
        h,
        '821c17fffffffff',
        '821c1ffffffffff',
        '821c27fffffffff',
        '821c2ffffffffff',
        '821c37fffffffff',
    }

    assert out == expected


def test_hex_range_distances():
    h = '8928308280fffff'

    # should consist of `h` and it's 5 neighbors
    out = h3.hex_range_distances(h, 1)

    expected = [
        {h},
        {
            '8928308280bffff',
            '89283082807ffff',
            '89283082877ffff',
            '89283082803ffff',
            '89283082873ffff',
            '8928308283bffff',
        }
    ]

    assert out == expected


def test_hex_range_distances_pentagon():

    h = '821c07fffffffff'
    out = h3.hex_range_distances(h, 1)

    expected = [
        {h},
        {
            '821c17fffffffff',
            '821c1ffffffffff',
            '821c27fffffffff',
            '821c2ffffffffff',
            '821c37fffffffff',
        }
    ]

    assert out == expected


def test_hex_ranges():
    h = '8928308280fffff'
    out = h3.hex_ranges([h], 1)

    assert set(out.keys()) == {h}

    expected = [
        {h},
        {
            '8928308280bffff',
            '89283082807ffff',
            '89283082877ffff',
            '89283082803ffff',
            '89283082873ffff',
            '8928308283bffff',
        }
    ]

    assert out[h] == expected


def test_hex_ranges_pentagon():
    h = '821c07fffffffff'
    out = h3.hex_ranges([h], 1)

    expected = {
        h: [
            {h},
            {
                '821c17fffffffff',
                '821c1ffffffffff',
                '821c27fffffffff',
                '821c2ffffffffff',
                '821c37fffffffff'
            }
        ]
    }

    assert out == expected


def test_many_hex_ranges():
    hexes = h3.k_ring('8928308280fffff', 2)
    out = h3.hex_ranges(hexes, 2)

    assert len(out) == 19

    hexes = out['8928308280fffff']
    assert [len(x) for x in hexes] == [1, 6, 12]


def test_many_hex_ranges2():
    hexes = h3.k_ring('8928308280fffff', 5)
    out = h3.hex_ranges(hexes, 5)
    assert len(out) == 91

    hexes = out['8928308280fffff']

    assert [len(x) for x in hexes] == [1, 6, 12, 18, 24, 30]


def test_hex_area():
    for i in range(0, 15):
        assert isinstance(h3.hex_area(i), float)
        assert isinstance(h3.hex_area(i, 'm^2'), float)

    with pytest.raises(ValueError):
        h3.hex_area(5, 'ft^2')


def test_edge_length():
    for i in range(0, 15):
        assert isinstance(h3.edge_length(i), float)
        assert isinstance(h3.edge_length(i, 'm'), float)

    with pytest.raises(ValueError):
        h3.edge_length(5, 'ft')


def test_num_hexagons():
    h0 = 122
    assert h3.num_hexagons(0) == h0

    for i in range(0, 15):
        n = h3.num_hexagons(i) * 1.0 / h0

        assert 6**i <= n <= 7**i


def test_h3_get_base_cell():
    assert h3.h3_get_base_cell('8928308280fffff') == 20


def test_h3_is_res_class_iiiIII():
    assert h3.h3_is_res_class_iii('8928308280fffff')
    assert not h3.h3_is_res_class_iii('8828308280fffff')
    assert h3.h3_is_res_class_III('8928308280fffff')


def test_h3_is_pentagon():
    assert h3.h3_is_pentagon('821c07fffffffff')
    assert not h3.h3_is_pentagon('8928308280fffff')


def test_h3_indexes_are_neighbors():
    assert h3.h3_indexes_are_neighbors('8928308280fffff', '8928308280bffff')
    assert not h3.h3_indexes_are_neighbors('821c07fffffffff', '8928308280fffff')


def test_get_h3_unidirectional_edge():
    out = h3.get_h3_unidirectional_edge('8928308280fffff', '8928308280bffff')
    assert h3.h3_unidirectional_edge_is_valid(out)

    with pytest.raises(ValueError):
        h3.get_h3_unidirectional_edge('821c07fffffffff', '8928308280fffff')


def test_h3_unidirectional_edge_is_valid():
    assert not h3.h3_unidirectional_edge_is_valid('8928308280fffff')
    assert h3.h3_unidirectional_edge_is_valid('11928308280fffff')


def test_get_origin_h3_index_from_unidirectional_edge():
    out = h3.get_origin_h3_index_from_unidirectional_edge('11928308280fffff')
    assert out == '8928308280fffff'


def test_get_destination_h3_index_from_unidirectional_edge():
    h = '11928308280fffff'
    out = h3.get_destination_h3_index_from_unidirectional_edge(h)

    assert out == '8928308283bffff'


def test_get_h3_indexes_from_unidirectional_edge():
    e = h3.get_h3_indexes_from_unidirectional_edge('11928308280fffff')

    assert e == ('8928308280fffff', '8928308283bffff')


def test_get_h3_unidirectional_edges_from_hexagon():
    h3_uni_edges = h3.get_h3_unidirectional_edges_from_hexagon(
        '8928308280fffff'
    )
    assert len(h3_uni_edges) == 6

    h3_uni_edge_pentagon = h3.get_h3_unidirectional_edges_from_hexagon(
        '821c07fffffffff'
    )
    assert len(h3_uni_edge_pentagon) == 5


def test_get_h3_unidirectional_edge_boundary():
    e = '11928308280fffff'
    boundary = h3.get_h3_unidirectional_edge_boundary(e)
    assert len(boundary) == 2

    boundary_geo_json = h3.get_h3_unidirectional_edge_boundary(e, True)
    assert len(boundary_geo_json) == 3


def test_h3_distance():
    h = '89283082993ffff'

    assert 0 == h3.h3_distance(h, h)
    assert 1 == h3.h3_distance(h, '8928308299bffff')
    assert 5 == h3.h3_distance(h, '89283082827ffff')


def test_h3_line():
    h1 = '8a2a84730587fff'
    h2 = '8a2a8471414ffff'

    out = h3.h3_line(h1, h2)

    expected = [
        h1,
        '8a2a8473059ffff',
        '8a2a847304b7fff',
        '8a2a84730487fff',
        '8a2a8473049ffff',
        '8a2a84732b37fff',
        '8a2a84732b17fff',
        '8a2a84732baffff',
        '8a2a84732a37fff',
        '8a2a84732a17fff',
        '8a2a84732aaffff',
        '8a2a84732a8ffff',
        '8a2a84732327fff',
        '8a2a8473232ffff',
        '8a2a8473230ffff',
        '8a2a84732227fff',
        '8a2a84732207fff',
        '8a2a8473221ffff',
        '8a2a847322e7fff',
        '8a2a847322c7fff',
        '8a2a847322dffff',
        '8a2a84714977fff',
        '8a2a84714957fff',
        '8a2a8471495ffff',
        '8a2a84714877fff',
        '8a2a84714857fff',
        '8a2a847148effff',
        '8a2a847148cffff',
        '8a2a84714b97fff',
        '8a2a8471416ffff',
        h2,
    ]

    assert out == expected

    with pytest.raises(ValueError):
        h3.h3_line(h1, '8001fffffffffff')
