import h3


def test_h3_set_to_multi_polygon():
    h = '8928308280fffff'
    hexes = h3.k_ring(h, 1)

    mpoly = h3.h3_set_to_multi_polygon(hexes)

    out = h3.polyfill_polygon(mpoly[0][0], 9, holes=None, lnglat_order=False)

    assert out == hexes


def test_2_polys():
    h = '8928308280fffff'
    hexes = h3.hex_ring(h, 2)
    hexes = hexes | {h}
    # hexes should be a center hex, and the 2-ring around it
    # (with the 1-ring being absent)

    out = [
        h3.polyfill_polygon(poly[0], 9, holes=poly[1:], lnglat_order=False)
        for poly in h3.h3_set_to_multi_polygon(hexes, geo_json=False)
    ]

    assert set.union(*out) == hexes


def test_2_polys_json():
    h = '8928308280fffff'
    hexes = h3.hex_ring(h, 2)
    hexes = hexes | {h}
    # hexes should be a center hex, and the 2-ring around it
    # (with the 1-ring being absent)

    # not deterministic which poly is first..
    poly1, poly2 = h3.h3_set_to_multi_polygon(hexes, geo_json=True)

    assert {len(poly1), len(poly2)} == {1, 2}

    for poly in poly1, poly2:
        for loop in poly:
            assert loop[0] == loop[-1]


def test_2_polys_not_json():
    h = '8928308280fffff'
    hexes = h3.hex_ring(h, 2)
    hexes = hexes | {h}
    # hexes should be a center hex, and the 2-ring around it
    # (with the 1-ring being absent)

    # not deterministic which poly is first..
    poly1, poly2 = h3.h3_set_to_multi_polygon(hexes, geo_json=False)

    assert {len(poly1), len(poly2)} == {1, 2}

    for poly in poly1, poly2:
        for loop in poly:
            assert loop[0] != loop[-1]
