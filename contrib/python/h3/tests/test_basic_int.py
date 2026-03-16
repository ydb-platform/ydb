import h3.api.basic_int as h3


def test_int_output():
    lat = 37.7752702151959
    lng = -122.418307270836

    assert h3.geo_to_h3(lat, lng, 9) == 617700169958293503
    assert h3.geo_to_h3(lat, lng, 9) == 0x8928308280fffff


def test_k_ring():
    expected = {
        617700169957507071,
        617700169957769215,
        617700169958031359,
        617700169958293503,
        617700169961177087,
        617700169964847103,
        617700169965109247,
    }

    out = h3.k_ring(617700169958293503, 1)
    assert out == expected


def test_compact():
    h = 617700169958293503
    hexes = h3.h3_to_children(h)

    assert h3.compact(hexes) == {h}


def test_get_faces():
    h = 577832942814887935
    expected = {2, 3, 7, 8, 12}
    out = h3.h3_get_faces(h)
    assert out == expected

    h = 579873636396040191
    expected = {13}
    out = h3.h3_get_faces(h)
    assert out == expected

    h = 579768083279773695
    expected = {16, 15}
    out = h3.h3_get_faces(h)
    assert out == expected
