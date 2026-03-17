import h3
import pytest

from h3 import H3ValueError


def approx2(a, b):
    if len(a) != len(b):
        return False

    return all(
        x == pytest.approx(y)
        for x, y in zip(a, b)
    )


def cell_perimiter1(h, unit='km'):
    edges = h3.get_h3_unidirectional_edges_from_hexagon(h)

    dists = [
        h3.exact_edge_length(e, unit=unit)
        for e in edges
    ]

    assert all(d > 0 for d in dists)

    return sum(dists)


def cell_perimiter2(h, unit='km'):
    verts = h3.h3_to_geo_boundary(h)
    N = len(verts)
    verts += (verts[0],)

    dists = [
        h3.point_dist(verts[i], verts[i + 1], unit=unit)
        for i in range(N)
    ]

    assert all(d > 0 for d in dists)

    return sum(dists)


def test_areas_at_00():
    areas_km2 = [
        2.562182162955495529e+06,
        4.476842018179409206e+05,
        6.596162242711056024e+04,
        9.228872919002589697e+03,
        1.318694490797110348e+03,
        1.879593512281297762e+02,
        2.687164354763186225e+01,
        3.840848847060638782e+00,
        5.486939641329895423e-01,
        7.838600808637447015e-02,
        1.119834221989390345e-02,
        1.599777169186613647e-03,
        2.285390931423379875e-04,
        3.264850232091780848e-05,
        4.664070326136773890e-06,
        6.662957615868890711e-07,
    ]

    out = [
        h3.cell_area(h3.geo_to_h3(0, 0, r), unit='km^2')
        for r in range(16)
    ]

    assert approx2(out, areas_km2)

    areas_rads2 = [
        6.312389871006786335e-02,
        1.102949377223657809e-02,
        1.625081476657283096e-03,
        2.273696413041990331e-04,
        3.248837599063685022e-05,
        4.630711750349743332e-06,
        6.620305651949173071e-07,
        9.462611873890716096e-08,
        1.351804829317986891e-08,
        1.931178237937334527e-09,
        2.758910081529350229e-10,
        3.941334595426616175e-11,
        5.630465614578665530e-12,
        8.043537197853909460e-13,
        1.149076389260636790e-13,
        1.641537700693487648e-14,
    ]

    out = [
        h3.cell_area(h3.geo_to_h3(0, 0, r), unit='rads^2')
        for r in range(16)
    ]

    assert approx2(out, areas_rads2)


def test_bad_units():
    h = '89754e64993ffff'
    e = '139754e64993ffff'

    assert h3.h3_is_valid(h)
    assert h3.h3_unidirectional_edge_is_valid(e)

    with pytest.raises(H3ValueError):
        h3.cell_area(h, unit='foot-pounds')

    with pytest.raises(H3ValueError):
        h3.exact_edge_length(h, unit='foot-pounds')

    with pytest.raises(H3ValueError):
        h3.point_dist((0, 0), (0, 0), unit='foot-pounds')


def test_point_dist():
    lyon = (45.7597, 4.8422)  # (lat, lon)
    paris = (48.8567, 2.3508)

    d = h3.point_dist(lyon, paris, unit='rads')
    assert d == pytest.approx(0.0615628186794217)

    d = h3.point_dist(lyon, paris, unit='m')
    assert d == pytest.approx(392217.1598841777)

    d = h3.point_dist(lyon, paris, unit='km')
    assert d == pytest.approx(392.21715988417765)

    # test that 'km' is the default unit
    assert h3.point_dist(lyon, paris, unit='km') == h3.point_dist(lyon, paris)


def test_cell_perimiter_calculations():
    resolutions = [0, 1]

    for r in resolutions:
        cells = h3.uncompact(h3.get_res0_indexes(), r)
        for h in cells:
            for unit in ['rads', 'm', 'km']:
                v1 = cell_perimiter1(h, unit)
                v2 = cell_perimiter2(h, unit)

                assert v1 == pytest.approx(v2)
