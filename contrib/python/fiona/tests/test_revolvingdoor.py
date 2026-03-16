# Test of opening and closing and opening

import fiona


def test_write_revolving_door(tmpdir, path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp) as src:
        meta = src.meta
        features = list(src)

    shpname = str(tmpdir.join('foo.shp'))

    with fiona.open(shpname, 'w', **meta) as dst:
        dst.writerecords(features)

    with fiona.open(shpname) as src:
        pass
