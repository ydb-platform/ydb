"""Test of compound CRS crash avoidance"""

import fiona
from fiona.crs import CRS


def test_compound_crs(data):
    """Don't crash"""
    prj = data.join("coutwildrnp.prj")
    prj.write("""COMPD_CS["unknown",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],VERT_CS["unknown",VERT_DATUM["unknown",2005],UNIT["metre",1.0,AUTHORITY["EPSG","9001"]],AXIS["Up",UP]]]""")
    with fiona.open(str(data.join("coutwildrnp.shp"))) as collection:
        assert isinstance(collection.crs, CRS)
