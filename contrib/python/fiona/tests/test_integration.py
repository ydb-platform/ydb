"""Unittests to verify Fiona is functioning properly with other software."""


from collections import UserDict

import fiona
from fiona.model import Feature


def test_dict_subclass(tmpdir):
    """Rasterio now has a `CRS()` class that subclasses
    `collections.UserDict()`.  Make sure we can receive it.

    `UserDict()` is a good class to test against because in Python 2 it is
    not a subclass of `collections.Mapping()`, so it provides an edge case.
    """

    class CRS(UserDict):
        pass

    outfile = str(tmpdir.join("test_UserDict.geojson"))

    profile = {
        "crs": CRS(init="EPSG:4326"),
        "driver": "GeoJSON",
        "schema": {"geometry": "Point", "properties": {}},
    }

    with fiona.open(outfile, "w", **profile) as dst:
        dst.write(
            Feature.from_dict(
                **{
                    "type": "Feature",
                    "properties": {},
                    "geometry": {"type": "Point", "coordinates": (10, -10)},
                }
            )
        )

    with fiona.open(outfile) as src:
        assert len(src) == 1
        assert src.crs == {"init": "epsg:4326"}
