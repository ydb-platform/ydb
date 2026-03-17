import glob
import os
import tempfile

import fiona
from fiona.model import Feature

from .conftest import get_temp_filename, requires_gdal2


def test_gml_format_option(tmp_path):
    """Test GML dataset creation option FORMAT (see gh-968)"""

    schema = {"geometry": "Point", "properties": {"position": "int"}}
    records = [
        Feature.from_dict(
            **{
                "geometry": {"type": "Point", "coordinates": (0.0, float(i))},
                "properties": {"position": i},
            }
        )
        for i in range(10)
    ]

    fpath = tmp_path.joinpath(get_temp_filename("GML"))

    with fiona.open(fpath, "w", driver="GML", schema=schema, FORMAT="GML3") as out:
        out.writerecords(records)

    xsd_path = list(tmp_path.glob("*.xsd"))[0]

    with open(xsd_path) as f:
        xsd = f.read()
        assert "http://schemas.opengis.net/gml/3.1.1" in xsd
