# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import pytest

import cartopy
import cartopy.io.shapereader as shp


@pytest.mark.filterwarnings("ignore:Downloading")
@pytest.mark.natural_earth
class TestCoastline:
    def test_robust(self):
        COASTLINE_PATH = shp.natural_earth()

        # Make sure all the coastlines can be projected without raising any
        # exceptions.
        projection = cartopy.crs.TransverseMercator(central_longitude=-90,
                                                    approx=False)
        reader = shp.Reader(COASTLINE_PATH)
        all_geometries = list(reader.geometries())
        geometries = []
        geometries += all_geometries
        # geometries += all_geometries[48:52] # Aus & Taz
        # geometries += all_geometries[72:73] # GB
        # for geometry in geometries:
        for geometry in geometries[93:]:
            projection.project_geometry(geometry)
