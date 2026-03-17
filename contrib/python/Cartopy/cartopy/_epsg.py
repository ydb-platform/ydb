# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Provide support for converting EPSG codes to Projection instances.

"""
from pyproj.crs import CRS as _CRS

import cartopy.crs as ccrs


class _EPSGProjection(ccrs.Projection):
    def __init__(self, code):
        crs = _CRS.from_epsg(code)
        if not crs.is_projected:
            raise ValueError('EPSG code does not define a projection')
        if not crs.area_of_use:
            raise ValueError("Area of use not defined.")

        self.epsg_code = code
        super().__init__(crs.to_wkt())

    def __repr__(self):
        return f'_EPSGProjection({self.epsg_code})'

    def __reduce__(self):
        return self.__class__, (self.epsg_code, )
