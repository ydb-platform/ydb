"""Raster dataset profiles."""

from collections import UserDict

from rasterio.dtypes import uint8


class Profile(UserDict):
    """Base class for Rasterio dataset profiles.

    Subclasses will declare driver-specific creation options.
    """

    defaults = {}

    def __init__(self, data={}, **kwds):
        """Create a new profile based on the class defaults, which are
        overlaid with items from the `data` dict and keyword arguments."""
        UserDict.__init__(self)
        initdata = self.defaults.copy()
        initdata.update(data)
        initdata.update(**kwds)
        self.data.update(initdata)

    def __getitem__(self, key):
        """Like normal item access but with affine alias."""
        return self.data[key]

    def __setitem__(self, key, val):
        """Like normal item setter but forbidding affine item."""
        if key == 'affine':
            raise TypeError("affine key is prohibited")
        self.data[key] = val


class DefaultGTiffProfile(Profile):
    """Tiled, band-interleaved, LZW-compressed, 8-bit GTiff."""

    defaults = {
        'driver': 'GTiff',
        'interleave': 'band',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'compress': 'lzw',
        'nodata': 0,
        'dtype': uint8
    }


default_gtiff_profile = DefaultGTiffProfile()
