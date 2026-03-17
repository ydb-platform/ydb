"""Dataset paths, identifiers, and filenames

Note well: this module is deprecated in 1.3.0 and will be removed in a
future version.
"""

import warnings

from rasterio._path import _ParsedPath as ParsedPath
from rasterio._path import _UnparsedPath as UnparsedPath
from rasterio._path import _parse_path as parse_path
from rasterio._path import _vsi_path as vsi_path
from rasterio.errors import RasterioDeprecationWarning

warnings.warn(
    "rasterio.path will be removed in version 1.4.", RasterioDeprecationWarning
)
