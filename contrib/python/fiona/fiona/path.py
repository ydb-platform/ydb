"""Dataset paths, identifiers, and filenames

Note well: this module is deprecated in 1.3.0 and will be removed in a
future version.
"""

import warnings

from fiona._path import _ParsedPath as ParsedPath
from fiona._path import _UnparsedPath as UnparsedPath
from fiona._path import _parse_path as parse_path
from fiona._path import _vsi_path as vsi_path
from fiona.errors import FionaDeprecationWarning

warnings.warn(
    "fiona.path will be removed in version 2.0.", FionaDeprecationWarning
)
