"""GeoAlchemy2 package."""

from geoalchemy2 import admin
from geoalchemy2 import elements  # noqa
from geoalchemy2 import exc  # noqa
from geoalchemy2 import functions  # noqa
from geoalchemy2 import shape  # noqa
from geoalchemy2 import types  # noqa
from geoalchemy2.admin.dialects.geopackage import load_spatialite_gpkg  # noqa
from geoalchemy2.admin.dialects.sqlite import load_spatialite  # noqa
from geoalchemy2.admin.plugin import GeoEngine  # noqa
from geoalchemy2.elements import CompositeElement  # noqa
from geoalchemy2.elements import RasterElement  # noqa
from geoalchemy2.elements import WKBElement  # noqa
from geoalchemy2.elements import WKTElement  # noqa
from geoalchemy2.exc import ArgumentError  # noqa
from geoalchemy2.types import Geography  # noqa
from geoalchemy2.types import Geometry  # noqa
from geoalchemy2.types import Raster  # noqa

admin.setup_ddl_event_listeners()

try:
    from importlib.metadata import version

    __version__ = version("geoalchemy2")
except ImportError:
    __version__ = "0.0.0"

__all__ = [
    "__version__",
    "ArgumentError",
    "CompositeElement",
    "GeoEngine",
    "Geography",
    "Geometry",
    "Raster",
    "RasterElement",
    "WKBElement",
    "WKTElement",
    "admin",
    "elements",
    "exc",
    "load_spatialite",
    "load_spatialite_gpkg",
    "shape",
    "types",
]


def __dir__():
    return __all__
