from packaging.version import Version

from pyogrio.core import __gdal_geos_version__, __gdal_version__

# detect optional dependencies
try:
    import pyarrow
except ImportError:
    pyarrow = None

try:
    import pyproj
except ImportError:
    pyproj = None

try:
    import shapely
except ImportError:
    shapely = None

try:
    import geopandas
except ImportError:
    geopandas = None

try:
    import pandas
except ImportError:
    pandas = None


HAS_ARROW_WRITE_API = __gdal_version__ >= (3, 8, 0)
HAS_PYARROW = pyarrow is not None
HAS_PYPROJ = pyproj is not None
PYARROW_GE_19 = pyarrow is not None and Version(pyarrow.__version__) >= Version(
    "19.0.0"
)

HAS_GEOPANDAS = geopandas is not None

PANDAS_GE_15 = pandas is not None and Version(pandas.__version__) >= Version("1.5.0")
PANDAS_GE_20 = pandas is not None and Version(pandas.__version__) >= Version("2.0.0")
PANDAS_GE_22 = pandas is not None and Version(pandas.__version__) >= Version("2.2.0")
PANDAS_GE_23 = pandas is not None and Version(pandas.__version__) >= Version("2.3.0")
PANDAS_GE_30 = pandas is not None and Version(pandas.__version__) >= Version("3.0.0dev")

GDAL_GE_37 = __gdal_version__ >= (3, 7, 0)
GDAL_GE_38 = __gdal_version__ >= (3, 8, 0)
GDAL_GE_311 = __gdal_version__ >= (3, 11, 0)

HAS_GDAL_GEOS = __gdal_geos_version__ is not None

HAS_SHAPELY = shapely is not None and Version(shapely.__version__) >= Version("2.0.0")
SHAPELY_GE_21 = shapely is not None and Version(shapely.__version__) >= Version("2.1.0")
