import os

from fiona.env import Env
from fiona._env import get_gdal_version_tuple


_GDAL_VERSION = get_gdal_version_tuple()

# Here is the list of available drivers as (name, modes) tuples. Currently,
# we only expose the defaults (excepting FileGDB). We also don't expose
# the CSV or GeoJSON drivers. Use Python's csv and json modules instead.
# Might still exclude a few more of these after making a pass through the
# entries for each at https://gdal.org/drivers/vector/index.html to screen
# out the multi-layer formats.

supported_drivers = dict(
    [
        # OGR Vector Formats
        # Format Name 	Code 	Creation 	Georeferencing 	Compiled by default
        # Aeronav FAA files 	AeronavFAA 	No 	Yes 	Yes
        ("AeronavFAA", "r"),
        # ESRI ArcObjects 	ArcObjects 	No 	Yes 	No, needs ESRI ArcObjects
        # Arc/Info Binary Coverage 	AVCBin 	No 	Yes 	Yes
        # multi-layer
        #   ("AVCBin", "r"),
        # Arc/Info .E00 (ASCII) Coverage 	AVCE00 	No 	Yes 	Yes
        # multi-layer
        #    ("AVCE00", "r"),
        # Arc/Info Generate 	ARCGEN 	No 	No 	Yes
        ("ARCGEN", "r"),
        # Atlas BNA 	BNA 	Yes 	No 	Yes
        ("BNA", "rw"),
        # AutoCAD DWG 	DWG 	No 	No 	No
        # AutoCAD DXF 	DXF 	Yes 	No 	Yes
        ("DXF", "rw"),
        # Comma Separated Value (.csv) 	CSV 	Yes 	No 	Yes
        ("CSV", "raw"),
        # CouchDB / GeoCouch 	CouchDB 	Yes 	Yes 	No, needs libcurl
        # DODS/OPeNDAP 	DODS 	No 	Yes 	No, needs libdap
        # EDIGEO 	EDIGEO 	No 	Yes 	Yes
        # multi-layer? Hard to tell from the OGR docs
        #   ("EDIGEO", "r"),
        # ElasticSearch 	ElasticSearch 	Yes (write-only) 	- 	No, needs libcurl
        # ESRI FileGDB 	FileGDB 	Yes 	Yes 	No, needs FileGDB API library
        # multi-layer
        ("FileGDB", "raw"),
        ("OpenFileGDB", "raw"),
        # ESRI Personal GeoDatabase 	PGeo 	No 	Yes 	No, needs ODBC library
        # ESRI ArcSDE 	SDE 	No 	Yes 	No, needs ESRI SDE
        # ESRIJSON 	ESRIJSON 	No 	Yes 	Yes
        ("ESRIJSON", "r"),
        # ESRI Shapefile 	ESRI Shapefile 	Yes 	Yes 	Yes
        ("ESRI Shapefile", "raw"),
        # FMEObjects Gateway 	FMEObjects Gateway 	No 	Yes 	No, needs FME
        ("FlatGeobuf", "raw"),
        # GeoJSON 	GeoJSON 	Yes 	Yes 	Yes
        ("GeoJSON", "raw"),
        # GeoJSONSeq 	GeoJSON sequences 	Yes 	Yes 	Yes
        ("GeoJSONSeq", "raw"),
        # Géoconcept Export 	Geoconcept 	Yes 	Yes 	Yes
        # multi-layers
        #   ("Geoconcept", "raw"),
        # Geomedia .mdb 	Geomedia 	No 	No 	No, needs ODBC library
        # GeoPackage	GPKG	Yes	Yes	No, needs libsqlite3
        ("GPKG", "raw"),
        # GeoRSS 	GeoRSS 	Yes 	Yes 	Yes (read support needs libexpat)
        # Google Fusion Tables 	GFT 	Yes 	Yes 	No, needs libcurl
        # GML 	GML 	Yes 	Yes 	Yes (read support needs Xerces or libexpat)
        ("GML", "rw"),
        # GMT 	GMT 	Yes 	Yes 	Yes
        ("GMT", "rw"),
        # GMT renamed to OGR_GMT for GDAL 2.x
        ("OGR_GMT", "rw"),
        # GPSBabel 	GPSBabel 	Yes 	Yes 	Yes (needs GPSBabel and GPX driver)
        # GPX 	GPX 	Yes 	Yes 	Yes (read support needs libexpat)
        ("GPX", "rw"),
        # GRASS 	GRASS 	No 	Yes 	No, needs libgrass
        # GPSTrackMaker (.gtm, .gtz) 	GPSTrackMaker 	Yes 	Yes 	Yes
        # ("GPSTrackMaker", "rw"),
        # Hydrographic Transfer Format 	HTF 	No 	Yes 	Yes
        # TODO: Fiona is not ready for multi-layer formats: ("HTF", "r"),
        # Idrisi Vector (.VCT) 	Idrisi 	No 	Yes 	Yes
        ("Idrisi", "r"),
        # Informix DataBlade 	IDB 	Yes 	Yes 	No, needs Informix DataBlade
        # INTERLIS 	"Interlis 1" and "Interlis 2" 	Yes 	Yes 	No, needs Xerces (INTERLIS model reading needs ili2c.jar)
        # INGRES 	INGRES 	Yes 	No 	No, needs INGRESS
        # KML 	KML 	Yes 	Yes 	Yes (read support needs libexpat)
        # LIBKML 	LIBKML 	Yes 	Yes 	No, needs libkml
        # Mapinfo File 	MapInfo File 	Yes 	Yes 	Yes
        ("MapInfo File", "raw"),
        # Microstation DGN 	DGN 	Yes 	No 	Yes
        ("DGN", "raw"),
        # Access MDB (PGeo and Geomedia capable) 	MDB 	No 	Yes 	No, needs JDK/JRE
        # Memory 	Memory 	Yes 	Yes 	Yes
        # MySQL 	MySQL 	No 	Yes 	No, needs MySQL library
        # NAS - ALKIS 	NAS 	No 	Yes 	No, needs Xerces
        # Oracle Spatial 	OCI 	Yes 	Yes 	No, needs OCI library
        # ODBC 	ODBC 	No 	Yes 	No, needs ODBC library
        # MS SQL Spatial 	MSSQLSpatial 	Yes 	Yes 	No, needs ODBC library
        # Open Document Spreadsheet 	ODS 	Yes 	No 	No, needs libexpat
        # OGDI Vectors (VPF, VMAP, DCW) 	OGDI 	No 	Yes 	No, needs OGDI library
        # OpenAir 	OpenAir 	No 	Yes 	Yes
        # multi-layer
        #   ("OpenAir", "r"),
        # (Geo)Parquet
        ("Parquet", "rw"),
        # PCI Geomatics Database File 	PCIDSK 	No 	No 	Yes, using internal PCIDSK SDK (from GDAL 1.7.0)
        ("PCIDSK", "raw"),
        # PDS 	PDS 	No 	Yes 	Yes
        ("PDS", "r"),
        # PDS renamed to OGR_PDS for GDAL 2.x
        ("OGR_PDS", "r"),
        # PGDump 	PostgreSQL SQL dump 	Yes 	Yes 	Yes
        # PostgreSQL/PostGIS 	PostgreSQL/PostGIS 	Yes 	Yes 	No, needs PostgreSQL client library (libpq)
        # EPIInfo .REC 	REC 	No 	No 	Yes
        # S-57 (ENC) 	S57 	No 	Yes 	Yes
        # multi-layer
        ("S57", "r"),
        # SDTS 	SDTS 	No 	Yes 	Yes
        # multi-layer
        #   ("SDTS", "r"),
        # SEG-P1 / UKOOA P1/90 	SEGUKOOA 	No 	Yes 	Yes
        # multi-layers
        #   ("SEGUKOOA", "r"),
        # SEG-Y 	SEGY 	No 	No 	Yes
        ("SEGY", "r"),
        # Norwegian SOSI Standard 	SOSI 	No 	Yes 	No, needs FYBA library
        # SQLite/SpatiaLite 	SQLite 	Yes 	Yes 	No, needs libsqlite3 or libspatialite
        ("SQLite", "raw"),
        # SUA 	SUA 	No 	Yes 	Yes
        ("SUA", "r"),
        # SVG 	SVG 	No 	Yes 	No, needs libexpat
        ("TileDB", "raw"),
        # TopoJSON 	TopoJSON 	No 	Yes 	Yes
        ("TopoJSON", "r"),
        # UK .NTF 	UK. NTF 	No 	Yes 	Yes
        # multi-layer
        #   ("UK. NTF", "r"),
        # U.S. Census TIGER/Line 	TIGER 	No 	Yes 	Yes
        # multi-layer
        #   ("TIGER", "r"),
        # VFK data 	VFK 	No 	Yes 	Yes
        # multi-layer
        #   ("VFK", "r"),
        # VRT - Virtual Datasource 	VRT 	No 	Yes 	Yes
        # multi-layer
        #   ("VRT", "r"),
        # OGC WFS (Web Feature Service) 	WFS 	Yes 	Yes 	No, needs libcurl
        # MS Excel format 	XLS 	No 	No 	No, needs libfreexl
        # Office Open XML spreadsheet 	XLSX 	Yes 	No 	No, needs libexpat
        # X-Plane/Flighgear aeronautical data 	XPLANE 	No 	Yes 	Yes
        # multi-layer
        #   ("XPLANE", "r")
    ]
)

# Minimal gdal version for different modes
driver_mode_mingdal = {
    "r": {"GPKG": (1, 11, 0), "GeoJSONSeq": (2, 4, 0), "FlatGeobuf": (3, 1, 0)},
    "w": {
        "GPKG": (1, 11, 0),
        "PCIDSK": (2, 0, 0),
        "GeoJSONSeq": (2, 4, 0),
        "FlatGeobuf": (3, 1, 3),
        "OpenFileGDB": (3, 6, 0),
    },
    "a": {
        "GPKG": (1, 11, 0),
        "PCIDSK": (2, 0, 0),
        "GeoJSON": (2, 1, 0),
        "GeoJSONSeq": (3, 6, 0),
        "MapInfo File": (2, 0, 0),
        "FlatGeobuf": (3, 5, 1),
        "OpenFileGDB": (3, 6, 0),
    },
}


def _driver_supports_mode(driver, mode):
    """ Returns True if driver supports mode, False otherwise

        Note: this function is not part of Fiona's public API.
    """
    if driver not in supported_drivers:
        return False
    if mode not in supported_drivers[driver]:
        return False
    if driver in driver_mode_mingdal[mode]:
        if _GDAL_VERSION < driver_mode_mingdal[mode][driver]:
            return False
    return True


# Removes drivers in the supported_drivers dictionary that the
# machine's installation of OGR due to how it is compiled.
# OGR may not have optional libraries compiled or installed.
def _filter_supported_drivers():
    global supported_drivers

    with Env() as gdalenv:
        ogrdrv_names = gdalenv.drivers().keys()
        supported_drivers_copy = supported_drivers.copy()
        for drv in supported_drivers.keys():
            if drv not in ogrdrv_names:
                del supported_drivers_copy[drv]

    supported_drivers = supported_drivers_copy


_filter_supported_drivers()


def vector_driver_extensions():
    """
    Returns
    -------
    dict:
        Map of extensions to the driver.
    """
    from fiona.meta import extensions  # prevent circular import

    extension_to_driver = {}
    for drv, modes in supported_drivers.items():
        # update extensions based on driver support
        for extension in extensions(drv) or ():
            if "w" in modes:
                extension_to_driver[extension] = extension_to_driver.get(extension, drv)
    return extension_to_driver


def driver_from_extension(path):
    """
    Attempt to auto-detect driver based on the extension.

    Parameters
    ----------
    path: str or pathlike object
        The path to the dataset to write with.

    Returns
    -------
    str:
        The name of the driver for the extension.
    """
    try:
        # in case the path is a file handle
        # or a partsed path
        path = path.name
    except AttributeError:
        pass

    driver_extensions = vector_driver_extensions()

    try:
        return driver_extensions[os.path.splitext(path)[-1].lstrip(".").lower()]
    except KeyError:
        raise ValueError("Unable to detect driver. Please specify driver.")


# driver_converts_to_str contains field type, driver combinations that
# are silently converted to string None: field type is always converted
# to str (2, 0, 0): starting from gdal 2.0 field type is not converted
# to string
_driver_converts_to_str = {
    'time': {
        'CSV': None,
        'PCIDSK': None,
        'GeoJSON': (2, 0, 0),
        'GPKG': None,
        'GMT': None,
        'OGR_GMT': None
    },
    'datetime': {
        'CSV': None,
        'PCIDSK': None,
        'GeoJSON': (2, 0, 0),
        'GML': (3, 1, 0),
    },
    'date': {
        'CSV': None,
        'PCIDSK': None,
        'GeoJSON': (2, 0, 0),
        'GMT': None,
        'OGR_GMT': None,
        'GML': (3, 1, 0),
    }
}


def _driver_converts_field_type_silently_to_str(driver, field_type):
    """ Returns True if the driver converts the field_type silently to str, False otherwise

        Note: this function is not part of Fiona's public API.
    """
    if field_type in _driver_converts_to_str and driver in _driver_converts_to_str[field_type]:
        if _driver_converts_to_str[field_type][driver] is None:
            return True
        elif _GDAL_VERSION < _driver_converts_to_str[field_type][driver]:
            return True
    return False


# None: field type is never supported, (2, 0, 0) field type is supported starting with gdal 2.0
_driver_field_type_unsupported = {
    "time": {
        "ESRI Shapefile": None,
        "GPKG": (2, 0, 0),
        "GPX": None,
        "GPSTrackMaker": None,
        "GML": (3, 1, 0),
        "DGN": None,
        "BNA": None,
        "DXF": None,
        "PCIDSK": (2, 1, 0),
        "FileGDB": (3, 5, 0),
        "FlatGeobuf": None,
        "OpenFileGDB": None,
    },
    'datetime': {
        'ESRI Shapefile': None,
        'GPKG': (2, 0, 0),
        'DGN': None,
        'BNA': None,
        'DXF': None,
        'PCIDSK': (2, 1, 0)
    },
    "date": {
        "GPX": None,
        "GPSTrackMaker": None,
        "DGN": None,
        "BNA": None,
        "DXF": None,
        "PCIDSK": (2, 1, 0),
        "FileGDB": (3, 5, 0),
        "FlatGeobuf": None,
        "OpenFileGDB": None,
    },
}


def _driver_supports_field(driver, field_type):
    """ Returns True if the driver supports the field_type, False otherwise

        Note: this function is not part of Fiona's public API.
    """
    if field_type in _driver_field_type_unsupported and driver in _driver_field_type_unsupported[field_type]:
        if _driver_field_type_unsupported[field_type][driver] is None:
            return False
        elif _GDAL_VERSION < _driver_field_type_unsupported[field_type][driver]:
            return False

    return True


# None: field type never supports timezones, (2, 0, 0): field type supports timezones with GDAL 2.0.0
_drivers_not_supporting_timezones = {
    'datetime': {
        'MapInfo File': None,
        'GPKG': (3, 1, 0),
        'GPSTrackMaker': (3, 1, 1),
        'FileGDB': None,
        'SQLite': (2, 4, 0)
    },
    "time": {
        "MapInfo File": None,
        "GPKG": None,
        "GPSTrackMaker": None,
        "GeoJSON": None,
        "GeoJSONSeq": None,
        "GML": None,
        "CSV": None,
        "GMT": None,
        "OGR_GMT": None,
        "SQLite": None,
    },
}


def _driver_supports_timezones(driver, field_type):
    """ Returns True if the driver supports timezones for field_type, False otherwise

        Note: this function is not part of Fiona's public API.
    """
    if field_type in _drivers_not_supporting_timezones and driver in _drivers_not_supporting_timezones[field_type]:
        if _drivers_not_supporting_timezones[field_type][driver] is None:
            return False
        elif _GDAL_VERSION < _drivers_not_supporting_timezones[field_type][driver]:
            return False
    return True


# None: driver never supports timezones, (2, 0, 0): driver supports timezones with GDAL 2.0.0
_drivers_not_supporting_milliseconds = {
    "GPSTrackMaker": None,
    "FileGDB": None,
    "OpenFileGDB": None,
}


def _driver_supports_milliseconds(driver):
    """ Returns True if the driver supports milliseconds, False otherwise

        Note: this function is not part of Fiona's public API.
    """
    # GDAL 2.0 introduced support for milliseconds
    if _GDAL_VERSION.major < 2:
        return False

    if driver in _drivers_not_supporting_milliseconds:
        if _drivers_not_supporting_milliseconds[driver] is None:
            return False
        elif _drivers_not_supporting_milliseconds[driver] < _GDAL_VERSION:
            return False

    return True
