"""Collections provide file-like access to feature data."""

from contextlib import ExitStack
import logging
from pathlib import Path
import warnings

from fiona import compat, vfs
from fiona.ogrext import Iterator, ItemsIterator, KeysIterator
from fiona.ogrext import Session, WritingSession
from fiona.ogrext import buffer_to_virtual_file, remove_virtual_file, GEOMETRY_TYPES
from fiona.errors import (
    DriverError,
    DriverSupportError,
    GDALVersionError,
    SchemaError,
    UnsupportedGeometryTypeError,
    UnsupportedOperation,
)
from fiona.logutils import FieldSkipLogFilter
from fiona.crs import CRS
from fiona._env import get_gdal_release_name, get_gdal_version_tuple
from fiona.env import env_ctx_if_needed
from fiona.errors import FionaDeprecationWarning
from fiona.drvsupport import (
    driver_from_extension,
    supported_drivers,
    driver_mode_mingdal,
    _driver_converts_field_type_silently_to_str,
    _driver_supports_field,
)
from fiona._path import _Path, _vsi_path, _parse_path


_GDAL_VERSION_TUPLE = get_gdal_version_tuple()
_GDAL_RELEASE_NAME = get_gdal_release_name()

log = logging.getLogger(__name__)


class Collection:

    """A file-like interface to features of a vector dataset

    Python text file objects are iterators over lines of a file. Fiona
    Collections are similar iterators (not lists!) over features
    represented as GeoJSON-like mappings.
    """

    def __init__(
        self,
        path,
        mode="r",
        driver=None,
        schema=None,
        crs=None,
        encoding=None,
        layer=None,
        vsi=None,
        archive=None,
        enabled_drivers=None,
        crs_wkt=None,
        ignore_fields=None,
        ignore_geometry=False,
        include_fields=None,
        wkt_version=None,
        allow_unsupported_drivers=False,
        **kwargs
    ):

        """The required ``path`` is the absolute or relative path to
        a file, such as '/data/test_uk.shp'. In ``mode`` 'r', data can
        be read only. In ``mode`` 'a', data can be appended to a file.
        In ``mode`` 'w', data overwrites the existing contents of
        a file.

        In ``mode`` 'w', an OGR ``driver`` name and a ``schema`` are
        required. A Proj4 ``crs`` string is recommended. If both ``crs``
        and ``crs_wkt`` keyword arguments are passed, the latter will
        trump the former.

        In 'w' mode, kwargs will be mapped to OGR layer creation
        options.

        """
        self._closed = True

        if not isinstance(path, (str, _Path)):
            raise TypeError(f"invalid path: {path!r}")
        if not isinstance(mode, str) or mode not in ("r", "w", "a"):
            raise TypeError(f"invalid mode: {mode!r}")
        if driver and not isinstance(driver, str):
            raise TypeError(f"invalid driver: {driver!r}")
        if schema and not hasattr(schema, "get"):
            raise TypeError("invalid schema: %r" % schema)

        # Rasterio's CRS is compatible with Fiona. This class
        # constructor only requires that the crs value have a to_wkt()
        # method.
        if (
            crs
            and not isinstance(crs, compat.DICT_TYPES + (str, CRS))
            and not (hasattr(crs, "to_wkt") and callable(crs.to_wkt))
        ):
            raise TypeError("invalid crs: %r" % crs)

        if crs_wkt and not isinstance(crs_wkt, str):
            raise TypeError(f"invalid crs_wkt: {crs_wkt!r}")
        if encoding and not isinstance(encoding, str):
            raise TypeError(f"invalid encoding: {encoding!r}")
        if layer and not isinstance(layer, (str, int)):
            raise TypeError(f"invalid name: {layer!r}")
        if vsi:
            if not isinstance(vsi, str) or not vfs.valid_vsi(vsi):
                raise TypeError(f"invalid vsi: {vsi!r}")
        if archive and not isinstance(archive, str):
            raise TypeError(f"invalid archive: {archive!r}")
        if ignore_fields is not None and include_fields is not None:
            raise ValueError("Cannot specify both 'ignore_fields' and 'include_fields'")

        if mode == "w" and driver is None:
            driver = driver_from_extension(path)

        # Check GDAL version against drivers
        if (
            driver in driver_mode_mingdal[mode]
            and get_gdal_version_tuple() < driver_mode_mingdal[mode][driver]
        ):
            min_gdal_version = ".".join(
                list(map(str, driver_mode_mingdal[mode][driver]))
            )

            raise DriverError(
                f"{driver} driver requires at least GDAL {min_gdal_version} "
                f"for mode '{mode}', "
                f"Fiona was compiled against: {get_gdal_release_name()}"
            )

        self.session = None
        self.iterator = None
        self._len = 0
        self._bounds = None
        self._driver = None
        self._schema = None
        self._crs = None
        self._crs_wkt = None
        self.enabled_drivers = enabled_drivers
        self.include_fields = include_fields
        self.ignore_fields = ignore_fields
        self.ignore_geometry = bool(ignore_geometry)
        self._allow_unsupported_drivers = allow_unsupported_drivers
        self._closed = True

        # Check GDAL version against drivers
        if (
            driver in driver_mode_mingdal[mode]
            and get_gdal_version_tuple() < driver_mode_mingdal[mode][driver]
        ):
            min_gdal_version = ".".join(
                list(map(str, driver_mode_mingdal[mode][driver]))
            )

            raise DriverError(
                f"{driver} driver requires at least GDAL {min_gdal_version} "
                f"for mode '{mode}', "
                f"Fiona was compiled against: {get_gdal_release_name()}"
            )

        if vsi:
            self.path = vfs.vsi_path(path, vsi, archive)
            path = _parse_path(self.path)
        else:
            path = _parse_path(path)
            self.path = _vsi_path(path)

        self.layer = layer or 0

        if mode == "w":
            if layer and not isinstance(layer, str):
                raise ValueError("in 'w' mode, layer names must be strings")
            self.name = layer or Path(self.path).stem
        else:
            self.name = 0 if layer is None else layer or Path(self.path).stem

        self.mode = mode

        if self.mode == "w":
            if driver == "Shapefile":
                driver = "ESRI Shapefile"
            if not driver:
                raise DriverError("no driver")
            if not allow_unsupported_drivers:
                if driver not in supported_drivers:
                    raise DriverError(f"unsupported driver: {driver!r}")
                if self.mode not in supported_drivers[driver]:
                    raise DriverError(f"unsupported mode: {self.mode!r}")
            self._driver = driver

            if not schema:
                raise SchemaError("no schema")
            if "properties" in schema:
                # Make properties as a dict built-in
                this_schema = schema.copy()
                this_schema["properties"] = dict(schema["properties"])
                schema = this_schema
            else:
                schema["properties"] = {}
            if "geometry" not in schema:
                schema["geometry"] = None
            self._schema = schema

            self._check_schema_driver_support()

            if crs_wkt or crs:
                self._crs_wkt = CRS.from_user_input(crs_wkt or crs).to_wkt(
                    version=wkt_version
                )

        self._driver = driver
        kwargs.update(encoding=encoding)
        self.encoding = encoding

        try:
            if self.mode == "r":
                self.session = Session()
                self.session.start(self, **kwargs)
            elif self.mode in ("a", "w"):
                self.session = WritingSession()
                self.session.start(self, **kwargs)
        except OSError:
            self.session = None
            raise

        if self.session is not None:
            self.guard_driver_mode()

        if self.mode in ("a", "w"):
            self._valid_geom_types = _get_valid_geom_types(self.schema, self.driver)

        self.field_skip_log_filter = FieldSkipLogFilter()
        self._env = ExitStack()
        self._closed = False

    def __repr__(self):
        return "<{} Collection '{}', mode '{}' at {}>".format(
            self.closed and "closed" or "open",
            self.path + ":" + str(self.name),
            self.mode,
            hex(id(self)),
        )

    def guard_driver_mode(self):
        if not self._allow_unsupported_drivers:
            driver = self.session.get_driver()
            if driver not in supported_drivers:
                raise DriverError(f"unsupported driver: {driver!r}")
            if self.mode not in supported_drivers[driver]:
                raise DriverError(f"unsupported mode: {self.mode!r}")

    @property
    def driver(self):
        """Returns the name of the proper OGR driver."""
        if not self._driver and self.mode in ("a", "r") and self.session:
            self._driver = self.session.get_driver()
        return self._driver

    @property
    def schema(self):
        """Returns a mapping describing the data schema.

        The mapping has 'geometry' and 'properties' items. The former is a
        string such as 'Point' and the latter is an ordered mapping that
        follows the order of fields in the data file.
        """
        if not self._schema and self.mode in ("a", "r") and self.session:
            self._schema = self.session.get_schema()
        return self._schema

    @property
    def crs(self):
        """The coordinate reference system (CRS) of the Collection."""
        if self._crs is None and self.session:
            self._crs = self.session.get_crs()
        return self._crs

    @property
    def crs_wkt(self):
        """Returns a WKT string."""
        if self._crs_wkt is None and self.session:
            self._crs_wkt = self.session.get_crs_wkt()
        return self._crs_wkt

    def tags(self, ns=None):
        """Returns a dict containing copies of the dataset or layers's
        tags. Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        Parameters
        ----------
        ns: str, optional
            Can be used to select a namespace other than the default.

        Returns
        -------
        dict
        """
        if _GDAL_VERSION_TUPLE.major < 2:
            raise GDALVersionError(
                "tags requires GDAL 2+, fiona was compiled "
                f"against: {_GDAL_RELEASE_NAME}"
            )
        if self.session:
            return self.session.tags(ns=ns)
        return None

    def get_tag_item(self, key, ns=None):
        """Returns tag item value

        Parameters
        ----------
        key: str
            The key for the metadata item to fetch.
        ns: str, optional
            Used to select a namespace other than the default.

        Returns
        -------
        str
        """
        if _GDAL_VERSION_TUPLE.major < 2:
            raise GDALVersionError(
                "get_tag_item requires GDAL 2+, fiona was compiled "
                f"against: {_GDAL_RELEASE_NAME}"
            )
        if self.session:
            return self.session.get_tag_item(key=key, ns=ns)
        return None

    def update_tags(self, tags, ns=None):
        """Writes a dict containing the dataset or layers's tags.
        Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        Parameters
        ----------
        tags: dict
            The dict of metadata items to set.
        ns: str, optional
            Used to select a namespace other than the default.

        Returns
        -------
        int
        """
        if _GDAL_VERSION_TUPLE.major < 2:
            raise GDALVersionError(
                "update_tags requires GDAL 2+, fiona was compiled "
                f"against: {_GDAL_RELEASE_NAME}"
            )
        if not isinstance(self.session, WritingSession):
            raise UnsupportedOperation("Unable to update tags as not in writing mode.")
        return self.session.update_tags(tags, ns=ns)

    def update_tag_item(self, key, tag, ns=None):
        """Updates the tag item value

        Parameters
        ----------
        key: str
            The key for the metadata item to set.
        tag: str
            The value of the metadata item to set.
        ns: str, optional
            Used to select a namespace other than the default.

        Returns
        -------
        int
        """
        if _GDAL_VERSION_TUPLE.major < 2:
            raise GDALVersionError(
                "update_tag_item requires GDAL 2+, fiona was compiled "
                f"against: {_GDAL_RELEASE_NAME}"
            )
        if not isinstance(self.session, WritingSession):
            raise UnsupportedOperation("Unable to update tag as not in writing mode.")
        return self.session.update_tag_item(key=key, tag=tag, ns=ns)

    @property
    def meta(self):
        """Returns a mapping with the driver, schema, crs, and additional
        properties."""
        return {
            "driver": self.driver,
            "schema": self.schema,
            "crs": self.crs,
            "crs_wkt": self.crs_wkt,
        }

    profile = meta

    def filter(self, *args, **kwds):
        """Returns an iterator over records, but filtered by a test for
        spatial intersection with the provided ``bbox``, a (minx, miny,
        maxx, maxy) tuple or a geometry ``mask``. An attribute filter can
        be set using an SQL ``where`` clause, which uses the `OGR SQL dialect
        <https://gdal.org/user/ogr_sql_dialect.html#where>`__.

        Positional arguments ``stop`` or ``start, stop[, step]`` allows
        iteration to skip over items or stop at a specific item.

        Note: spatial filtering using ``mask`` may be inaccurate and returning
        all features overlapping the envelope of ``mask``.

        """
        if self.closed:
            raise ValueError("I/O operation on closed collection")
        elif self.mode != "r":
            raise OSError("collection not open for reading")
        if args:
            s = slice(*args)
            start = s.start
            stop = s.stop
            step = s.step
        else:
            start = stop = step = None
        bbox = kwds.get("bbox")
        mask = kwds.get("mask")
        if bbox and mask:
            raise ValueError("mask and bbox can not be set together")
        where = kwds.get("where")
        self.iterator = Iterator(self, start, stop, step, bbox, mask, where)
        return self.iterator

    def items(self, *args, **kwds):
        """Returns an iterator over FID, record pairs, optionally
        filtered by a test for spatial intersection with the provided
        ``bbox``, a (minx, miny, maxx, maxy) tuple or a geometry
        ``mask``. An attribute filter can be set using an SQL ``where``
        clause, which uses the `OGR SQL dialect
        <https://gdal.org/user/ogr_sql_dialect.html#where>`__.

        Positional arguments ``stop`` or ``start, stop[, step]`` allows
        iteration to skip over items or stop at a specific item.

        Note: spatial filtering using ``mask`` may be inaccurate and returning
        all features overlapping the envelope of ``mask``.

        """
        if self.closed:
            raise ValueError("I/O operation on closed collection")
        elif self.mode != "r":
            raise OSError("collection not open for reading")
        if args:
            s = slice(*args)
            start = s.start
            stop = s.stop
            step = s.step
        else:
            start = stop = step = None
        bbox = kwds.get("bbox")
        mask = kwds.get("mask")
        if bbox and mask:
            raise ValueError("mask and bbox can not be set together")
        where = kwds.get("where")
        self.iterator = ItemsIterator(self, start, stop, step, bbox, mask, where)
        return self.iterator

    def keys(self, *args, **kwds):
        """Returns an iterator over FIDs, optionally
        filtered by a test for spatial intersection with the provided
        ``bbox``, a (minx, miny, maxx, maxy) tuple or a geometry
        ``mask``. An attribute filter can be set using an SQL ``where``
        clause, which uses the `OGR SQL dialect
        <https://gdal.org/user/ogr_sql_dialect.html#where>`__.

        Positional arguments ``stop`` or ``start, stop[, step]`` allows
        iteration to skip over items or stop at a specific item.

        Note: spatial filtering using ``mask`` may be inaccurate and returning
        all features overlapping the envelope of ``mask``.
        """
        if self.closed:
            raise ValueError("I/O operation on closed collection")
        elif self.mode != "r":
            raise OSError("collection not open for reading")
        if args:
            s = slice(*args)
            start = s.start
            stop = s.stop
            step = s.step
        else:
            start = stop = step = None
        bbox = kwds.get("bbox")
        mask = kwds.get("mask")
        if bbox and mask:
            raise ValueError("mask and bbox can not be set together")
        where = kwds.get("where")
        self.iterator = KeysIterator(self, start, stop, step, bbox, mask, where)
        return self.iterator

    def __contains__(self, fid):
        return self.session.has_feature(fid)

    values = filter

    def __iter__(self):
        """Returns an iterator over records."""
        return self.filter()

    def __next__(self):
        """Returns next record from iterator."""
        warnings.warn(
            "Collection.__next__() is buggy and will be removed in "
            "Fiona 2.0. Switch to `next(iter(collection))`.",
            FionaDeprecationWarning,
            stacklevel=2,
        )
        if not self.iterator:
            iter(self)
        return next(self.iterator)

    next = __next__

    def __getitem__(self, item):
        return self.session.__getitem__(item)

    def get(self, item):
        return self.session.get(item)

    def writerecords(self, records):
        """Stages multiple records for writing to disk."""
        if self.closed:
            raise ValueError("I/O operation on closed collection")
        if self.mode not in ("a", "w"):
            raise OSError("collection not open for writing")
        self.session.writerecs(records, self)
        self._len = self.session.get_length()
        self._bounds = None

    def write(self, record):
        """Stages a record for writing to disk.

        Note: Each call of this method will start and commit a
        unique transaction with the data source.
        """
        self.writerecords([record])

    def validate_record(self, record):
        """Compares the record to the collection's schema.

        Returns ``True`` if the record matches, else ``False``.
        """
        # Currently we only compare keys of properties, not the types of
        # values.
        return set(record["properties"].keys()) == set(
            self.schema["properties"].keys()
        ) and self.validate_record_geometry(record)

    def validate_record_geometry(self, record):
        """Compares the record's geometry to the collection's schema.

        Returns ``True`` if the record matches, else ``False``.
        """
        # Shapefiles welcome mixes of line/multis and polygon/multis.
        # OGR reports these mixed files as type "Polygon" or "LineString"
        # but will return either these or their multi counterparts when
        # reading features.
        if (
            self.driver == "ESRI Shapefile"
            and "Point" not in record["geometry"]["type"]
        ):
            return record["geometry"]["type"].lstrip("Multi") == self.schema[
                "geometry"
            ].lstrip("3D ").lstrip("Multi")
        else:
            return record["geometry"]["type"] == self.schema["geometry"].lstrip("3D ")

    def __len__(self):
        if self._len <= 0 and self.session is not None:
            self._len = self.session.get_length()
        if self._len < 0:
            # Raise TypeError when we don't know the length so that Python
            # will treat Collection as a generator
            raise TypeError("Layer does not support counting")
        return self._len

    @property
    def bounds(self):
        """Returns (minx, miny, maxx, maxy)."""
        if self._bounds is None and self.session is not None:
            self._bounds = self.session.get_extent()
        return self._bounds

    def _check_schema_driver_support(self):
        """Check support for the schema against the driver

        See GH#572 for discussion.
        """
        gdal_version_major = _GDAL_VERSION_TUPLE.major

        for field in self._schema["properties"].values():
            field_type = field.split(":")[0]

            if not _driver_supports_field(self.driver, field_type):
                if (
                    self.driver == "GPKG"
                    and gdal_version_major < 2
                    and field_type == "datetime"
                ):
                    raise DriverSupportError(
                        "GDAL 1.x GPKG driver does not support datetime fields"
                    )
                else:
                    raise DriverSupportError(
                        f"{self.driver} does not support {field_type} fields"
                    )
            elif (
                field_type
                in {
                    "time",
                    "datetime",
                    "date",
                }
                and _driver_converts_field_type_silently_to_str(self.driver, field_type)
            ):
                if (
                    self._driver == "GeoJSON"
                    and gdal_version_major < 2
                    and field_type in {"datetime", "date"}
                ):
                    warnings.warn(
                        "GeoJSON driver in GDAL 1.x silently converts "
                        f"{field_type} to string in non-standard format"
                    )
                else:
                    warnings.warn(
                        f"{self.driver} driver silently converts {field_type} "
                        "to string"
                    )

    def flush(self):
        """Flush the buffer."""
        if self.session is not None:
            self.session.sync(self)
            new_len = self.session.get_length()
            self._len = new_len > self._len and new_len or self._len
            self._bounds = None

    def close(self):
        """In append or write mode, flushes data to disk, then ends access."""
        if not self._closed:
            if self.session is not None and self.session.isactive():
                if self.mode in ("a", "w"):
                    self.flush()
                log.debug("Flushed buffer")
                self.session.stop()
                log.debug("Stopped session")
                self.session = None
                self.iterator = None
            if self._env:
                self._env.close()
                self._env = None
            self._closed = True

    @property
    def closed(self):
        """``False`` if data can be accessed, otherwise ``True``."""
        return self._closed

    def __enter__(self):
        self._env.enter_context(env_ctx_if_needed())
        logging.getLogger("fiona.ogrext").addFilter(self.field_skip_log_filter)
        return self

    def __exit__(self, type, value, traceback):
        logging.getLogger("fiona.ogrext").removeFilter(self.field_skip_log_filter)
        self.close()

    def __del__(self):
        # Note: you can't count on this being called. Call close() explicitly
        # or use the context manager protocol ("with").
        if not self._closed:
            self.close()


ALL_GEOMETRY_TYPES = {
        geom_type
        for geom_type in GEOMETRY_TYPES.values()
        if "3D " not in geom_type and geom_type != "None"
}
ALL_GEOMETRY_TYPES.add("None")


def _get_valid_geom_types(schema, driver):
    """Returns a set of geometry types the schema will accept"""
    schema_geom_type = schema["geometry"]
    if isinstance(schema_geom_type, str) or schema_geom_type is None:
        schema_geom_type = (schema_geom_type,)
    valid_types = set()
    for geom_type in schema_geom_type:
        geom_type = str(geom_type).lstrip("3D ")
        if geom_type == "Unknown" or geom_type == "Any":
            valid_types.update(ALL_GEOMETRY_TYPES)
        else:
            if geom_type not in ALL_GEOMETRY_TYPES:
                raise UnsupportedGeometryTypeError(geom_type)
            valid_types.add(geom_type)

    # shapefiles don't differentiate between single/multi geometries, except points
    if driver == "ESRI Shapefile" and "Point" not in valid_types:
        for geom_type in list(valid_types):
            if not geom_type.startswith("Multi"):
                valid_types.add("Multi" + geom_type)

    return valid_types


def get_filetype(bytesbuf):
    """Detect compression type of bytesbuf.

    ZIP only. TODO: add others relevant to GDAL/OGR."""
    if bytesbuf[:4].startswith(b"PK\x03\x04"):
        return "zip"
    else:
        return ""


class BytesCollection(Collection):
    """BytesCollection takes a buffer of bytes and maps that to
    a virtual file that can then be opened by fiona.
    """

    def __init__(self, bytesbuf, **kwds):
        """Takes buffer of bytes whose contents is something we'd like
        to open with Fiona and maps it to a virtual file.

        """
        self._closed = True

        if not isinstance(bytesbuf, bytes):
            raise ValueError("input buffer must be bytes")

        # Hold a reference to the buffer, as bad things will happen if
        # it is garbage collected while in use.
        self.bytesbuf = bytesbuf

        # Map the buffer to a file. If the buffer contains a zipfile
        # we take extra steps in naming the buffer and in opening
        # it. If the requested driver is for GeoJSON, we append an an
        # appropriate extension to ensure the driver reads it.
        filetype = get_filetype(self.bytesbuf)
        ext = ""
        if filetype == "zip":
            ext = ".zip"
        elif kwds.get("driver") == "GeoJSON":
            ext = ".json"
        self.virtual_file = buffer_to_virtual_file(self.bytesbuf, ext=ext)

        # Instantiate the parent class.
        super().__init__(self.virtual_file, vsi=filetype, **kwds)
        self._closed = False

    def close(self):
        """Removes the virtual file associated with the class."""
        super().close()
        if self.virtual_file:
            remove_virtual_file(self.virtual_file)
            self.virtual_file = None
            self.bytesbuf = None

    def __repr__(self):
        return "<{} BytesCollection '{}', mode '{}' at {}>".format(
            self.closed and "closed" or "open",
            self.path + ":" + str(self.name),
            self.mode,
            hex(id(self)),
        )
