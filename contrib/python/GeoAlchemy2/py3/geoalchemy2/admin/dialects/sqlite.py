"""This module defines specific functions for SQLite dialect."""

import os
from typing import Optional

from sqlalchemy import text
from sqlalchemy.dialects.sqlite.base import ischema_names as _sqlite_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _format_select_args
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster
from geoalchemy2.types import _DummyGeometry
from geoalchemy2.utils import authorized_values_in_docstring

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_sqlite_ischema_names["GEOMETRY"] = Geometry
_sqlite_ischema_names["POINT"] = Geometry
_sqlite_ischema_names["LINESTRING"] = Geometry
_sqlite_ischema_names["POLYGON"] = Geometry
_sqlite_ischema_names["MULTIPOINT"] = Geometry
_sqlite_ischema_names["MULTILINESTRING"] = Geometry
_sqlite_ischema_names["MULTIPOLYGON"] = Geometry
_sqlite_ischema_names["CURVE"] = Geometry
_sqlite_ischema_names["GEOMETRYCOLLECTION"] = Geometry
_sqlite_ischema_names["RASTER"] = Raster


def load_spatialite_driver(dbapi_conn, *args):
    """Load SpatiaLite extension in SQLite connection.

    .. Warning::
        The path to the SpatiaLite module should be set in the `SPATIALITE_LIBRARY_PATH`
        environment variable.

    Args:
        dbapi_conn: The DBAPI connection.
    """
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        raise RuntimeError("The SPATIALITE_LIBRARY_PATH environment variable is not set.")
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension(os.environ["SPATIALITE_LIBRARY_PATH"])
    dbapi_conn.enable_load_extension(False)


_JOURNAL_MODE_VALUES = ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"]


@authorized_values_in_docstring(JOURNAL_MODE_VALUES=_JOURNAL_MODE_VALUES)
def init_spatialite(
    dbapi_conn,
    *args,
    transaction: bool = False,
    init_mode: Optional[str] = None,
    journal_mode: Optional[str] = None,
):
    """Initialize internal SpatiaLite tables.

    Args:
        dbapi_conn: The DBAPI connection.
        transaction: If set to `True` the whole operation will be handled as a single Transaction
            (faster). The default value is `False` (slower, but safer).
        init_mode: Can be `None` to load all EPSG SRIDs, `'WGS84'` to load only the ones related
            to WGS84 or `'EMPTY'` to not load any EPSG SRID.

            .. Note::

                It is possible to load other EPSG SRIDs afterwards using `InsertEpsgSrid(srid)`.

        journal_mode: Change the journal mode to the given value. This can make the table creation
            much faster. The possible values are the following: <JOURNAL_MODE_VALUES>. See
            https://www.sqlite.org/pragma.html#pragma_journal_mode for more details.

            .. Warning::
                Some values, like 'MEMORY' or 'OFF', can lead to corrupted databases if the process
                is interrupted during initialization.

            .. Note::
                The original value is restored after the initialization.

    .. Note::
        When using this function as a listener it is not possible to pass the `transaction`,
        `init_mode` or `journal_mode` arguments directly. To do this you can either create another
        function that calls `init_spatialite` (or
        :func:`geoalchemy2.admin.dialects.sqlite.load_spatialite` if you also want to load the
        SpatiaLite drivers) with an hard-coded `init_mode` or just use a lambda::

            >>> sqlalchemy.event.listen(
            ...     engine,
            ...     "connect",
            ...     lambda x, y: init_spatialite(
            ...         x,
            ...         y,
            ...         transaction=True,
            ...         init_mode="EMPTY",
            ...         journal_mode="OFF",
            ...     )
            ... )
    """
    func_args = []

    # Check the value of the 'transaction' parameter
    if not isinstance(transaction, (bool, int)):
        raise ValueError("The 'transaction' argument must be True or False.")
    else:
        func_args.append(str(transaction))

    # Check the value of the 'init_mode' parameter
    init_mode_values = ["WGS84", "EMPTY"]
    if isinstance(init_mode, str):
        init_mode = init_mode.upper()
    if init_mode is not None:
        if init_mode not in init_mode_values:
            raise ValueError("The 'init_mode' argument must be one of {}.".format(init_mode_values))
        func_args.append(f"'{init_mode}'")

    # Check the value of the 'journal_mode' parameter
    if isinstance(journal_mode, str):
        journal_mode = journal_mode.upper()
    if journal_mode is not None:
        if journal_mode not in _JOURNAL_MODE_VALUES:
            raise ValueError(
                "The 'journal_mode' argument must be one of {}.".format(_JOURNAL_MODE_VALUES)
            )

    if dbapi_conn.execute("SELECT CheckSpatialMetaData();").fetchone()[0] < 1:
        if journal_mode is not None:
            current_journal_mode = dbapi_conn.execute("PRAGMA journal_mode").fetchone()[0]
            dbapi_conn.execute("PRAGMA journal_mode = {}".format(journal_mode))

        dbapi_conn.execute("SELECT InitSpatialMetaData({});".format(", ".join(func_args)))

        if journal_mode is not None:
            dbapi_conn.execute("PRAGMA journal_mode = {}".format(current_journal_mode))


def load_spatialite(dbapi_conn, *args, **kwargs):
    """Load SpatiaLite extension in SQLite DB and initialize internal tables.

    See :func:`geoalchemy2.admin.dialects.sqlite.load_spatialite_driver` and
    :func:`geoalchemy2.admin.dialects.sqlite.init_spatialite` functions for details about
    arguments.
    """
    load_spatialite_driver(dbapi_conn)
    init_spatialite(dbapi_conn, **kwargs)


def _get_spatialite_attrs(bind, table_name, col_name):
    attrs = bind.execute(
        text(
            """SELECT * FROM "geometry_columns"
            WHERE LOWER(f_table_name) = LOWER(:table_name)
                AND LOWER(f_geometry_column) = LOWER(:column_name)
        """
        ).bindparams(table_name=table_name, column_name=col_name)
    ).fetchone()
    if attrs is None:
        # If the column is not registered as a spatial column we ignore it
        return None
    return attrs[2:]


def get_spatialite_version(bind):
    """Get the version of the currently loaded Spatialite extension."""
    return bind.execute(text("SELECT spatialite_version();")).fetchone()[0]


def _setup_dummy_type(table, gis_cols):
    """Setup dummy type for new Geometry columns so they can be updated later."""
    for col in gis_cols:
        # Add dummy columns with GEOMETRY type
        col._actual_type = col.type
        col.type = _DummyGeometry()
    table.columns = table.info["_saved_columns"]


def get_col_dim(col):
    """Get dimension of the column type."""
    if col.type.dimension == 4:
        dimension = "XYZM"
    elif col.type.dimension == 2 or col.type.geometry_type is None:
        dimension = "XY"
    else:
        if col.type.geometry_type.endswith("M"):
            dimension = "XYM"
        else:
            dimension = "XYZ"
    return dimension


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    if col.computed is not None:
        # Do not create spatial index for computed columns
        return
    stmt = select(*_format_select_args(func.CreateSpatialIndex(table.name, col.name)))
    stmt = stmt.execution_options(autocommit=True)
    bind.execute(stmt)


def disable_spatial_index(bind, table, col):
    """Disable spatial indexes if present."""
    if col.computed is not None:
        # Do not disable spatial index for computed columns because it can not exist
        return
    # Check if the spatial index is enabled
    stmt = select(*_format_select_args(func.CheckSpatialIndex(table.name, col.name)))
    if bind.execute(stmt).fetchone()[0] is not None:
        stmt = select(*_format_select_args(func.DisableSpatialIndex(table.name, col.name)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)
        bind.execute(
            text(
                "DROP TABLE IF EXISTS {};".format(
                    _spatial_idx_name(
                        table.name,
                        col.name,
                    )
                )
            )
        )


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with SQLite dialect."""
    # Get geometry type, SRID and spatial index from the SpatiaLite metadata
    if not isinstance(column_info.get("type"), Geometry):
        return
    col_attributes = _get_spatialite_attrs(inspector.bind, table.name, column_info["name"])
    if col_attributes is not None:
        geometry_type, coord_dimension, srid, spatial_index = col_attributes

        if isinstance(geometry_type, int):
            geometry_type_str = str(geometry_type)
            if geometry_type >= 1000:
                first_digit = geometry_type_str[0]
                has_z = first_digit in ["1", "3"]
                has_m = first_digit in ["2", "3"]
            else:
                has_z = has_m = False
            geometry_type = {
                "0": "GEOMETRY",
                "1": "POINT",
                "2": "LINESTRING",
                "3": "POLYGON",
                "4": "MULTIPOINT",
                "5": "MULTILINESTRING",
                "6": "MULTIPOLYGON",
                "7": "GEOMETRYCOLLECTION",
            }[geometry_type_str[-1]]
            if has_z:
                geometry_type += "Z"
            if has_m:
                geometry_type += "M"
        else:
            if "Z" in coord_dimension and "Z" not in geometry_type[-2:]:
                geometry_type += "Z"
            if "M" in coord_dimension and "M" not in geometry_type[-2:]:
                geometry_type += "M"
            coord_dimension = {
                "XY": 2,
                "XYZ": 3,
                "XYM": 3,
                "XYZM": 4,
            }.get(coord_dimension, coord_dimension)

        # Set attributes
        column_info["type"].geometry_type = geometry_type
        column_info["type"].dimension = coord_dimension
        column_info["type"].srid = srid
        column_info["type"].spatial_index = bool(spatial_index)

        # Spatial indexes are not automatically reflected with SQLite dialect
        column_info["type"]._spatial_index_reflected = False


def connect(dbapi_conn, *args, **kwargs):
    """Even handler to load spatial extension when a new connection is created."""
    return load_spatialite(dbapi_conn, *args, **kwargs)


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if _check_spatial_type(col.type, Geometry, dialect) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)

    _setup_dummy_type(table, gis_cols)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    dialect = bind.dialect

    table.columns = table.info.pop("_saved_columns")
    for col in table.columns:
        # Add the managed Geometry columns with RecoverGeometryColumn()
        if _check_spatial_type(col.type, Geometry, dialect) and col.computed is None:
            col.type = col._actual_type
            del col._actual_type
            dimension = get_col_dim(col)
            args = [
                table.name,
                col.name,
                col.type.srid,
                col.type.geometry_type or "GEOMETRY",
                dimension,
            ]

            stmt = select(*_format_select_args(func.RecoverGeometryColumn(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)

    for col in table.columns:
        # Add spatial indexes for the Geometry and Geography columns
        # TODO: Check that the Geography type makes sense here
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
        ):
            create_spatial_index(bind, table, col)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)
        idx.create(bind=bind)


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)

    for col in gis_cols:
        if col.computed is not None:
            # Computed columns are not managed
            continue
        # Disable spatial indexes if present
        disable_spatial_index(bind, table, col)

        args = [table.name, col.name]

        stmt = select(*_format_select_args(func.DiscardGeometryColumn(*args)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    table.columns = table.info.pop("_saved_columns")


# Define compiled versions for functions in SpatiaLite whose names don't have
# the ST_ prefix.
_SQLITE_FUNCTIONS = {
    "ST_GeomFromEWKT": "GeomFromEWKT",
    # "ST_GeomFromEWKB": "GeomFromEWKB",
    "ST_AsBinary": "AsBinary",
    "ST_AsEWKB": "AsEWKB",
    "ST_AsGeoJSON": "AsGeoJSON",
}


def _compiles_sqlite(cls, fn):
    def _compile_sqlite(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "sqlite")(_compile_sqlite)


def register_sqlite_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "sqlite_function_name_1",
                    "function_name_2": "sqlite_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_sqlite(cls, fn)


register_sqlite_mapping(_SQLITE_FUNCTIONS)


def _compile_GeomFromWKB_SQLite(element, compiler, *, identifier, **kw):
    element.identifier = identifier

    # Store the SRID
    clauses = list(element.clauses)
    try:
        srid = clauses[1].value
        element.type.srid = srid
    except (IndexError, TypeError, ValueError):
        srid = element.type.srid

    if kw.get("literal_binds", False):
        wkb_clause = compile_bin_literal(clauses[0])
        prefix = "unhex("
        suffix = ")"
    else:
        wkb_clause = clauses[0]
        prefix = ""
        suffix = ""

    compiled = compiler.process(wkb_clause, **kw)

    if srid > 0:
        return "{}({}{}{}, {})".format(identifier, prefix, compiled, suffix, srid)
    else:
        return "{}({}{}{})".format(identifier, prefix, compiled, suffix)


@compiles(functions.ST_GeomFromWKB, "sqlite")  # type: ignore
def _SQLite_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_SQLite(element, compiler, identifier="GeomFromWKB", **kw)


@compiles(functions.ST_GeomFromEWKB, "sqlite")  # type: ignore
def _SQLite_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_SQLite(element, compiler, identifier="GeomFromEWKB", **kw)
