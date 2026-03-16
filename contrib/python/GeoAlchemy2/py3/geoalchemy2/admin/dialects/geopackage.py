"""This module defines specific functions for GeoPackage dialect.

See GeoPackage specifications here: http://www.geopackage.org/spec/
"""

import re

from sqlalchemy import text
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _format_select_args
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.admin.dialects.sqlite import _SQLITE_FUNCTIONS
from geoalchemy2.admin.dialects.sqlite import get_col_dim
from geoalchemy2.admin.dialects.sqlite import load_spatialite_driver
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import _DummyGeometry


class GeoPackageDialect(SQLiteDialect_pysqlite):
    """Define a specific dialect for GeoPackage."""

    name = "geopackage"
    driver = "gpkg"

    supports_statement_cache = True
    """Enable caching for GeoPackage dialect."""


registry.register("gpkg", "geoalchemy2.admin.dialects.geopackage", "GeoPackageDialect")


def load_geopackage_driver(dbapi_conn, *args):
    """Load SpatiaLite extension in GeoPackage connection and set VirtualGpkg and Amphibious modes.

    .. Warning::
        The path to the SpatiaLite module should be set in the `SPATIALITE_LIBRARY_PATH`
        environment variable.

    Args:
        dbapi_conn: The DBAPI connection.
    """
    load_spatialite_driver(dbapi_conn, *args)

    dbapi_conn.execute("SELECT AutoGpkgStart();")
    dbapi_conn.execute("SELECT EnableGpkgAmphibiousMode();")


def init_geopackage(dbapi_conn, *args):
    """Initialize GeoPackage tables.

    Args:
        dbapi_conn: The DBAPI connection.

    .. Warning::
        No EPSG SRID is loaded in the `gpkg_spatial_ref_sys` table after initialization but
        it is possible to load other EPSG SRIDs afterwards using the
        `gpkgInsertEpsgSRID(srid)`.
        Nevertheless, SRIDs of newly created tables are automatically added.
    """
    if not dbapi_conn.execute("SELECT CheckGeoPackageMetaData();").fetchone()[0]:
        # This only works on the main database
        dbapi_conn.execute("SELECT gpkgCreateBaseTables();")


def load_spatialite_gpkg(dbapi_conn, *args, **kwargs):
    """Load SpatiaLite extension in GeoPackage and initialize internal tables.

    See :func:`geoalchemy2.admin.dialects.geopackage.load_geopackage_driver` and
    :func:`geoalchemy2.admin.dialects.geopackage.init_geopackage` functions for details about
    arguments.
    """
    load_geopackage_driver(dbapi_conn)
    init_geopackage(dbapi_conn, **kwargs)


def _get_spatialite_attrs(bind, table_name, col_name):
    attrs = bind.execute(
        text(
            """SELECT
                A.geometry_type_name,
                A.srs_id,
                A.z,
                A.m,
                IFNULL(B.has_index, 0) AS has_index
            FROM gpkg_geometry_columns
            AS A
            LEFT JOIN (
                SELECT table_name, column_name, COUNT(*) AS has_index
                FROM gpkg_extensions
                WHERE LOWER(table_name) = LOWER(:table_name)
                    AND column_name = :column_name
                    AND extension_name = 'gpkg_rtree_index'
            ) AS B
            ON LOWER(A.table_name) = LOWER(B.table_name)
                AND A.column_name = B.column_name
            WHERE LOWER(A.table_name) = LOWER(:table_name)
                AND A.column_name = :column_name;
        """
        ).bindparams(table_name=table_name, column_name=col_name)
    ).fetchone()
    if attrs is None:
        # If the column is not registered as a spatial column we ignore it
        return None
    geometry_type, srid, has_z, has_m, has_index = attrs
    coord_dimension = "XY"
    if has_z:
        coord_dimension += "Z"
    if has_m:
        coord_dimension += "M"
    col_attributes = geometry_type, coord_dimension, srid, has_index
    return col_attributes


def _setup_dummy_type(table, gis_cols):
    """Setup dummy type for new Geometry columns so they can be updated later."""
    for col in gis_cols:
        # Add dummy columns with GEOMETRY type
        type_str = re.fullmatch("(.+?)[ZMzm]*", col.type.geometry_type).group(1)
        col._actual_type = col.type
        col.type = _DummyGeometry(geometry_type=type_str)
    table.columns = table.info["_saved_columns"]


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    stmt = select(*_format_select_args(func.gpkgAddSpatialIndex(table.name, col.name)))
    stmt = stmt.execution_options(autocommit=True)
    bind.execute(stmt)


def disable_spatial_index(bind, table, col):
    """Disable spatial indexes if present."""
    for i in ["", "_node", "_parent", "_rowid"]:
        bind.execute(
            text(
                "DROP TABLE IF EXISTS rtree_{}_{}{};".format(
                    table.name,
                    col.name,
                    i,
                )
            )
        )
    bind.execute(
        text(
            """DELETE FROM gpkg_extensions
            WHERE LOWER(table_name) = LOWER(:table_name)
                AND column_name = :column_name
                AND extension_name = 'gpkg_rtree_index';"""
        ).bindparams(
            table_name=table.name,
            column_name=col.name,
        )
    )


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with GeoPackage dialect."""
    # Get geometry type, SRID and spatial index from the SpatiaLite metadata
    if not isinstance(column_info.get("type"), Geometry):
        return
    col_attributes = _get_spatialite_attrs(inspector.bind, table.name, column_info["name"])
    if col_attributes is not None:
        geometry_type, coord_dimension, srid, spatial_index = col_attributes

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

        # Spatial indexes are not automatically reflected with GeoPackage dialect
        column_info["type"]._spatial_index_reflected = False


def connect(dbapi_conn, *args, **kwargs):
    return load_spatialite_gpkg(dbapi_conn, *args, **kwargs)


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

    if len(gis_cols) > 1:
        raise ValueError("Only one geometry column is allowed for a table stored in a GeoPackage.")
    elif len(gis_cols) == 1:
        col = gis_cols[0]
        srid = col.type.srid

        if col.type.geometry_type is None:
            col.type.geometry_type = "GEOMETRY"

        # Add the SRID of the table in 'gpkg_spatial_ref_sys' if this table exists
        if not bind.execute(
            text("SELECT COUNT(*) FROM gpkg_spatial_ref_sys WHERE srs_id = :srid;").bindparams(
                srid=srid
            )
        ).scalar():
            bind.execute(text("SELECT gpkgInsertEpsgSRID(:srid)").bindparams(srid=srid))
    _setup_dummy_type(table, gis_cols)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    dialect = bind.dialect

    for col in table.columns:
        # Add the managed Geometry columns with gpkgAddGeometryColumn()
        if _check_spatial_type(col.type, Geometry, dialect):
            col.type = col._actual_type
            del col._actual_type
            dimension = get_col_dim(col)
            has_z = "Z" in dimension
            has_m = "M" in dimension

            bind.execute(
                text(
                    """INSERT INTO gpkg_contents
                    VALUES (
                        :table_name,
                        'features',
                        :table_name,
                        "",
                        strftime('%Y-%m-%dT%H:%M:%fZ', CURRENT_TIMESTAMP),
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        :srid
                    );"""
                ).bindparams(
                    table_name=table.name,
                    srid=col.type.srid,
                )
            )
            bind.execute(
                text(
                    """INSERT INTO gpkg_geometry_columns
                    VALUES (:table_name, :column_name, :geometry_type, :srid, :has_z, :has_m);"""
                ).bindparams(
                    table_name=table.name,
                    column_name=col.name,
                    geometry_type=col.type.geometry_type,
                    srid=col.type.srid,
                    has_z=has_z,
                    has_m=has_m,
                )
            )
            stmt = select(*_format_select_args(func.gpkgAddGeometryTriggers(table.name, col.name)))
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
        # Disable spatial indexes if present
        # TODO: This is useless but if we remove it then the disable_spatial_index should be
        # tested separately
        disable_spatial_index(bind, table, col)

        # Remove metadata from internal tables
        # (this is equivalent to DiscardGeometryColumn but for GeoPackage)
        bind.execute(
            text(
                """DELETE FROM gpkg_extensions
                WHERE LOWER(table_name) = LOWER(:table_name)
                    AND column_name = :column_name;"""
            ).bindparams(
                table_name=table.name,
                column_name=col.name,
            )
        )
        bind.execute(
            text(
                """DELETE FROM gpkg_geometry_columns
                WHERE LOWER(table_name) = LOWER(:table_name)
                    AND column_name = :column_name;"""
            ).bindparams(
                table_name=table.name,
                column_name=col.name,
            )
        )
        bind.execute(
            text(
                """DELETE FROM gpkg_contents
                WHERE LOWER(table_name) = LOWER(:table_name);"""
            ).bindparams(table_name=table.name)
        )


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    table.columns = table.info.pop("_saved_columns")


def _compiles_gpkg(cls, fn):
    def _compile_gpkg(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "geopackage")(_compile_gpkg)


def register_gpkg_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "gpkg_function_name_1",
                    "function_name_2": "gpkg_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_gpkg(cls, fn)


register_gpkg_mapping(_SQLITE_FUNCTIONS)


def create_spatial_ref_sys_view(bind):
    """Create the `spatial_ref_sys` view from the `gpkg_spatial_ref_sys` table.

    .. Note::

        This is usually only needed to use the `ST_Transform` function on GeoPackage data
        because this function, when used with SpatiaLite, requires the `spatial_ref_sys` table.
    """
    bind.execute(
        text(
            """CREATE VIEW spatial_ref_sys AS
            SELECT
                srs_id AS srid,
                organization AS auth_name,
                organization_coordsys_id AS auth_srid,
                definition AS srtext
            FROM gpkg_spatial_ref_sys;"""
        )
    )


def _compile_GeomFromWKB_gpkg(element, compiler, *, identifier, **kw):
    # Store the SRID
    clauses = list(element.clauses)
    try:
        srid = clauses[1].value
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


@compiles(functions.ST_GeomFromWKB, "geopackage")  # type: ignore
def _gpkg_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_gpkg(element, compiler, identifier="GeomFromWKB", **kw)


@compiles(functions.ST_GeomFromEWKB, "geopackage")  # type: ignore
def _gpkg_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_gpkg(element, compiler, identifier="GeomFromEWKB", **kw)
