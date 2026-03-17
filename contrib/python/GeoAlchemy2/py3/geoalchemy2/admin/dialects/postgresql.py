"""This module defines specific functions for Postgresql dialect."""

import sqlalchemy
from packaging import version
from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.base import ischema_names as _postgresql_ischema_names
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

_SQLALCHEMY_VERSION_BEFORE_2 = version.parse(sqlalchemy.__version__) < version.parse("2")

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_postgresql_ischema_names["geometry"] = Geometry
_postgresql_ischema_names["geography"] = Geography
_postgresql_ischema_names["raster"] = Raster


def check_management(column):
    """Check if the column should be managed."""
    if _check_spatial_type(column.type, Raster):
        # Raster columns are not managed
        return _SQLALCHEMY_VERSION_BEFORE_2
    return getattr(column.type, "use_typmod", None) is False


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    if col.type.use_N_D_index:
        postgresql_ops = {col.name: "gist_geometry_ops_nd"}
    else:
        postgresql_ops = {}
    if _check_spatial_type(col.type, Raster):
        col_func = func.ST_ConvexHull(col)
    else:
        col_func = col
    idx = Index(
        _spatial_idx_name(table.name, col.name),
        col_func,
        postgresql_using="gist",
        postgresql_ops=postgresql_ops,
        _column_flag=True,
    )
    if bind is not None:
        idx.create(bind=bind)
    return idx


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not _check_spatial_type(column_info.get("type"), (Geometry, Geography, Raster)):
        return
    geo_type = column_info["type"]
    geometry_type = geo_type.geometry_type
    coord_dimension = geo_type.dimension
    if geometry_type is not None:
        if geometry_type.endswith("ZM"):
            coord_dimension = 4
        elif geometry_type[-1] in ["Z", "M"]:
            coord_dimension = 3

    # Query to check a given column has spatial index
    if table.schema is not None:
        schema_part = " AND nspname = '{}'".format(table.schema)
    else:
        schema_part = ""

    # Check if the column has a spatial index (the regular expression checks for the column name
    # in the index definition, which is required for functional indexes)
    has_index_query = """SELECT EXISTS (
        SELECT 1
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        WHERE
            t.relname = '{table_name}'{schema_part}
            AND am.amname = 'gist'
            AND (
                EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    WHERE a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND a.attname = '{col_name}'
                )
                OR pg_get_indexdef(
                    ix.indexrelid
                ) ~ '(^|[^a-zA-Z0-9_])("?{col_name}"?)($|[^a-zA-Z0-9_])'
            )
    );""".format(
        table_name=table.name, col_name=column_info["name"], schema_part=schema_part
    )
    spatial_index = inspector.bind.execute(text(has_index_query)).scalar()

    # Set attributes
    if not _check_spatial_type(column_info["type"], Raster):
        column_info["type"].geometry_type = geometry_type
        column_info["type"].dimension = coord_dimension
    column_info["type"].spatial_index = bool(spatial_index)

    # Spatial indexes are automatically reflected with PostgreSQL dialect
    column_info["type"]._spatial_index_reflected = True


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind, check_management)

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (
                _check_spatial_type(col.type, (Geometry, Raster), dialect) and check_management(col)
            ) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect

    table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add the managed Geometry columns with AddGeometryColumn()
        if _check_spatial_type(col.type, Geometry, dialect) and check_management(col):
            dimension = col.type.dimension
            args = [table.schema] if table.schema else []
            args.extend([table.name, col.name, col.type.srid, col.type.geometry_type, dimension])
            if col.type.use_typmod is not None:
                args.append(col.type.use_typmod)

            stmt = select(*_format_select_args(func.AddGeometryColumn(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)

        # Add spatial indices for the Geometry, Geography and Raster columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography, Raster), dialect)
            and col.type.spatial_index is True
        ):
            # If the index does not exist, define it and create it
            if not [i for i in table.indexes if col in i.columns.values()] and check_management(
                col
            ):
                create_spatial_index(bind, table, col)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)
        idx.create(bind=bind)


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind, check_management)

    # Drop the managed Geometry columns
    for col in gis_cols:
        if _check_spatial_type(col.type, Raster):
            # Raster columns are dropped with the table, no need to drop them separately
            continue
        args = [table.schema] if table.schema else []
        args.extend([table.name, col.name])

        stmt = select(*_format_select_args(func.DropGeometryColumn(*args)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    # Restore original column list including managed Geometry columns
    saved_cols = table.info.pop("_saved_columns", None)
    if saved_cols is not None:
        table.columns = saved_cols


def _compile_GeomFromWKB_Postgresql(element, compiler, **kw):
    # Store the SRID
    clauses = list(element.clauses)
    try:
        srid = clauses[1].value
    except (IndexError, TypeError, ValueError):
        srid = element.type.srid

    if kw.get("literal_binds", False):
        wkb_clause = compile_bin_literal(clauses[0])
        prefix = "decode("
        suffix = ", 'hex')"
    else:
        wkb_clause = clauses[0]
        prefix = ""
        suffix = ""

    compiled = compiler.process(wkb_clause, **kw)

    if srid > 0:
        return "{}({}{}{}, {})".format(element.identifier, prefix, compiled, suffix, srid)
    else:
        return "{}({}{}{})".format(element.identifier, prefix, compiled, suffix)


@compiles(functions.ST_GeomFromWKB, "postgresql")  # type: ignore
def _PostgreSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Postgresql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "postgresql")  # type: ignore
def _PostgreSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Postgresql(element, compiler, **kw)
