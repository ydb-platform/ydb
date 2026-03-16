"""This module defines specific functions for MySQL dialect."""

from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import ischema_names as _mysql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_mysql_ischema_names["geometry"] = Geometry
_mysql_ischema_names["point"] = Geometry
_mysql_ischema_names["linestring"] = Geometry
_mysql_ischema_names["polygon"] = Geometry
_mysql_ischema_names["multipoint"] = Geometry
_mysql_ischema_names["multilinestring"] = Geometry
_mysql_ischema_names["multipolygon"] = Geometry
_mysql_ischema_names["geometrycollection"] = Geometry


_POSSIBLE_TYPES = [
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
]


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not isinstance(column_info.get("type"), (Geometry, NullType)):
        return

    column_name = column_info.get("name")
    schema = table.schema or inspector.default_schema_name

    if inspector.dialect.name == "mariadb":
        select_srid = "-1, "
    else:
        select_srid = "SRS_ID, "

    # Check geometry type, SRID and if the column is nullable
    geometry_type_query = """SELECT DATA_TYPE, {}IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{}' and COLUMN_NAME = '{}'""".format(
        select_srid, table.name, column_name
    )
    if schema is not None:
        geometry_type_query += """ and table_schema = '{}'""".format(schema)
    geometry_type, srid, nullable_str = inspector.bind.execute(text(geometry_type_query)).one()
    is_nullable = str(nullable_str).lower() == "yes"

    if geometry_type not in _POSSIBLE_TYPES:
        return  # pragma: no cover

    # Check if the column has spatial index
    has_index_query = """SELECT DISTINCT
            INDEX_TYPE
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_NAME = '{}' and COLUMN_NAME = '{}'""".format(
        table.name, column_name
    )
    if schema is not None:
        has_index_query += """ and TABLE_SCHEMA = '{}'""".format(schema)
    spatial_index_res = inspector.bind.execute(text(has_index_query)).scalar()
    spatial_index = str(spatial_index_res).lower() == "spatial"

    # Set attributes
    column_info["type"] = Geometry(
        geometry_type=geometry_type.upper(),
        srid=srid,
        spatial_index=spatial_index,
        nullable=is_nullable,
        _spatial_index_reflected=True,
    )


def before_cursor_execute(
    conn, cursor, statement, parameters, context, executemany, convert=True
):  # noqa: D417
    """Event handler to cast the parameters properly.

    Args:
        convert (bool): Trigger the conversion.
    """
    if convert:
        if isinstance(parameters, (tuple, list)):
            parameters = tuple(x.tobytes() if isinstance(x, memoryview) else x for x in parameters)
        elif isinstance(parameters, dict):
            for k in parameters:
                if isinstance(parameters[k], memoryview):
                    parameters[k] = parameters[k].tobytes()

    return statement, parameters


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
            if (_check_spatial_type(col.type, Geometry, dialect)) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)

    table.columns = table.info.pop("_saved_columns")


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect

    # table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
            and col.computed is None
        ):
            # If the index does not exist, define it and create it
            if not [i for i in table.indexes if col in i.columns.values()]:
                sql = "ALTER TABLE {} ADD SPATIAL INDEX({});".format(table.name, col.name)
                q = text(sql)
                bind.execute(q)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


_MYSQL_FUNCTIONS = {"ST_AsEWKB": "ST_AsBinary", "ST_SetSRID": "ST_SRID"}


def _compiles_mysql(cls, fn):
    def _compile_mysql(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "mysql")(_compile_mysql)


def register_mysql_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mysql_function_name_1",
                    "function_name_2": "mysql_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mysql(cls, fn)


register_mysql_mapping(_MYSQL_FUNCTIONS)


def _compile_GeomFromText_MySql(element, compiler, **kw):
    identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(identifier, compiled, srid)
    else:
        return "{}({})".format(identifier, compiled)


def _compile_GeomFromWKB_MySql(element, compiler, **kw):
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
        return "{}({}{}{}, {})".format(element.identifier, prefix, compiled, suffix, srid)
    else:
        return "{}({}{}{})".format(element.identifier, prefix, compiled, suffix)


@compiles(functions.ST_GeomFromText, "mysql")  # type: ignore
def _MySQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mysql")  # type: ignore
def _MySQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mysql")  # type: ignore
def _MySQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mysql")  # type: ignore
def _MySQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(element, compiler, **kw)
