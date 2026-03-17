"""Some helpers to use with Alembic migration tool."""

from alembic.autogenerate import renderers
from alembic.autogenerate import rewriter
from alembic.autogenerate.render import _add_column
from alembic.autogenerate.render import _add_index
from alembic.autogenerate.render import _add_table
from alembic.autogenerate.render import _drop_column
from alembic.autogenerate.render import _drop_index
from alembic.autogenerate.render import _drop_table
from alembic.ddl.base import RenameTable
from alembic.ddl.base import format_table_name
from alembic.ddl.base import visit_rename_table
from alembic.ddl.sqlite import SQLiteImpl
from alembic.operations import BatchOperations
from alembic.operations import Operations
from alembic.operations import ops
from sqlalchemy import Column
from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DropTable
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _get_gis_cols
from geoalchemy2.admin.dialects.common import _spatial_idx_name

writer = rewriter.Rewriter()
"""Rewriter object for Alembic."""

_SPATIAL_TABLES = set()


class GeoPackageImpl(SQLiteImpl):
    """Class to copy the Alembic implementation from SQLite to GeoPackage."""

    __dialect__ = "geopackage"


def _monkey_patch_get_indexes_for_sqlite():
    """Monkey patch SQLAlchemy to fix spatial index reflection."""
    normal_behavior = SQLiteDialect.get_indexes

    def spatial_behavior(self, connection, table_name, schema=None, **kw):
        indexes = self._get_indexes_normal_behavior(connection, table_name, schema=None, **kw)

        is_gpkg = connection.dialect.name == "geopackage"

        try:
            # Check that SpatiaLite was loaded into the DB
            is_spatial_db = connection.exec_driver_sql(
                """PRAGMA main.table_info({})""".format(
                    "gpkg_geometry_columns" if is_gpkg else "geometry_columns"
                )
            ).fetchall()
            if not is_spatial_db:
                return indexes
        except AttributeError:
            return indexes

        # Get spatial indexes
        if is_gpkg:
            spatial_index_query = text(
                """SELECT A.table_name, A.column_name, IFNULL(B.has_index, 0) AS has_index
                FROM "gpkg_geometry_columns"
                AS A
                LEFT JOIN (
                    SELECT table_name, column_name, COUNT(*) AS has_index
                    FROM gpkg_extensions
                    WHERE LOWER(table_name) = LOWER('{table_name}')
                        AND extension_name = 'gpkg_rtree_index'
                ) AS B
                ON LOWER(A.table_name) = LOWER(B.table_name)
                WHERE LOWER(A.table_name) = LOWER('{table_name}');
            """.format(
                    table_name=table_name
                )
            )
        else:
            spatial_index_query = text(
                """SELECT *
                FROM geometry_columns
                WHERE f_table_name = '{}'
                ORDER BY f_table_name, f_geometry_column;""".format(
                    table_name
                )
            )

        spatial_indexes = connection.execute(spatial_index_query).fetchall()

        if spatial_indexes:
            reflected_names = set([i["name"] for i in indexes])
            for idx in spatial_indexes:
                idx_col = idx[1]
                idx_name = _spatial_idx_name(table_name, idx_col)
                if not bool(idx[-1]) or idx_name in reflected_names:
                    continue
                indexes.append(
                    {
                        "name": idx_name,
                        "column_names": [idx_col],
                        "unique": 0,
                        "dialect_options": {"_column_flag": True},
                    }
                )
                reflected_names.add(idx_name)

        return indexes

    spatial_behavior.__doc__ = normal_behavior.__doc__
    SQLiteDialect.get_indexes = spatial_behavior
    SQLiteDialect._get_indexes_normal_behavior = normal_behavior


_monkey_patch_get_indexes_for_sqlite()


def _monkey_patch_get_indexes_for_mysql():
    """Monkey patch SQLAlchemy to fix spatial index reflection."""
    normal_behavior = MySQLDialect.get_indexes

    def spatial_behavior(self, connection, table_name, schema=None, **kw):
        indexes = self._get_indexes_normal_behavior(connection, table_name, schema=None, **kw)

        # Get spatial indexes
        has_index_query = """SELECT DISTINCT
                COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_NAME = '{}' AND INDEX_TYPE = 'SPATIAL'""".format(
            table_name
        )
        if schema is not None:
            has_index_query += """ AND TABLE_SCHEMA = '{}'""".format(schema)
        spatial_indexes = connection.execute(text(has_index_query)).fetchall()

        if spatial_indexes:
            reflected_names = set([i["name"] for i in indexes])
            for idx in spatial_indexes:
                idx_col = idx[0]
                idx_name = _spatial_idx_name(table_name, idx_col)
                if idx_name in reflected_names:
                    continue
                indexes.append(
                    {
                        "name": idx_name,
                        "column_names": [idx_col],
                        "unique": 0,
                        "dialect_options": {"_column_flag": True},
                    }
                )
                reflected_names.add(idx_name)

        return indexes

    spatial_behavior.__doc__ = normal_behavior.__doc__
    MySQLDialect.get_indexes = spatial_behavior
    MySQLDialect._get_indexes_normal_behavior = normal_behavior


_monkey_patch_get_indexes_for_mysql()


def render_item(obj_type, obj, autogen_context):
    """Add proper imports for spatial types."""
    if obj_type == "type" and isinstance(obj, (Geometry, Geography, Raster)):
        import_name = obj.__class__.__name__
        autogen_context.imports.add(f"from geoalchemy2 import {import_name}")
        return "%r" % obj

    # Default rendering for other objects
    return False


def include_object(obj, name, obj_type, reflected, compare_to):
    """Do not include internal tables of spatial extensions.

    .. warning::
        This function only checks the table names, so it might exclude tables that should not be.
        In such case, you should create your own function to handle your specific table names.

    """
    if obj_type == "table" and (
        name.startswith("geometry_columns")
        or name.startswith("spatial_ref_sys")
        or name.startswith("spatialite_history")
        or name.startswith("sqlite_sequence")
        or name.startswith("views_geometry_columns")
        or name.startswith("virts_geometry_columns")
        or name.startswith("idx_")
        or name.startswith("gpkg_")
        or name.startswith("vgpkg_")
    ):
        return False
    return True


@Operations.register_operation("add_geospatial_column")
@BatchOperations.register_operation("add_geospatial_column", "batch_add_geospatial_column")
class AddGeospatialColumnOp(ops.AddColumnOp):
    """Add a Geospatial Column in an Alembic migration context.

    This method originates from:
    https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
    """

    @classmethod
    def add_geospatial_column(cls, operations, table_name, column, schema=None):
        """Handle the different situations arising from adding geospatial column to a DB."""
        op = cls(table_name, column, schema=schema)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialColumnOp.from_column_and_tablename(
            self.schema, self.table_name, self.column.name
        )

    @classmethod
    def batch_add_geospatial_column(
        cls,
        operations,
        column,
        insert_before=None,
        insert_after=None,
    ):
        """Issue an "add column" instruction using the current batch migration context."""
        kw = {}
        if insert_before:
            kw["insert_before"] = insert_before
        if insert_after:
            kw["insert_after"] = insert_after

        op = cls(
            operations.impl.table_name,
            column,
            schema=operations.impl.schema,
            **kw,
        )
        return operations.invoke(op)


@Operations.register_operation("drop_geospatial_column")
@BatchOperations.register_operation("drop_geospatial_column", "batch_drop_geospatial_column")
class DropGeospatialColumnOp(ops.DropColumnOp):
    """Drop a Geospatial Column in an Alembic migration context."""

    @classmethod
    def drop_geospatial_column(cls, operations, table_name, column_name, schema=None, **kw):
        """Handle the different situations arising from dropping geospatial column from a DB."""
        op = cls(table_name, column_name, schema=schema, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return AddGeospatialColumnOp.from_column_and_tablename(
            self.schema, self.table_name, self.column
        )

    @classmethod
    def batch_drop_geospatial_column(cls, operations, column_name, **kw):
        """Issue a "drop column" instruction using the current batch migration context."""
        op = cls(
            operations.impl.table_name,
            column_name,
            schema=operations.impl.schema,
            **kw,
        )
        return operations.invoke(op)


@Operations.implementation_for(AddGeospatialColumnOp)
def add_geospatial_column(operations, operation):
    """Handle the actual column addition according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumnOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name
    column_name = operation.column.name

    dialect = operations.get_bind().dialect

    if dialect.name == "sqlite":
        if isinstance(operation.column, TypeDecorator):
            # Will be either geoalchemy2.types.Geography or geoalchemy2.types.Geometry, if using a
            # custom type
            geospatial_core_type = operation.column.type.load_dialect_impl(dialect)
        else:
            geospatial_core_type = operation.column.type
        operations.execute(
            func.AddGeometryColumn(
                table_name,
                column_name,
                geospatial_core_type.srid,
                geospatial_core_type.geometry_type,
                geospatial_core_type.dimension,
                not geospatial_core_type.nullable,
            )
        )
    elif "postgresql" in dialect.name:
        operations.impl.add_column(
            table_name,
            operation.column,
            schema=operation.schema,
        )


@Operations.implementation_for(DropGeospatialColumnOp)
def drop_geospatial_column(operations, operation):
    """Handle the actual column removal according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumnOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name
    column = operation.to_column(operations.migration_context)

    dialect = operations.get_bind().dialect

    if dialect.name == "sqlite":
        _SPATIAL_TABLES.add(table_name)
    operations.impl.drop_column(table_name, column, schema=operation.schema, **operation.kw)


@compiles(RenameTable, "sqlite")
def visit_rename_geospatial_table(element, compiler, **kw):
    """Specific compilation rule to rename spatial tables with SQLite dialect."""
    table_is_spatial = element.table_name in _SPATIAL_TABLES
    new_table_is_spatial = element.new_table_name in _SPATIAL_TABLES

    if table_is_spatial or new_table_is_spatial:
        # Here we suppose there is only one DB attached to the current engine, so the prefix
        # is set to NULL
        return "SELECT RenameTable(NULL, '%s', '%s')" % (
            format_table_name(compiler, element.table_name, element.schema),
            format_table_name(compiler, element.new_table_name, element.schema),
        )
    else:
        return visit_rename_table(element, compiler, **kw)


@compiles(DropTable, "sqlite")
def visit_drop_geospatial_table(element, compiler, **kw):
    """Specific compilation rule to drop spatial tables with SQLite dialect."""
    table_is_spatial = element.element.name in _SPATIAL_TABLES

    if table_is_spatial:
        # Here we suppose there is only one DB attached to the current engine
        return "SELECT DropTable(NULL, '%s')" % (
            format_table_name(compiler, element.element.name, None),
        )
    else:
        return compiler.visit_drop_table(element, **kw)


@renderers.dispatch_for(AddGeospatialColumnOp)
def render_add_geo_column(autogen_context, op):
    """Render the add_geospatial_column operation in migration script."""
    col_render = _add_column(autogen_context, op)
    return col_render.replace(".add_column(", ".add_geospatial_column(")


@renderers.dispatch_for(DropGeospatialColumnOp)
def render_drop_geo_column(autogen_context, op):
    """Render the drop_geospatial_column operation in migration script."""
    col_render = _drop_column(autogen_context, op)
    return col_render.replace(".drop_column(", ".drop_geospatial_column(")


@writer.rewrites(ops.AddColumnOp)
def add_geo_column(context, revision, op):
    """Replace the default AddColumnOp by a geospatial-specific one."""
    col_type = op.column.type
    if isinstance(col_type, TypeDecorator):
        dialect = context.bind.dialect
        col_type = col_type.load_dialect_impl(dialect)
    if isinstance(col_type, (Geometry, Geography, Raster)):
        op.column.type.spatial_index = False
        op.column.type._spatial_index_reflected = None
        new_op = AddGeospatialColumnOp(op.table_name, op.column, schema=op.schema)
    else:
        new_op = op
    return new_op


@writer.rewrites(ops.DropColumnOp)
def drop_geo_column(context, revision, op):
    """Replace the default DropColumnOp by a geospatial-specific one."""
    col_type = op.to_column().type
    if isinstance(col_type, TypeDecorator):
        dialect = context.bind.dialect
        col_type = col_type.load_dialect_impl(dialect)
    if isinstance(col_type, (Geometry, Geography, Raster)):
        new_op = DropGeospatialColumnOp(op.table_name, op.column_name, schema=op.schema)
    else:
        new_op = op
    return new_op


@Operations.register_operation("create_geospatial_table")
class CreateGeospatialTableOp(ops.CreateTableOp):
    """Create a Geospatial Table in an Alembic migration context.

    This method originates from:
    https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
    """

    @classmethod
    def create_geospatial_table(cls, operations, table_name, *columns, **kw):
        """Handle the different situations arising from creating geospatial table to a DB."""
        op = cls(table_name, columns, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialColumnOp.from_table(
            self.to_table(),
            _namespace_metadata=self._namespace_metadata,
        )

    @classmethod
    def from_table(cls, table, _namespace_metadata=None):
        obj = super().from_table(table, _namespace_metadata)
        return obj

    def to_table(self, migration_context=None):
        table = super().to_table(migration_context)

        # Set spatial_index attribute to False so the indexes are created explicitly
        for col in table.columns:
            try:
                if col.type.spatial_index:
                    col.type.spatial_index = False
            except AttributeError:
                pass
        return table


@Operations.register_operation("drop_geospatial_table")
class DropGeospatialTableOp(ops.DropTableOp):
    @classmethod
    def drop_geospatial_table(cls, operations, table_name, schema=None, **kw):
        """Handle the different situations arising from dropping geospatial table from a DB."""
        op = cls(table_name, schema=schema, table_kw=kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return CreateGeospatialTableOp.from_table(
            self.to_table(),
            _namespace_metadata=self._namespace_metadata,
        )

    @classmethod
    def from_table(cls, table, _namespace_metadata=None):
        obj = super().from_table(table, _namespace_metadata)
        return obj

    def to_table(self, migration_context=None):
        table = super().to_table(migration_context)
        return table


@Operations.implementation_for(CreateGeospatialTableOp)
def create_geospatial_table(operations, operation):
    """Handle the actual table creation according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: CreateGeospatialTableOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name
    bind = operations.get_bind()

    # For now the default events defined in geoalchemy2 are enough to handle table creation
    operations.create_table(table_name, *operation.columns, schema=operation.schema, **operation.kw)

    if bind.dialect.name == "sqlite":
        _SPATIAL_TABLES.add(table_name)


@Operations.implementation_for(DropGeospatialTableOp)
def drop_geospatial_table(operations, operation):
    """Handle the actual table removal according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: DropGeospatialTableOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name
    bind = operations.get_bind()
    dialect = bind.dialect

    if dialect.name == "sqlite":
        _SPATIAL_TABLES.add(table_name)
    operations.drop_table(table_name, schema=operation.schema, **operation.table_kw)


@renderers.dispatch_for(CreateGeospatialTableOp)
def render_create_geo_table(autogen_context, op):
    """Render the create_geospatial_table operation in migration script."""
    table_render = _add_table(autogen_context, op)
    return table_render.replace(".create_table(", ".create_geospatial_table(")


@renderers.dispatch_for(DropGeospatialTableOp)
def render_drop_geo_table(autogen_context, op):
    """Render the drop_geospatial_table operation in migration script."""
    table_render = _drop_table(autogen_context, op)
    return table_render.replace(".drop_table(", ".drop_geospatial_table(")


@writer.rewrites(ops.CreateTableOp)
def create_geo_table(context, revision, op):
    """Replace the default CreateTableOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    gis_cols = _get_gis_cols(op, (Geometry, Geography, Raster), dialect)

    if gis_cols:
        new_op = CreateGeospatialTableOp(
            op.table_name,
            op.columns,
            schema=op.schema,
            _namespace_metadata=op._namespace_metadata,
            _constraints_included=op._constraints_included,
            **op.kw,
        )
    else:
        new_op = op

    return new_op


@writer.rewrites(ops.DropTableOp)
def drop_geo_table(context, revision, op):
    """Replace the default DropTableOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    table = op.to_table()
    gis_cols = _get_gis_cols(table, (Geometry, Geography, Raster), dialect)

    if gis_cols:
        new_op = DropGeospatialTableOp(op.table_name, schema=op.schema)
    else:
        new_op = op

    return new_op


@Operations.register_operation("create_geospatial_index")
@BatchOperations.register_operation("create_geospatial_index", "batch_create_geospatial_index")
class CreateGeospatialIndexOp(ops.CreateIndexOp):
    @classmethod
    def create_geospatial_index(
        cls,
        operations,
        index_name,
        table_name,
        columns,
        schema=None,
        unique=False,
        **kw,
    ):
        """Handle the different situations arising from creating geospatial index into a DB."""
        op = cls(index_name, table_name, columns, schema=schema, unique=unique, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialIndexOp(
            self.index_name,
            self.table_name,
            column_name=self.columns[0].name,
            schema=self.schema,
        )

    @classmethod
    def batch_create_geospatial_index(
        cls,
        operations,
        index_name,
        columns,
        **kw,
    ):
        """Issue a "create index" instruction using the current batch migration context."""
        op = cls(
            index_name,
            operations.impl.table_name,
            columns,
            schema=operations.impl.schema,
            **kw,
        )
        return operations.invoke(op)


@Operations.register_operation("drop_geospatial_index")
@BatchOperations.register_operation("drop_geospatial_index", "batch_drop_geospatial_index")
class DropGeospatialIndexOp(ops.DropIndexOp):
    def __init__(self, *args, column_name, **kwargs):
        super().__init__(*args, **kwargs)
        self.column_name = column_name

    @classmethod
    def drop_geospatial_index(
        cls,
        operations,
        index_name,
        table_name,
        column_name,
        schema=None,
        unique=False,
        **kw,
    ):
        """Handle the different situations arising from dropping geospatial index from a DB."""
        op = cls(
            index_name,
            table_name=table_name,
            column_name=column_name,
            schema=schema,
            unique=unique,
            **kw,
        )
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return CreateGeospatialIndexOp(
            self.index_name,
            self.table_name,
            column_name=self.column_name,
            schema=self.schema,
            _reverse=self,
            **self.kw,
        )

    @classmethod
    def from_index(cls, index):
        assert index.table is not None
        assert len(index.columns) == 1, "A spatial index must be set on one column only"
        return cls(
            index.name,
            index.table.name,
            column_name=index.columns[0].name,
            schema=index.table.schema,
            _reverse=CreateGeospatialIndexOp.from_index(index),
            **index.kwargs,
        )

    @classmethod
    def batch_drop_geospatial_index(cls, operations, index_name, **kw):
        """Issue a "drop index" instruction using the current batch migration context."""
        op = cls(
            index_name,
            table_name=operations.impl.table_name,
            schema=operations.impl.schema,
            **kw,
        )
        return operations.invoke(op)


@Operations.implementation_for(CreateGeospatialIndexOp)
def create_geospatial_index(operations, operation):
    """Handle the actual index creation according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: CreateGeospatialIndexOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    bind = operations.get_bind()
    dialect = bind.dialect

    if dialect.name == "sqlite":
        assert len(operation.columns) == 1, "A spatial index must be set on one column only"
        operations.execute(func.CreateSpatialIndex(operation.table_name, operation.columns[0]))
    else:
        idx = operation.to_index(operations.migration_context)
        operations.impl.create_index(idx)


@Operations.implementation_for(DropGeospatialIndexOp)
def drop_geospatial_index(operations, operation):
    """Handle the actual index drop according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: DropGeospatialIndexOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    bind = operations.get_bind()
    dialect = bind.dialect

    if dialect.name == "sqlite":
        operations.execute(func.DisableSpatialIndex(operation.table_name, operation.column_name))
    else:
        operations.impl.drop_index(operation.to_index(operations.migration_context))


@renderers.dispatch_for(CreateGeospatialIndexOp)
def render_create_geo_index(autogen_context, op):
    """Render the create_geospatial_index operation in migration script."""
    idx_render = _add_index(autogen_context, op)
    return idx_render.replace(".create_index(", ".create_geospatial_index(")


@renderers.dispatch_for(DropGeospatialIndexOp)
def render_drop_geo_index(autogen_context, op):
    """Render the drop_geospatial_index operation in migration script."""
    idx_render = _drop_index(autogen_context, op)

    # Replace function name
    text = idx_render.replace(".drop_index(", ".drop_geospatial_index(")

    # Add column name as keyword argument
    text = text[:-1] + ", column_name='%s')" % (op.column_name,)

    return text


@writer.rewrites(ops.CreateIndexOp)
def create_geo_index(context, revision, op):
    """Replace the default CreateIndexOp by a geospatial-specific one."""
    dialect = context.bind.dialect

    if len(op.columns) == 1:
        col = op.columns[0]
        if isinstance(col, Column) and _check_spatial_type(
            col.type, (Geometry, Geography, Raster), dialect
        ):
            # Fix index properties
            op.kw["postgresql_using"] = op.kw.get("postgresql_using", "gist")
            if col.type.use_N_D_index:
                postgresql_ops = {col.name: "gist_geometry_ops_nd"}
            else:
                postgresql_ops = {}
            op.kw["postgresql_ops"] = op.kw.get("postgresql_ops", postgresql_ops)

            return CreateGeospatialIndexOp(
                op.index_name,
                op.table_name,
                op.columns,
                schema=op.schema,
                unique=op.unique,
                **op.kw,
            )

    return op


@writer.rewrites(ops.DropIndexOp)
def drop_geo_index(context, revision, op):
    """Replace the default DropIndexOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    idx = op.to_index()

    if len(idx.columns) == 1:
        col = idx.columns[0]
        if isinstance(col, Column) and _check_spatial_type(
            col.type, (Geometry, Geography, Raster), dialect
        ):
            return DropGeospatialIndexOp(
                op.index_name,
                table_name=op.table_name,
                column_name=col.name,
                schema=op.schema,
                **op.kw,
            )

    return op
