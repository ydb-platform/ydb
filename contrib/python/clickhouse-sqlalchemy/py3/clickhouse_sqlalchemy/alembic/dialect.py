from sqlalchemy import func, Column, types as sqltypes

try:
    from alembic.ddl import impl
    from alembic.ddl.base import (
        compiles, ColumnComment, format_table_name, format_column_name
    )
except ImportError:
    raise RuntimeError('alembic must be installed')

from clickhouse_sqlalchemy import types, engines
from clickhouse_sqlalchemy.sql.ddl import DropTable
from .comparators import compare_mat_view
from .renderers import (
    render_attach_mat_view, render_detach_mat_view,
    render_create_mat_view, render_drop_mat_view
)
from .toimpl import (
    create_mat_view, attach_mat_view
)


class ClickHouseDialectImpl(impl.DefaultImpl):
    __dialect__ = 'clickhouse'
    transactional_ddl = False

    def drop_table(self, table):
        table.dispatch.before_drop(
            table, self.connection, checkfirst=False, _ddl_runner=self
        )

        self._exec(DropTable(table))

        table.dispatch.after_drop(
            table, self.connection, checkfirst=False, _ddl_runner=self
        )


def patch_alembic_version(context, **kwargs):
    migration_context = context._proxy._migration_context
    version = migration_context._version

    dt = Column('dt', types.DateTime, server_default=func.now())
    version_num = Column('version_num', types.String, primary_key=True)
    version.append_column(dt)
    version.append_column(version_num)

    if 'cluster' in kwargs:
        cluster = kwargs['cluster']
        version.engine = engines.ReplicatedReplacingMergeTree(
            kwargs['table_path'], kwargs['replica_name'],
            version=dt, order_by=func.tuple()
        )
        version.kwargs['clickhouse_cluster'] = cluster
    else:
        version.engine = engines.ReplacingMergeTree(
            version=dt, order_by=func.tuple()
        )


def include_object(object, name, type_, reflected, compare_to):
    # skip inner matview tables in autogeneration.
    if type_ == 'table' and object.info.get('mv_storage'):
        return False

    return True


@compiles(ColumnComment, 'clickhouse')
def visit_column_comment(element, compiler, **kw):
    ddl = "ALTER TABLE {table_name} COMMENT COLUMN {column_name} {comment}"
    comment = (
        compiler.sql_compiler.render_literal_value(
            element.comment or '', sqltypes.String()
        )
    )

    return ddl.format(
        table_name=format_table_name(
            compiler, element.table_name, element.schema
        ),
        column_name=format_column_name(compiler, element.column_name),
        comment=comment,
    )


__all__ = (
    'ClickHouseDialectImpl', 'compare_mat_view',
    'render_attach_mat_view', 'render_detach_mat_view',
    'render_create_mat_view', 'render_drop_mat_view',
    'create_mat_view', 'attach_mat_view'
)
