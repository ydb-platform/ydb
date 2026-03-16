import logging

from alembic import __version__ as alembic_version
from alembic.autogenerate import comparators
from alembic.autogenerate.compare import _compare_columns
from alembic.operations.ops import ModifyTableOps
from alembic.util.sqla_compat import _reflect_table as _alembic_reflect_table
from sqlalchemy import schema as sa_schema
from sqlalchemy import text

from clickhouse_sqlalchemy.sql.schema import Table
from . import operations

logger = logging.getLogger(__name__)

alembic_version = tuple(
    (int(x) if x.isdigit() else x) for x in alembic_version.split('.')
)


def _extract_to_table_name(create_table_query):
    query = create_table_query
    # Naive inner name detection
    brace = query.index('(')
    inner_name = query[query.index(' TO ', 0, brace) + 4:brace - 1].strip(' `')
    return inner_name.split('.')[1] if '.' in inner_name else inner_name


# Direct call .dispatch_for('schema', 'clickhouse') override an Alembic
# default ('schema', 'default') comparator. To avoid it (as we have own
# implementation only for a materialized views ) register default "schema"
# comparators as "clickhouse" comparators too.
for default_comparator in comparators._registry[('schema', 'default')]:
    comparators.dispatch_for('schema', 'clickhouse')(default_comparator)


def _reflect_table(inspector, table):
    if alembic_version >= (1, 11, 0):
        return _alembic_reflect_table(inspector, table)
    else:
        return _alembic_reflect_table(inspector, table, None)


@comparators.dispatch_for('schema', 'clickhouse')
def compare_mat_view(autogen_context, upgrade_ops, schemas):
    connection = autogen_context.connection
    dialect = autogen_context.dialect
    metadata = autogen_context.metadata

    database_engine = dialect._execute(
        connection,
        text(
            'SELECT engine '
            'FROM system.databases '
            'WHERE database = currentDatabase()'
        ), scalar=True
    )

    is_atomic = database_engine.lower() == 'atomic'
    logger.info('Database engine: %s', database_engine)
    if is_atomic:
        logger.info('Using "TO table" for materialized views storage')
    else:
        logger.info('Using ".inner" for materialized views storage by default')

    all_mat_views = set(dialect.get_view_names(connection))

    metadata_mat_views = metadata.info.setdefault('mat_views', set())

    statement_compiler = dialect.statement_compiler(dialect, None)
    ddl_compiler = dialect.ddl_compiler(dialect, None)

    for name in sorted(metadata_mat_views.difference(all_mat_views)):
        view = metadata.mat_views[name]

        selectable = statement_compiler.process(
            view.mv_selectable, literal_binds=True
        )

        logger.info('Detected added materialized view %s', name)

        if is_atomic or view.to:
            create = operations.CreateMatViewToTableOp(
                view.name, selectable, view.inner_table.name
            )

        else:
            inner_table = view.inner_table
            engine = ddl_compiler.process(inner_table.engine)

            create = operations.CreateMatViewOp(
                view.name, selectable, engine, *inner_table.columns
            )

        upgrade_ops.ops.append(create)

    existing_metadata = sa_schema.MetaData()
    inspector = autogen_context.inspector

    removed_mat_views = all_mat_views.difference(metadata_mat_views)
    existing_mat_views = all_mat_views.intersection(metadata_mat_views)

    mat_view_params_by_name = {}
    if removed_mat_views | existing_mat_views:
        rv = dialect._execute(
            connection,
            text(
                'SELECT name, as_select, engine_full, create_table_query '
                'FROM system.tables '
                'WHERE database = currentDatabase() AND name IN :names'
            ), names=list(removed_mat_views | existing_mat_views)
        )
        mat_view_params_by_name = {x.name: x for x in rv}

    for name in sorted(removed_mat_views):
        logger.info('Detected removed materialized view %s', name)
        params = mat_view_params_by_name[name]

        try:
            inner_name = _extract_to_table_name(params.create_table_query)
        except ValueError:
            inner_name = None

        if inner_name:
            drop = operations.DropMatViewToTableOp(
                name, params.as_select, inner_name
            )
        else:
            table = Table(name, existing_metadata)
            _reflect_table(inspector, table)

            drop = operations.DropMatViewOp(
                name, params.as_select, params.engine_full, *table.columns
            )

        upgrade_ops.ops.append(drop)

    for name in sorted(existing_mat_views):
        view = metadata.mat_views[name]
        params = mat_view_params_by_name[name]
        if is_atomic or view.to:
            inner_name = _extract_to_table_name(params.create_table_query)
        else:
            inner_name = '.inner.' + name

        conn_table = Table(inner_name, existing_metadata)
        _reflect_table(inspector, conn_table)

        if not autogen_context.run_object_filters(
            view, name, 'mat_view', False, conn_table
        ):
            return

        metadata_table = metadata.mat_views[name].inner_table
        modify_table_ops = ModifyTableOps(name, [])
        schema = None
        with _compare_columns(
                schema,
                inner_name,
                conn_table,
                metadata_table,
                modify_table_ops,
                autogen_context,
                inspector,
        ):
            comparators.dispatch('table')(
                autogen_context,
                modify_table_ops,
                schema,
                inner_name,
                conn_table,
                metadata_table,
            )

        if not modify_table_ops.is_empty():
            selectable = statement_compiler.process(
                view.mv_selectable, literal_binds=True
            )
            engine = ddl_compiler.process(metadata_table.engine)

            if is_atomic or view.to:
                ops = [
                    operations.DropMatViewToTableOp(
                        name, params.as_select, metadata_table.name
                    ),
                    modify_table_ops,
                    operations.CreateMatViewToTableOp(
                        view.name, selectable, metadata_table.name
                    )
                ]
            else:
                ops = [
                    operations.DetachMatViewOp(
                        name, params.as_select, engine,
                        *metadata_table.columns
                    ),
                    modify_table_ops,
                    operations.AttachMatViewOp(
                        name, selectable, engine, *metadata_table.columns
                    )
                ]

            upgrade_ops.ops.extend(ops)
