from alembic.operations import Operations
from sqlalchemy.sql.ddl import CreateColumn

from . import operations


@Operations.implementation_for(operations.CreateMatViewOp)
def create_mat_view(operations, operation):
    impl = operations.impl
    ddl_compiler = impl.dialect.ddl_compiler(impl.dialect, None)

    text = 'CREATE MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' (' + ', '.join(
        ddl_compiler.process(CreateColumn(c)) for c in operation.columns
    ) + ') '

    text += 'ENGINE = ' + operation.engine

    if operation.kwargs.get('populate'):
        text += ' POPULATE'

    text += ' AS ' + operation.selectable

    operations.execute(text)


@Operations.implementation_for(operations.CreateMatViewToTableOp)
def create_mat_view_to_table(operations, operation):
    text = 'CREATE MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' TO ' + operation.inner_name

    if operation.kwargs.get('populate'):
        text += ' POPULATE'

    text += ' AS ' + operation.selectable

    operations.execute(text)


@Operations.implementation_for(operations.AttachMatViewOp)
def attach_mat_view(operations, operation):
    impl = operations.impl
    ddl_compiler = impl.dialect.ddl_compiler(impl.dialect, None)

    text = 'ATTACH MATERIALIZED VIEW '

    if operation.kwargs.get('if_not_exists'):
        text += 'IF NOT EXISTS '

    text += operation.name + ' '

    if operation.kwargs.get('on_cluster'):
        text += ' ON CLUSTER ' + operation.kwargs['on_cluster']

    text += ' (' + ', '.join(
        ddl_compiler.process(CreateColumn(c)) for c in operation.columns
    ) + ') '

    text += 'ENGINE = ' + operation.engine + ' AS ' + operation.selectable

    operations.execute(text)
