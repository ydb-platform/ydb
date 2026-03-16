from sqlalchemy.sql.ddl import (
    SchemaDropper as SchemaDropperBase, DropTable as DropTableBase,
    SchemaGenerator as SchemaGeneratorBase, _CreateDropBase
)
from sqlalchemy.sql.expression import UnaryExpression
from sqlalchemy.sql.operators import custom_op


class DropTable(DropTableBase):
    def __init__(self, element, if_exists=False):
        self.if_exists = if_exists
        self.on_cluster = element.dialect_options['clickhouse']['cluster']
        super(DropTable, self).__init__(element)


class SchemaDropper(SchemaDropperBase):
    def __init__(self, dialect, connection, if_exists=False, **kwargs):
        self.if_exists = if_exists
        super(SchemaDropper, self).__init__(dialect, connection, **kwargs)

    def visit_table(self, table, drop_ok=False, _is_metadata_operation=False):
        self.connection.execute(DropTable(table, if_exists=self.if_exists))


class CreateMaterializedView(_CreateDropBase):
    """Represent a CREATE MATERIALIZED VIEW statement."""

    __visit_name__ = "create_materialized_view"

    def __init__(self, element, if_not_exists=False):
        self.if_not_exists = if_not_exists
        super(CreateMaterializedView, self).__init__(element)


class SchemaGenerator(SchemaGeneratorBase):
    def __init__(self, dialect, connection, if_not_exists=False, **kwargs):
        self.if_not_exists = if_not_exists
        super(SchemaGenerator, self).__init__(dialect, connection, **kwargs)

    def visit_materialized_view(self, table, **kwargs):
        self.connection.execute(
            CreateMaterializedView(table, if_not_exists=self.if_not_exists)
        )


def ttl_delete(expr):
    return UnaryExpression(expr, modifier=custom_op('DELETE'))


def ttl_to_disk(expr, disk):
    assert isinstance(disk, str), 'Disk must be str'
    return expr.op('TO DISK')(disk)


def ttl_to_volume(expr, volume):
    assert isinstance(volume, str), 'Volume must be str'
    return expr.op('TO VOLUME')(volume)
