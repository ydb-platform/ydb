import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam

from .utils import version_table


class VersionExpressionReflector(sa.sql.visitors.ReplacingCloningVisitor):
    def __init__(self, parent, relationship):
        self.parent = parent
        self.relationship = relationship

    def replace(self, column):
        if not isinstance(column, sa.Column):
            return
        try:
            table = version_table(column.table)
        except KeyError:
            reflected_column = column
        else:
            reflected_column = table.c[column.name]
            if (
                column in self.relationship.local_columns
                and table == self.parent.__table__
            ):
                reflected_column = bindparam(
                    column.key, getattr(self.parent, column.key)
                )

        return reflected_column

    def __call__(self, expr):
        return self.traverse(expr)
