from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem, Constraint


class Engine(Constraint):
    __visit_name__ = 'engine'

    def __init__(self, *args, **kwargs):
        pass

    def get_parameters(self):
        return []

    def extend_parameters(self, *params):
        rv = []
        for param in params:
            if isinstance(param, (tuple, list)):
                rv.extend(param)
            elif param is not None:
                rv.append(param)
        return rv

    @property
    def name(self):
        return self.__class__.__name__

    def _set_parent(self, parent, **kwargs):
        self.parent = parent
        parent.engine = self

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        raise NotImplementedError


class TableCol(ColumnCollectionMixin, SchemaItem):
    def __init__(self, column, **kwargs):
        super(TableCol, self).__init__(*[column], **kwargs)

    def get_column(self):
        return list(self.columns)[0]


class KeysExpressionOrColumn(ColumnCollectionMixin, SchemaItem):
    def __init__(self, *expressions, **kwargs):
        self.expressions = []

        super(KeysExpressionOrColumn, self).__init__(
            *expressions, _gather_expressions=self.expressions, **kwargs
        )

    def _set_parent(self, table, **kw):
        ColumnCollectionMixin._set_parent(self, table)

        self.table = table

        expressions = self.expressions
        col_expressions = self._col_expressions(table)
        assert len(expressions) == len(col_expressions)
        self.expressions = [
            expr if isinstance(expr, ClauseElement) else colexpr
            for expr, colexpr in zip(expressions, col_expressions)
        ]

    def get_expressions_or_columns(self):
        return self.expressions
