from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import ColumnCollectionMixin, SchemaItem, Constraint
from sqlalchemy.util import zip_longest


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

    def _set_parent(self, parent, **kw):
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
        columns = []
        self.expressions = []
        for expr, column, strname, add_element in self.\
                _extract_col_expression_collection(expressions):
            if add_element is not None:
                columns.append(add_element)
            self.expressions.append(expr)

        super(KeysExpressionOrColumn, self).__init__(*columns, **kwargs)

    def _set_parent(self, table):
        super(KeysExpressionOrColumn, self)._set_parent(table)

    def get_expressions_or_columns(self):
        expr_columns = zip_longest(self.expressions, self.columns)
        return [
            (expr if isinstance(expr, ClauseElement) else colexpr)
            for expr, colexpr in expr_columns
        ]
