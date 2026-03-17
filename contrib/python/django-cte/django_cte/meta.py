import weakref

from django.db.models.expressions import Col, Expression


class CTEColumns:

    def __init__(self, cte):
        self._cte = weakref.ref(cte)

    def __getattr__(self, name):
        return CTEColumn(self._cte(), name)


class CTEColumn(Expression):

    def __init__(self, cte, name, output_field=None):
        self._cte = cte
        self.table_alias = cte.name
        self.name = self.alias = name
        self._output_field = output_field

    def __repr__(self):
        return "<{} {}.{}>".format(
            self.__class__.__name__,
            self._cte.name,
            self.name,
        )

    @property
    def _ref(self):
        if self._cte.query is None:
            raise ValueError(
                "cannot resolve '{cte}.{name}' in recursive CTE setup. "
                "Hint: use ExpressionWrapper({cte}.col.{name}, "
                "output_field=...)".format(cte=self._cte.name, name=self.name)
            )
        ref = self._cte._resolve_ref(self.name)
        if ref is self or self in ref.get_source_expressions():
            raise ValueError("Circular reference: {} = {}".format(self, ref))
        return ref

    @property
    def target(self):
        return self._ref.target

    @property
    def output_field(self):
        # required to fix error caused by django commit
        #     9d519d3dc4e5bd1d9ff3806b44624c3e487d61c1
        if self._cte.query is None:
            raise AttributeError

        if self._output_field is not None:
            return self._output_field
        return self._ref.output_field

    def as_sql(self, compiler, connection):
        qn = compiler.quote_name_unless_alias
        ref = self._ref
        if isinstance(ref, Col) and self.name == "pk":
            column = ref.target.column
        else:
            column = self.name
        return "%s.%s" % (qn(self.table_alias), qn(column)), []

    def relabeled_clone(self, relabels):
        if self.table_alias is not None and self.table_alias in relabels:
            clone = self.copy()
            clone.table_alias = relabels[self.table_alias]
            return clone
        return self


class CTEColumnRef(Expression):

    def __init__(self, name, cte_name, output_field):
        self.name = name
        self.cte_name = cte_name
        self.output_field = output_field
        self._alias = None

    def resolve_expression(self, query=None, allow_joins=True, reuse=None,
                           summarize=False, for_save=False):
        if query:
            clone = self.copy()
            clone._alias = self._alias or query.table_map.get(
                self.cte_name, [self.cte_name])[0]
            return clone
        return super().resolve_expression(
            query, allow_joins, reuse, summarize, for_save)

    def relabeled_clone(self, change_map):
        if (
            self.cte_name not in change_map
            and self._alias not in change_map
        ):
            return super().relabeled_clone(change_map)

        clone = self.copy()
        if self.cte_name in change_map:
            clone._alias = change_map[self.cte_name]

        if self._alias in change_map:
            clone._alias = change_map[self._alias]
        return clone

    def as_sql(self, compiler, connection):
        qn = compiler.quote_name_unless_alias
        table = self._alias or compiler.query.table_map.get(
            self.cte_name, [self.cte_name])[0]
        return "%s.%s" % (qn(table), qn(self.name)), []
