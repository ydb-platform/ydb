from copy import copy

import django
from django.db.models import Manager, sql
from django.db.models.expressions import Ref
from django.db.models.query import Q, QuerySet, ValuesIterable
from django.db.models.sql.datastructures import BaseTable

from .jitmixin import jit_mixin
from .join import QJoin, INNER
from .meta import CTEColumnRef, CTEColumns
from .query import CTEQuery
from ._deprecated import deprecated

__all__ = ["CTE", "with_cte"]


def with_cte(*ctes, select):
    """Add Common Table Expression(s) (CTEs) to a model or queryset

    :param *ctes: One or more CTE objects.
    :param select: A model class, queryset, or CTE to use as the base
        query to which CTEs are attached.
    :returns: A queryset with the given CTE added to it.
    """
    if isinstance(select, CTE):
        select = select.queryset()
    elif not isinstance(select, QuerySet):
        select = select._default_manager.all()
    jit_mixin(select.query, CTEQuery)
    select.query._with_ctes += ctes
    return select


class CTE:
    """Common Table Expression

    :param queryset: A queryset to use as the body of the CTE.
    :param name: Optional name parameter for the CTE (default: "cte").
    This must be a unique name that does not conflict with other
    entities (tables, views, functions, other CTE(s), etc.) referenced
    in the given query as well any query to which this CTE will
    eventually be added.
    :param materialized: Optional parameter (default: False) which enforce
    using of MATERIALIZED statement for supporting databases.
    """

    def __init__(self, queryset, name="cte", materialized=False):
        self._set_queryset(queryset)
        self.name = name
        self.col = CTEColumns(self)
        self.materialized = materialized

    def __getstate__(self):
        return (self.query, self.name, self.materialized, self._iterable_class)

    def __setstate__(self, state):
        if len(state) == 3:
            # Keep compatibility with the previous serialization method
            self.query, self.name, self.materialized = state
            self._iterable_class = ValuesIterable
        else:
            self.query, self.name, self.materialized, self._iterable_class = state
        self.col = CTEColumns(self)

    def __repr__(self):
        return f"<{type(self).__name__} {self.name}>"

    def _set_queryset(self, queryset):
        self.query = None if queryset is None else queryset.query
        self._iterable_class = getattr(queryset, "_iterable_class", ValuesIterable)

    @classmethod
    def recursive(cls, make_cte_queryset, name="cte", materialized=False):
        """Recursive Common Table Expression

        :param make_cte_queryset: Function taking a single argument (a
        not-yet-fully-constructed cte object) and returning a `QuerySet`
        object. The returned `QuerySet` normally consists of an initial
        statement unioned with a recursive statement.
        :param name: See `name` parameter of `__init__`.
        :param materialized: See `materialized` parameter of `__init__`.
        :returns: The fully constructed recursive cte object.
        """
        cte = cls(None, name, materialized)
        cte._set_queryset(make_cte_queryset(cte))
        return cte

    def join(self, model_or_queryset, *filter_q, **filter_kw):
        """Join this CTE to the given model or queryset

        This CTE will be referenced by the returned queryset, but the
        corresponding `WITH ...` statement will not be prepended to the
        queryset's SQL output; use `with_cte(cte, select=cte.join(...))`
        to achieve that outcome.

        :param model_or_queryset: Model class or queryset to which the
        CTE should be joined.
        :param *filter_q: Join condition Q expressions (optional).
        :param **filter_kw: Join conditions. All LHS fields (kwarg keys)
        are assumed to reference `model_or_queryset` fields. Use
        `cte.col.name` on the RHS to recursively reference CTE query
        columns. For example: `cte.join(Book, id=cte.col.id)`
        :returns: A queryset with the given model or queryset joined to
        this CTE.
        """
        if isinstance(model_or_queryset, QuerySet):
            queryset = model_or_queryset.all()
        else:
            queryset = model_or_queryset._default_manager.all()
        join_type = filter_kw.pop("_join_type", INNER)
        query = queryset.query

        # based on Query.add_q: add necessary joins to query, but no filter
        q_object = Q(*filter_q, **filter_kw)
        map = query.alias_map
        existing_inner = set(a for a in map if map[a].join_type == INNER)
        if django.VERSION >= (5, 2):
            on_clause, _ = query._add_q(
                q_object, query.used_aliases, update_join_types=(join_type == INNER)
            )
        else:
            on_clause, _ = query._add_q(q_object, query.used_aliases)
        query.demote_joins(existing_inner)

        parent = query.get_initial_alias()
        query.join(QJoin(parent, self.name, self.name, on_clause, join_type))
        return queryset

    def queryset(self):
        """Get a queryset selecting from this CTE

        This CTE will be referenced by the returned queryset, but the
        corresponding `WITH ...` statement will not be prepended to the
        queryset's SQL output; use `with_cte(cte, select=cte)` to do
        that.

        :returns: A queryset.
        """
        cte_query = self.query
        qs = cte_query.model._default_manager.get_queryset()
        qs._iterable_class = self._iterable_class
        qs._fields = ()  # Allow any field names to be used in further annotations

        query = jit_mixin(sql.Query(cte_query.model), CTEQuery)
        query.join(BaseTable(self.name, None))
        query.default_cols = cte_query.default_cols
        query.deferred_loading = cte_query.deferred_loading

        if django.VERSION < (5, 2) and cte_query.values_select:
            query.set_values(cte_query.values_select)

        if cte_query.annotations:
            for alias, value in cte_query.annotations.items():
                col = CTEColumnRef(alias, self.name, value.output_field)
                query.add_annotation(col, alias)
        query.annotation_select_mask = cte_query.annotation_select_mask

        if selected := getattr(cte_query, "selected", None):
            for alias in selected:
                if alias not in cte_query.annotations:
                    output_field = cte_query.resolve_ref(alias).output_field
                    col = CTEColumnRef(alias, self.name, output_field)
                    query.add_annotation(col, alias)
            query.selected = {alias: alias for alias in selected}

        qs.query = query
        return qs

    def _resolve_ref(self, name):
        selected = getattr(self.query, "selected", None)
        if selected and name in selected and name not in self.query.annotations:
            return Ref(name, self.query.resolve_ref(name))
        return self.query.resolve_ref(name)

    def resolve_expression(self, *args, **kw):
        if self.query is None:
            raise ValueError("Cannot resolve recursive CTE without a query.")
        clone = copy(self)
        clone.query = clone.query.resolve_expression(*args, **kw)
        return clone


@deprecated("Use `django_cte.CTE` instead.")
class With(CTE):

    @staticmethod
    @deprecated("Use `django_cte.CTE.recursive` instead.")
    def recursive(*args, **kw):
        return CTE.recursive(*args, **kw)


@deprecated("CTEQuerySet is deprecated. "
            "CTEs can now be applied to any queryset using `with_cte()`")
class CTEQuerySet(QuerySet):
    """QuerySet with support for Common Table Expressions"""

    def __init__(self, model=None, query=None, using=None, hints=None):
        # Only create an instance of a Query if this is the first invocation in
        # a query chain.
        super(CTEQuerySet, self).__init__(model, query, using, hints)
        jit_mixin(self.query, CTEQuery)

    @deprecated("Use `django_cte.with_cte(cte, select=...)` instead.")
    def with_cte(self, cte):
        qs = self._clone()
        qs.query._with_ctes += cte,
        return qs

    def as_manager(cls):
        # Address the circular dependency between
        # `CTEQuerySet` and `CTEManager`.
        manager = CTEManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)


@deprecated("CTEMAnager is deprecated. "
            "CTEs can now be applied to any queryset using `with_cte()`")
class CTEManager(Manager.from_queryset(CTEQuerySet)):
    """Manager for models that perform CTE queries"""

    @classmethod
    def from_queryset(cls, queryset_class, class_name=None):
        if not issubclass(queryset_class, CTEQuerySet):
            raise TypeError(
                "models with CTE support need to use a CTEQuerySet")
        return super(CTEManager, cls).from_queryset(
            queryset_class, class_name=class_name)
