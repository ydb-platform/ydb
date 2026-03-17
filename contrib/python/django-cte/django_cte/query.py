import django
from django.core.exceptions import EmptyResultSet
from django.db.models.sql.constants import LOUTER

from .jitmixin import JITMixin, jit_mixin
from .join import QJoin

# NOTE: it is currently not possible to execute delete queries that
# reference CTEs without patching `QuerySet.delete` (Django method)
# to call `self.query.chain(sql.DeleteQuery)` instead of
# `sql.DeleteQuery(self.model)`


class CTEQuery(JITMixin):
    """A Query mixin that processes SQL compilation through a CTE compiler"""
    _jit_mixin_prefix = "CTE"
    _with_ctes = ()

    @property
    def combined_queries(self):
        return self.__dict__.get("combined_queries", ())

    @combined_queries.setter
    def combined_queries(self, queries):
        ctes = []
        seen = {cte.name: cte for cte in self._with_ctes}
        for query in queries:
            for cte in getattr(query, "_with_ctes", ()):
                if seen.get(cte.name) is cte:
                    continue
                if cte.name in seen:
                    raise ValueError(
                        f"Found two or more CTEs named '{cte.name}'. "
                        "Hint: assign a unique name to each CTE."
                    )
                ctes.append(cte)
                seen[cte.name] = cte

        if seen:
            def without_ctes(query):
                if getattr(query, "_with_ctes", None):
                    query = query.clone()
                    del query._with_ctes
                return query

            self._with_ctes += tuple(ctes)
            queries = tuple(without_ctes(q) for q in queries)
        self.__dict__["combined_queries"] = queries

    def resolve_expression(self, *args, **kwargs):
        clone = super().resolve_expression(*args, **kwargs)
        clone._with_ctes = tuple(
            cte.resolve_expression(*args, **kwargs)
            for cte in clone._with_ctes
        )
        return clone

    def get_compiler(self, *args, **kwargs):
        return jit_mixin(super().get_compiler(*args, **kwargs), CTECompiler)

    def chain(self, klass=None):
        clone = jit_mixin(super().chain(klass), CTEQuery)
        clone._with_ctes = self._with_ctes
        return clone


def generate_cte_sql(connection, query, as_sql):
    if not query._with_ctes:
        return as_sql()

    ctes = []
    params = []
    for cte in query._with_ctes:
        if django.VERSION > (4, 2):
            _ignore_with_col_aliases(cte.query)

        alias = query.alias_map.get(cte.name)
        should_elide_empty = (
                not isinstance(alias, QJoin) or alias.join_type != LOUTER
        )

        compiler = cte.query.get_compiler(
            connection=connection, elide_empty=should_elide_empty
        )

        qn = compiler.quote_name_unless_alias
        try:
            cte_sql, cte_params = compiler.as_sql()
        except EmptyResultSet:
            # If the CTE raises an EmptyResultSet the SqlCompiler still
            # needs to know the information about this base compiler
            # like, col_count and klass_info.
            as_sql()
            raise
        template = get_cte_query_template(cte)
        ctes.append(template.format(name=qn(cte.name), query=cte_sql))
        params.extend(cte_params)

    explain_attribute = "explain_info"
    explain_info = getattr(query, explain_attribute, None)
    explain_format = getattr(explain_info, "format", None)
    explain_options = getattr(explain_info, "options", {})

    explain_query_or_info = getattr(query, explain_attribute, None)
    sql = []
    if explain_query_or_info:
        sql.append(
            connection.ops.explain_query_prefix(
                explain_format,
                **explain_options
            )
        )
        # this needs to get set to None so that the base as_sql() doesn't
        # insert the EXPLAIN statement where it would end up between the
        # WITH ... clause and the final SELECT
        setattr(query, explain_attribute, None)

    if ctes:
        # Always use WITH RECURSIVE
        # https://www.postgresql.org/message-id/13122.1339829536%40sss.pgh.pa.us
        sql.extend(["WITH RECURSIVE", ", ".join(ctes)])
    base_sql, base_params = as_sql()

    if explain_query_or_info:
        setattr(query, explain_attribute, explain_query_or_info)

    sql.append(base_sql)
    params.extend(base_params)
    return " ".join(sql), tuple(params)


def get_cte_query_template(cte):
    if cte.materialized:
        return "{name} AS MATERIALIZED ({query})"
    return "{name} AS ({query})"


def _ignore_with_col_aliases(cte_query):
    if getattr(cte_query, "combined_queries", None):
        cte_query.combined_queries = tuple(
            jit_mixin(q, NoAliasQuery) for q in cte_query.combined_queries
        )


class CTECompiler(JITMixin):
    """Mixin for django.db.models.sql.compiler.SQLCompiler"""
    _jit_mixin_prefix = "CTE"

    def as_sql(self, *args, **kwargs):
        def _as_sql():
            return super(CTECompiler, self).as_sql(*args, **kwargs)
        return generate_cte_sql(self.connection, self.query, _as_sql)


class NoAliasQuery(JITMixin):
    """Mixin for django.db.models.sql.compiler.Query"""
    _jit_mixin_prefix = "NoAlias"

    def get_compiler(self, *args, **kwargs):
        return jit_mixin(super().get_compiler(*args, **kwargs), NoAliasCompiler)


class NoAliasCompiler(JITMixin):
    """Mixin for django.db.models.sql.compiler.SQLCompiler"""
    _jit_mixin_prefix = "NoAlias"

    def get_select(self, *, with_col_aliases=False, **kw):
        return super().get_select(**kw)
