from collections import namedtuple

from django.db import router
from django.db.models.sql import query, subqueries
from django.db.models.sql.constants import INNER
from django.db.models.sql.datastructures import BaseTable, Join
from django.db.models.sql.where import AND

from clickhouse_backend import compat

ExplainInfo = namedtuple("ExplainInfo", ("format", "type", "options"))


class Query(query.Query):
    def __init__(self, model, where=query.WhereNode, alias_cols=True):
        if compat.dj_ge4:
            super().__init__(model, alias_cols)
        else:
            super().__init__(model, where, alias_cols)
        self.setting_info = {}
        self.prewhere = query.WhereNode()

    def sql_with_params(self):
        """Choose the right db when database router is used."""
        return self.get_compiler(router.db_for_read(self.model)).as_sql()

    def clone(self):
        obj = super().clone()
        obj.setting_info = self.setting_info.copy()
        obj.prewhere = self.prewhere.clone()
        return obj

    def explain(self, using, format=None, type=None, **settings):
        q = self.clone()
        q.explain_info = ExplainInfo(format, type, settings)
        compiler = q.get_compiler(using=using)
        return "\n".join(compiler.explain_query())

    def add_prewhere(self, q_object):
        """
        refer add_q
        """
        existing_inner = {
            a for a in self.alias_map if self.alias_map[a].join_type == INNER
        }
        clause, _ = self._add_q(q_object, self.used_aliases)
        if clause:
            self.prewhere.add(clause, AND)
        self.demote_joins(existing_inner)

    if not compat.dj_ge42:

        @property
        def is_sliced(self):
            return self.low_mark != 0 or self.high_mark is not None

    def resolve_expression(self, query, *args, **kwargs):
        clone = self.clone()
        # Subqueries need to use a different set of aliases than the outer query.
        clone.bump_prefix(query)
        clone.subquery = True
        clone.where.resolve_expression(query, *args, **kwargs)
        clone.prewhere.resolve_expression(query, *args, **kwargs)
        # Resolve combined queries.
        if clone.combinator:
            clone.combined_queries = tuple(
                [
                    combined_query.resolve_expression(query, *args, **kwargs)
                    for combined_query in clone.combined_queries
                ]
            )
        for key, value in clone.annotations.items():
            resolved = value.resolve_expression(query, *args, **kwargs)
            if hasattr(resolved, "external_aliases"):
                resolved.external_aliases.update(clone.external_aliases)
            clone.annotations[key] = resolved
        # Outer query's aliases are considered external.
        for alias, table in query.alias_map.items():
            clone.external_aliases[alias] = (
                isinstance(table, Join)
                and table.join_field.related_model._meta.db_table != alias
            ) or (
                isinstance(table, BaseTable) and table.table_name != table.table_alias
            )
        return clone

    def change_aliases(self, change_map):
        """
        Change the aliases in change_map (which maps old-alias -> new-alias),
        relabelling any references to them in select columns and the where
        clause.
        """
        # If keys and values of change_map were to intersect, an alias might be
        # updated twice (e.g. T4 -> T5, T5 -> T6, so also T4 -> T6) depending
        # on their order in change_map.
        assert set(change_map).isdisjoint(change_map.values())

        # 1. Update references in "select" (normal columns plus aliases),
        # "group by" and "where".
        self.where.relabel_aliases(change_map)
        self.prewhere.relabel_aliases(change_map)
        if isinstance(self.group_by, tuple):
            self.group_by = tuple(
                [col.relabeled_clone(change_map) for col in self.group_by]
            )
        self.select = tuple([col.relabeled_clone(change_map) for col in self.select])
        self.annotations = self.annotations and {
            key: col.relabeled_clone(change_map)
            for key, col in self.annotations.items()
        }

        # 2. Rename the alias in the internal table/alias datastructures.
        for old_alias, new_alias in change_map.items():
            if old_alias not in self.alias_map:
                continue
            alias_data = self.alias_map[old_alias].relabeled_clone(change_map)
            self.alias_map[new_alias] = alias_data
            self.alias_refcount[new_alias] = self.alias_refcount[old_alias]
            del self.alias_refcount[old_alias]
            del self.alias_map[old_alias]

            table_aliases = self.table_map[alias_data.table_name]
            for pos, alias in enumerate(table_aliases):
                if alias == old_alias:
                    table_aliases[pos] = new_alias
                    break
        self.external_aliases = {
            # Table is aliased or it's being changed and thus is aliased.
            change_map.get(alias, alias): (aliased or alias in change_map)
            for alias, aliased in self.external_aliases.items()
        }


def clone_decorator(cls):
    old_clone = cls.clone

    def clone(self):
        obj = old_clone(self)
        if hasattr(obj, "setting_info"):
            obj.setting_info = self.setting_info.copy()
        return obj

    cls.clone = clone
    return cls


clone_decorator(subqueries.UpdateQuery)
clone_decorator(subqueries.DeleteQuery)
