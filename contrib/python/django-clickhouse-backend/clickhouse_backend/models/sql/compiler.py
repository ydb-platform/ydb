import itertools

from django.core.exceptions import EmptyResultSet
from django.db import NotSupportedError
from django.db.models.fields import AutoFieldMixin
from django.db.models.sql import compiler

from clickhouse_backend import compat
from clickhouse_backend.idworker import id_worker
from clickhouse_backend.models import engines
from clickhouse_backend.models.sql import Query

if compat.dj_ge42:
    from django.core.exceptions import FullResultSet
else:

    class FullResultSet(Exception):
        """A database query predicate is matches everything."""

        pass


# Max rows you can insert using expression as value.
MAX_ROWS_INSERT_USE_EXPRESSION = 1000


class ClickhouseMixin:
    def _add_explain_sql(self, sql, params):
        # Backward compatible for django 3.2
        explain_info = getattr(self.query, "explain_info", None)
        if explain_info:
            prefix, suffix = self.connection.ops.explain_query(
                format=explain_info.format,
                type=explain_info.type,
                **explain_info.options,
            )
            sql = "%s %s" % (prefix, sql.lstrip())
            if suffix:
                sql = "%s %s" % (sql, suffix)
        return sql, params

    def _add_settings_sql(self, sql, params):
        if getattr(self.query, "setting_info", None):
            setting_sql, setting_params = self.connection.ops.settings_sql(
                **self.query.setting_info
            )
            sql = "%s %s" % (sql, setting_sql)
            params = (*params, *setting_params)
        return sql, params

    def _compile_where(self, table):
        try:
            where, params = self.compile(self.query.where)
        except FullResultSet:
            where, params = "", ()
        if where:
            where = where.replace(table + ".", "")
        else:
            where = "TRUE"
        return where, params


class SQLCompiler(ClickhouseMixin, compiler.SQLCompiler):
    def pre_sql_setup(self, with_col_aliases=False):
        """
        Do any necessary class setup immediately prior to producing SQL. This
        is for things that can't necessarily be done in __init__ because we
        might not have all the pieces in place at that time.
        """
        if compat.dj_ge42:
            self.setup_query(with_col_aliases=with_col_aliases)
            (
                self.where,
                self.having,
                self.qualify,
            ) = self.query.where.split_having_qualify(
                must_group_by=self.query.group_by is not None
            )
            if isinstance(self.query, Query):
                (
                    self.prewhere,
                    prehaving,
                    prequalify,
                ) = self.query.prewhere.split_having_qualify(
                    must_group_by=self.query.group_by is not None
                )
            else:
                self.prewhere, prehaving, prequalify = None, None, None
            # Check before ClickHouse complain.
            # DB::Exception: Window function is found in PREWHERE in query. (ILLEGAL_AGGREGATION)
            if prequalify:
                raise NotSupportedError(
                    "Window function is disallowed in the prewhere clause."
                )
        else:
            self.setup_query()
            self.where, self.having = self.query.where.split_having()
            if isinstance(self.query, Query):
                self.prewhere, prehaving = self.query.prewhere.split_having()
            else:
                self.prewhere, prehaving = None, None
        # Check before ClickHouse complain.
        # DB::Exception: Aggregate function is found in PREWHERE in query. (ILLEGAL_AGGREGATION)
        if prehaving:
            raise NotSupportedError(
                "Aggregate function is disallowed in the prewhere clause."
            )
        order_by = self.get_order_by()
        extra_select = self.get_extra_select(order_by, self.select)
        self.has_extra_select = bool(extra_select)
        group_by = self.get_group_by(self.select + extra_select, order_by)
        return extra_select, order_by, group_by

    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Create the SQL for this query. Return the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        refcounts_before = self.query.alias_refcount.copy()
        try:
            combinator = self.query.combinator
            if compat.dj_ge42:
                extra_select, order_by, group_by = self.pre_sql_setup(
                    with_col_aliases=with_col_aliases or bool(combinator),
                )
            else:
                extra_select, order_by, group_by = self.pre_sql_setup()
            # Is a LIMIT/OFFSET clause needed?
            with_limit_offset = with_limits and self.query.is_sliced
            if combinator:
                result, params = self.get_combinator_sql(
                    combinator, self.query.combinator_all
                )
            # Django >= 4.2 have this branch
            elif compat.dj_ge42 and self.qualify:
                result, params = self.get_qualify_sql()
                order_by = None
            else:
                distinct_fields, distinct_params = self.get_distinct()
                # This must come after 'select', 'ordering', and 'distinct'
                # (see docstring of get_from_clause() for details).
                from_, f_params = self.get_from_clause()
                try:
                    where, w_params = (
                        self.compile(self.where) if self.where is not None else ("", [])
                    )
                except EmptyResultSet:
                    if compat.dj3 or self.elide_empty:
                        raise
                    # Use a predicate that's always False.
                    where, w_params = "FALSE", []
                except FullResultSet:
                    where, w_params = "", []
                try:
                    having, h_params = (
                        self.compile(self.having)
                        if self.having is not None
                        else ("", [])
                    )
                except FullResultSet:
                    having, h_params = "", []
                # v1.2.0 new feature, support prewhere clause.
                # refer https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere
                try:
                    prewhere, p_params = (
                        self.compile(self.prewhere)
                        if self.prewhere is not None
                        else ("", [])
                    )
                except EmptyResultSet:
                    if compat.dj_ge42 and self.elide_empty:
                        raise
                    # Use a predicate that's always False.
                    prewhere, p_params = "FALSE", []
                except FullResultSet:
                    prewhere, p_params = "", []
                result = ["SELECT"]
                params = []

                if self.query.distinct:
                    distinct_result, distinct_params = self.connection.ops.distinct_sql(
                        distinct_fields,
                        distinct_params,
                    )
                    result += distinct_result
                    params += distinct_params

                out_cols = []
                for _, (s_sql, s_params), alias in self.select + extra_select:
                    if alias:
                        s_sql = "%s AS %s" % (
                            s_sql,
                            self.connection.ops.quote_name(alias),
                        )
                    params.extend(s_params)
                    out_cols.append(s_sql)

                result += [", ".join(out_cols)]
                if from_:
                    result += ["FROM", *from_]
                params.extend(f_params)

                if prewhere:
                    result.append("PREWHERE %s" % prewhere)
                    params.extend(p_params)

                if where:
                    result.append("WHERE %s" % where)
                    params.extend(w_params)

                grouping = []
                for g_sql, g_params in group_by:
                    grouping.append(g_sql)
                    params.extend(g_params)
                if grouping:
                    if distinct_fields:
                        raise NotImplementedError(
                            "annotate() + distinct(fields) is not implemented."
                        )
                    result.append("GROUP BY %s" % ", ".join(grouping))
                    if self._meta_ordering:
                        order_by = None
                if having:
                    result.append("HAVING %s" % having)
                    params.extend(h_params)

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                order_by_sql = "ORDER BY %s" % ", ".join(ordering)
                result.append(order_by_sql)

            if with_limit_offset:
                result.append(
                    self.connection.ops.limit_offset_sql(
                        self.query.low_mark, self.query.high_mark
                    )
                )

            if self.query.subquery and extra_select:
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering expressions
                # to be selected as well. Wrap the query in another subquery
                # to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if alias:
                        sub_selects.append(
                            "%s.%s"
                            % (
                                self.connection.ops.quote_name("subquery"),
                                self.connection.ops.quote_name(alias),
                            )
                        )
                    else:
                        select_clone = select.relabeled_clone(
                            {select.alias: "subquery"}
                        )
                        subselect, subparams = select_clone.as_sql(
                            self, self.connection
                        )
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                sql = "SELECT %s FROM (%s) subquery" % (
                    ", ".join(sub_selects),
                    " ".join(result),
                )
                params = tuple(sub_params + params)
            else:
                sql = " ".join(result)
                params = tuple(params)
            sql, params = self._add_settings_sql(sql, params)
            sql, params = self._add_explain_sql(sql, params)
            return sql, params
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)


class SQLInsertCompiler(compiler.SQLInsertCompiler):
    def as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement()

        fields = self.query.fields
        # Generate value for AutoField when needed.
        absent_of_pk = isinstance(opts.pk, AutoFieldMixin) and opts.pk not in fields
        if absent_of_pk:
            fields = fields + [opts.pk]
            for obj in self.query.objs:
                setattr(obj, opts.pk.attname, id_worker.get_id())

        # Check db_default
        if any(compat.field_has_db_default(field) for field in fields):
            from django.db.models.expressions import DatabaseDefault

            other_fields = []
            db_default_fields = []
            for field in fields:
                if compat.field_has_db_default(field):
                    db_default_fields.append(field)
                else:
                    other_fields.append(field)
            fields = other_fields

            # 1. Too many rows should not use mix of const values and db default values, so only check first object.
            if len(self.query.objs) > MAX_ROWS_INSERT_USE_EXPRESSION:
                first_obj = self.query.objs[0]
                for field in db_default_fields:
                    # Field with db_default can be omitted.
                    if isinstance(
                        self.prepare_value(field, self.pre_save_val(field, first_obj)),
                        DatabaseDefault,
                    ):
                        continue
                    fields.append(field)
            else:
                for field in db_default_fields:
                    # Field with db_default can be omitted.
                    if all(
                        isinstance(
                            self.prepare_value(field, self.pre_save_val(field, obj)),
                            DatabaseDefault,
                        )
                        for obj in self.query.objs
                    ):
                        continue
                    fields.append(field)
                # Fields should not be empty.
                if not fields:
                    fields.append(db_default_fields[0])

        result = [
            "%s %s(%s)"
            % (
                insert_statement,
                qn(opts.db_table),
                ", ".join(qn(f.column) for f in fields),
            )
        ]

        value_rows = [
            [
                self.prepare_value(field, self.pre_save_val(field, obj))
                for field in fields
            ]
            for obj in self.query.objs
        ]

        # https://clickhouse.com/docs/en/sql-reference/statements/insert-into
        # If you want to specify SETTINGS for INSERT query then you have to do it before FORMAT clause
        # since everything after FORMAT format_name is treated as data.
        if getattr(self.query, "setting_info", None):
            setting_sql, setting_params = self.connection.ops.settings_sql(
                **self.query.setting_info
            )
            qv = self.connection.schema_editor().quote_value
            result.append((setting_sql % map(qv, setting_params)) % ())

        # If value rows count exceed limitation, raw data is asserted.
        # Refer https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
        if len(value_rows) > MAX_ROWS_INSERT_USE_EXPRESSION:
            result.append("VALUES")
            params = value_rows
        else:
            placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)
            if any(i != "%s" for i in itertools.chain.from_iterable(placeholder_rows)):
                placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
                values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
                result.append("VALUES " + values_sql)
                params = tuple(itertools.chain.from_iterable(param_rows))
            else:
                result.append("VALUES")
                params = param_rows
        return [(" ".join(result), params)]

    def execute_sql(self, returning_fields=None):
        as_sql = self.as_sql()
        with self.connection.cursor() as cursor:
            for sql, params in as_sql:
                cursor.execute(sql, params)
        return []


class SQLDeleteCompiler(ClickhouseMixin, compiler.SQLDeleteCompiler):
    def _as_sql(self, query):
        """
        When execute DELETE and UPDATE query. Clickhouse does not support
        "table"."column" in WHERE clause.
        """
        table = self.quote_name_unless_alias(query.base_table)
        engine = getattr(query.model._meta, "engine", None)
        if isinstance(engine, engines.Distributed):
            cluster = self.quote_name_unless_alias(engine.cluster)
            local_table = self.quote_name_unless_alias(engine.table)
            delete = f"ALTER TABLE {local_table} ON CLUSTER {cluster} DELETE"
        else:
            delete = f"ALTER TABLE {table} DELETE"
        where, params = self._compile_where(table)
        return f"{delete} WHERE {where}", tuple(params)

    def as_sql(self):
        sql, params = super().as_sql()
        sql, params = self._add_settings_sql(sql, params)
        return sql, params


class SQLUpdateCompiler(ClickhouseMixin, compiler.SQLUpdateCompiler):
    def as_sql(self):
        """
        When execute DELETE and UPDATE query. Clickhouse does not support
        "table"."column" in WHERE clause.
        """
        self.pre_sql_setup()
        if not self.query.values:
            return "", ()
        qn = self.quote_name_unless_alias
        values, update_params = [], []
        for field, model, val in self.query.values:
            if hasattr(val, "resolve_expression"):
                val = val.resolve_expression(
                    self.query, allow_joins=False, for_save=True
                )
                if val.contains_aggregate:
                    raise compiler.FieldError(
                        "Aggregate functions are not allowed in this query "
                        "(%s=%r)." % (field.name, val)
                    )
                if val.contains_over_clause:
                    raise compiler.FieldError(
                        "Window expressions are not allowed in this query "
                        "(%s=%r)." % (field.name, val)
                    )
            elif hasattr(val, "prepare_database_save"):
                if field.remote_field:
                    val = field.get_db_prep_value(
                        val.prepare_database_save(field),
                        connection=self.connection,
                    )
                else:
                    raise TypeError(
                        "Tried to update field %s with a model instance, %r. "
                        "Use a value compatible with %s."
                        % (field, val, field.__class__.__name__)
                    )
            else:
                # update params are formatted into query string.
                val = field.get_db_prep_value(val, connection=self.connection)

            # Getting the placeholder for the field.
            if hasattr(field, "get_placeholder"):
                placeholder = field.get_placeholder(val, self, self.connection)
            else:
                placeholder = "%s"
            name = field.column
            if hasattr(val, "as_sql"):
                sql, params = self.compile(val)
                values.append("%s = %s" % (qn(name), placeholder % sql))
                update_params.extend(params)
            elif val is not None:
                values.append("%s = %s" % (qn(name), placeholder))
                update_params.append(val)
            else:
                values.append("%s = NULL" % qn(name))

        # Replace "table"."field" to "field", clickhouse does not support that.
        result = []
        table = qn(self.query.base_table)
        engine = getattr(self.query.model._meta, "engine", None)
        if isinstance(engine, engines.Distributed):
            cluster = qn(engine.cluster)
            local_table = qn(engine.table)
            result.append(f"ALTER TABLE {local_table} ON CLUSTER {cluster} UPDATE")
        else:
            result.append(f"ALTER TABLE {table} UPDATE")
        result.append(", ".join(values).replace(table + ".", ""))
        where, params = self._compile_where(table)
        result.append(f"WHERE {where}")
        params = (*update_params, *params)
        return self._add_settings_sql(" ".join(result), params)


class SQLAggregateCompiler(ClickhouseMixin, compiler.SQLAggregateCompiler):
    def as_sql(self):
        sql, params = super().as_sql()
        sql, params = self._add_settings_sql(sql, params)
        sql, params = self._add_explain_sql(sql, params)
        return sql, params
