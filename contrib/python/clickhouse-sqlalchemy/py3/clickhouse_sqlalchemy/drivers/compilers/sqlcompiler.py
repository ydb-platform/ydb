from sqlalchemy import exc, literal_column
from sqlalchemy.sql import compiler, elements, COLLECT_CARTESIAN_PRODUCTS, \
    WARN_LINTING, crud
from sqlalchemy.sql import type_api
from sqlalchemy.util import inspect_getfullargspec

from ... import types


class ClickHouseSQLCompiler(compiler.SQLCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + ' %% ' + \
            self.process(binary.right, **kw)

    def visit_is_not_distinct_from_binary(self, binary, operator, **kw):
        """
        Implementation of distinctness comparison in ClickHouse SQL.
        A distinctness comparison treats NULL as if it is a (singleton)
        value and is what ClickHouse uses for `SELECT DISTINCT` and `GROUP BY`.
        Some databases have direct support for a `IS DISTINCT` comparison, but
        ClickHouse does not, so we rely on the `hasAny` array function here.
        """

        return "hasAny([%s], [%s])" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def visit_is_distinct_from_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_is_not_distinct_from_binary(
            binary, operator, **kw
        )

    def visit_empty_set_expr(self, element_types):
        return "SELECT %s WHERE 1!=1" % (
            ", ".join(
                "CAST(NULL AS %s)"
                % self.dialect.type_compiler.process(
                    t if isinstance(t, types.Nullable) else types.Nullable(t)
                )
                for t in element_types or [types.Int8()]
            ),
        )

    def post_process_text(self, text):
        return text.replace('%', '%%')

    def visit_count_func(self, fn, **kw):
        # count accepts zero arguments.
        return 'count%s' % self.process(fn.clause_expr, **kw)

    def visit_case(self, clause, **kwargs):
        text = 'CASE '
        if clause.value is not None:
            text += clause.value._compiler_dispatch(self, **kwargs) + ' '
        for cond, result in clause.whens:
            text += 'WHEN ' + cond._compiler_dispatch(
                self, **kwargs
            ) + ' THEN ' + result._compiler_dispatch(
                self, **kwargs) + " "

        if clause.else_ is not None:
            text += 'ELSE ' + clause.else_._compiler_dispatch(
                self, **kwargs
            ) + ' '

        text += 'END'
        return text

    def visit_if__func(self, func, **kw):
        return "(%s) ? (%s) : (%s)" % (
            self.process(func.clauses.clauses[0], **kw),
            self.process(func.clauses.clauses[1], **kw),
            self.process(func.clauses.clauses[2], **kw)
        )

    def limit_by_clause(self, select, **kw):
        text = ''
        limit_by_clause = select._limit_by_clause
        if limit_by_clause:
            text += ' LIMIT '
            if limit_by_clause.offset is not None:
                text += self.process(limit_by_clause.offset, **kw) + ', '
            text += self.process(limit_by_clause.limit, **kw)
            limit_by_exprs = limit_by_clause.by_clauses._compiler_dispatch(
                self, **kw
            )
            text += ' BY ' + limit_by_exprs

        return text

    def limit_clause(self, select, **kw):
        text = ''
        if select._limit_clause is not None:
            text += ' \n LIMIT '
            if select._offset_clause is not None:
                text += self.process(select._offset_clause, **kw) + ', '
            text += self.process(select._limit_clause, **kw)
        else:
            if select._offset_clause is not None:
                raise exc.CompileError('OFFSET without LIMIT is not supported')

        return text

    def visit_lambda(self, lambda_, **kw):
        func = lambda_.func
        spec = inspect_getfullargspec(func)

        if spec.varargs:
            raise exc.CompileError('Lambdas with *args are not supported')

        try:
            # ArgSpec in SA>=1.3.0b2
            keywords = spec.keywords
        except AttributeError:
            # FullArgSpec in SA>=1.3.0b2
            keywords = spec.varkw

        if keywords:
            raise exc.CompileError('Lambdas with **kwargs are not supported')

        text = ', '.join(spec.args) + ' -> '

        args = [literal_column(arg) for arg in spec.args]
        text += self.process(func(*args), **kw)

        return text

    def visit_extract(self, extract, **kw):
        field = self.extract_map.get(extract.field, extract.field)
        column = self.process(extract.expr, **kw)
        if field == 'year':
            return 'toYear(%s)' % column
        elif field == 'month':
            return 'toMonth(%s)' % column
        elif field == 'day':
            return 'toDayOfMonth(%s)' % column
        else:
            return column

    def visit_join(self, join, asfrom=False, **kwargs):
        text = join.left._compiler_dispatch(self, asfrom=True, **kwargs)

        if text[0] == '(' and text[-1] == ')':
            text = text[1:-1]

        flags = join.full
        if not isinstance(flags, dict):
            if isinstance(flags, tuple):
                flags = dict(flags)
            else:
                flags = {'full': flags}
        # need to make a variable to prevent leaks in some debuggers
        join_type = flags.get('type')
        if join_type is None:
            if flags.get('full'):
                join_type = 'FULL OUTER'
            elif join.isouter:
                join_type = 'LEFT OUTER'
            else:
                join_type = 'INNER'
        else:
            join_type = join_type.upper()
            if join.isouter and 'INNER' in join_type:
                raise exc.CompileError(
                    "can't compile join with specified "
                    "INNER type and isouter=True"
                )
            # isouter=False by default, disable that checking
            # elif not join.isouter and 'OUTER' in join.type:
            #     raise exc.CompileError(
            #         "can't compile join with specified "
            #         "OUTER type and isouter=False"
            #     )

        strictness = flags.get('strictness')
        if strictness:
            join_type = strictness.upper() + ' ' + join_type

        distribution = flags.get('distribution')
        if distribution:
            join_type = distribution.upper() + ' ' + join_type

        if join_type is not None:
            text += ' ' + join_type.upper() + ' JOIN '

        onclause = join.onclause

        text += join.right._compiler_dispatch(self, asfrom=True, **kwargs)
        if isinstance(onclause, elements.Tuple):
            text += ' USING ' + onclause._compiler_dispatch(
                self, include_table=False, **kwargs
            )
        else:
            text += ' ON ' + onclause._compiler_dispatch(self, **kwargs)
        return text

    def visit_array_join(self, array_join, **kwargs):
        kwargs['within_columns_clause'] = True

        return ' \nARRAY JOIN {columns}'.format(
            columns=', '.join(
                col._compiler_dispatch(self,
                                       within_label_clause=False,
                                       **kwargs)
                for col in array_join.clauses

            )
        )

    def visit_left_array_join(self, array_join, **kwargs):
        kwargs['within_columns_clause'] = True

        return ' \nLEFT ARRAY JOIN {columns}'.format(
            columns=', '.join(
                col._compiler_dispatch(self,
                                       within_label_clause=False,
                                       **kwargs)
                for col in array_join.clauses

            )
        )

    def visit_label(self,
                    label,
                    from_labeled_label=False,
                    **kw):
        if from_labeled_label:
            return super(ClickHouseSQLCompiler, self).visit_label(
                label,
                render_label_as_label=label
            )
        else:
            return super(ClickHouseSQLCompiler, self).visit_label(
                label,
                **kw
            )

    def _compose_select_body(
        self,
        text,
        select,
        compile_state,
        inner_columns,
        froms,
        byfrom,
        toplevel,
        kwargs,
    ):
        text += ", ".join(inner_columns)

        if self.linting & COLLECT_CARTESIAN_PRODUCTS:
            from_linter = compiler.FromLinter({}, set())
            warn_linting = self.linting & WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None
            warn_linting = False

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ", ".join(
                    [
                        f._compiler_dispatch(
                            self,
                            asfrom=True,
                            fromhints=byfrom,
                            from_linter=from_linter,
                            **kwargs
                        )
                        for f in froms
                    ]
                )
            else:
                text += ", ".join(
                    [
                        f._compiler_dispatch(
                            self,
                            asfrom=True,
                            from_linter=from_linter,
                            **kwargs
                        )
                        for f in froms
                    ]
                )
        else:
            text += self.default_from()

        sample_clause = getattr(select, '_sample_clause', None)

        if sample_clause is not None:
            text += self.sample_clause(select, **kwargs)

        if getattr(select, '_array_join', None) is not None:
            text += select._array_join._compiler_dispatch(self, **kwargs)

        final_clause = getattr(select, '_final_clause', None)

        if final_clause is not None:
            text += self.final_clause()

        if select._where_criteria:
            t = self._generate_delimited_and_list(
                select._where_criteria, from_linter=from_linter, **kwargs
            )
            if t:
                text += " \nWHERE " + t

        if warn_linting:
            from_linter.warn()

        if select._group_by_clauses:
            text += self.group_by_clause(select, **kwargs)

        if select._having_criteria:
            t = self._generate_delimited_and_list(
                select._having_criteria, **kwargs
            )
            if t:
                text += " \nHAVING " + t

        if select._order_by_clauses:
            text += self.order_by_clause(select, **kwargs)

        limit_by_clause = getattr(select, '_limit_by_clause', None)

        if limit_by_clause is not None:
            text += self.limit_by_clause(select, **kwargs)

        if select._has_row_limiting_clause:
            text += self._row_limit_clause(select, **kwargs)

        if select._for_update_arg is not None:
            text += self.for_update_clause(select, **kwargs)

        return text

    def sample_clause(self, select, **kw):
        return " \nSAMPLE " + self.process(select._sample_clause, **kw)

    def final_clause(self):
        return " \nFINAL"

    def group_by_clause(self, select, **kw):
        text = ""

        group_by = select._group_by_clause._compiler_dispatch(
            self, **kw)

        if group_by:
            text = " GROUP BY " + group_by

            if getattr(select, '_with_cube', False):
                text += " WITH CUBE"

            if getattr(select, '_with_rollup', False):
                text += " WITH ROLLUP"

            if getattr(select, '_with_totals', False):
                text += " WITH TOTALS"

        return text

    def visit_delete(self, delete_stmt, **kw):
        if not self.dialect.supports_delete:
            raise exc.CompileError(
                'ALTER DELETE is not supported by this server version'
            )

        compile_state = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement

        extra_froms = compile_state._extra_froms

        correlate_froms = {delete_stmt.table}.union(extra_froms)
        self.stack.append(
            {
                "correlate_froms": correlate_froms,
                "asfrom_froms": correlate_froms,
                "selectable": delete_stmt,
            }
        )

        text = "ALTER TABLE "

        table_text = self.delete_table_clause(
            delete_stmt, delete_stmt.table, extra_froms
        )

        text += table_text + " DELETE"

        if delete_stmt._where_criteria:
            t = self._generate_delimited_and_list(
                delete_stmt._where_criteria, include_table=False, **kw
            )
            if t:
                text += " WHERE " + t
        else:
            raise exc.CompileError('WHERE clause is required')

        self.stack.pop(-1)

        return text

    def visit_update(self, update_stmt, **kw):
        if not self.dialect.supports_update:
            raise exc.CompileError(
                'ALTER UPDATE is not supported by this server version'
            )

        compile_state = update_stmt._compile_state_factory(
            update_stmt, self, **kw
        )
        update_stmt = compile_state.statement

        render_extra_froms = []
        correlate_froms = {update_stmt.table}

        self.stack.append(
            {
                "correlate_froms": correlate_froms,
                "asfrom_froms": correlate_froms,
                "selectable": update_stmt,
            }
        )

        text = "ALTER TABLE "

        table_text = self.update_tables_clause(
            update_stmt, update_stmt.table, render_extra_froms, **kw
        )
        crud_params = crud._get_crud_params(
            self, update_stmt, compile_state, **kw
        )

        text += table_text
        text += " UPDATE "

        text += ", ".join(expr + "=" + value for c, expr, value in crud_params)

        if update_stmt._where_criteria:
            t = self._generate_delimited_and_list(
                update_stmt._where_criteria, include_table=False, **kw
            )
            if t:
                text += " WHERE " + t
        else:
            raise exc.CompileError('WHERE clause is required')

        self.stack.pop(-1)

        return text

    def render_literal_value(self, value, type_):
        if isinstance(value, list):
            return (
                '[' +
                ', '.join(self.render_literal_value(
                        x, type_api._resolve_value_to_type(x)
                    ) for x in value) +
                ']'
            )
        else:
            return super(ClickHouseSQLCompiler, self).render_literal_value(
                value, type_
            )

    def _get_regexp_args(self, binary, kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        return string, pattern

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string, pattern = self._get_regexp_args(binary, kw)
        return "MATCH(%s, %s)" % (string, pattern)

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return "NOT %s" % self.visit_regexp_match_op_binary(
            binary,
            operator,
            **kw
        )

    def visit_ilike_case_insensitive_operand(self, element, **kw):
        return element.element._compiler_dispatch(self, **kw)

    def visit_ilike_op_binary(self, binary, operator, **kw):
        return "%s ILIKE %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_not_ilike_op_binary(self, binary, operator, **kw):
        return "%s NOT ILIKE %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )
