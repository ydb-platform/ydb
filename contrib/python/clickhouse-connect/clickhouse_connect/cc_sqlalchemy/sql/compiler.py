from sqlalchemy.exc import CompileError
from sqlalchemy.sql import elements, sqltypes
from sqlalchemy.sql.compiler import SQLCompiler

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.sql import format_table


def _find_outermost_marker(text, markers):
    """Earliest index in `text` where any of `markers` appears at paren depth 0, skipping
    string literals (single-quoted) and backtick-quoted identifiers. -1 if no match.
    Used to splice PREWHERE into a SELECT body without matching subquery clauses.
    """
    depth = 0
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c == "'" or c == "`":
            quote = c
            i += 1
            while i < n:
                if text[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                if text[i] == quote:
                    i += 1
                    break
                i += 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0:
            for marker in markers:
                if text.startswith(marker, i):
                    return i
        i += 1
    return -1


def _resolve_ch_type_name(sqla_type):
    """Resolve a SQLAlchemy type instance to a ClickHouse type name string.

    Handles both native ChSqlaType instances which carry their ClickHouse name
    directly and generic SQLAlchemy types by mapping to reasonable ClickHouse defaults.
    """
    if isinstance(sqla_type, ChSqlaType):
        return sqla_type.name
    # Order matters so we need to check subtypes before parent types
    if isinstance(sqla_type, sqltypes.SmallInteger):
        return "Int16"
    if isinstance(sqla_type, sqltypes.BigInteger):
        return "Int64"
    if isinstance(sqla_type, sqltypes.Integer):
        return "Int32"
    if isinstance(sqla_type, sqltypes.Float):
        return "Float64"
    if isinstance(sqla_type, sqltypes.Numeric):
        p = sqla_type.precision or 18
        s = sqla_type.scale or 0
        return f"Decimal({p}, {s})"
    if isinstance(sqla_type, sqltypes.Boolean):
        return "Bool"
    if isinstance(sqla_type, sqltypes.DateTime):
        return "DateTime"
    if isinstance(sqla_type, sqltypes.Date):
        return "Date"
    if isinstance(sqla_type, sqltypes.String):
        return "String"
    return "String"


class ChStatementCompiler(SQLCompiler):
    def _raise_on_escape(self, binary, operator_name: str):
        if binary.modifiers.get("escape") is not None:
            raise CompileError(f"ClickHouse does not support the ESCAPE clause on {operator_name}")

    def visit_delete(self, delete_stmt, visiting_cte=None, **kw):
        table = delete_stmt.table
        text = f"DELETE FROM {format_table(table)}"

        if delete_stmt.whereclause is not None:
            self._in_delete_where = True
            try:
                text += " WHERE " + self.process(delete_stmt.whereclause, **kw)
            finally:
                self._in_delete_where = False
        else:
            raise CompileError("ClickHouse DELETE statements require a WHERE clause. To delete all rows, use 'TRUNCATE TABLE' instead.")

        return text

    def visit_values(self, element, asfrom=False, from_linter=None, visiting_cte=None, **kw):
        """Compile a VALUES clause using ClickHouse's VALUES table function syntax.

        ClickHouse requires the column structure as the first argument:
            VALUES('col1 Type1, col2 Type2', (row1_val1, row1_val2), ...)

        This differs from standard SQL which places column names after the alias:
            (VALUES (row1), (row2)) AS name (col1, col2)

        Compatible with both SQLAlchemy 1.4 and 2.x.
        """
        if getattr(element, "_independent_ctes", None):
            self._dispatch_independent_ctes(element, kw)

        structure = ", ".join(f"{col.name} {_resolve_ch_type_name(col.type)}" for col in element.columns)

        kw.setdefault("literal_binds", element.literal_binds)
        tuples = ", ".join(
            self.process(
                elements.Tuple(types=element._column_types, *elem).self_group(),  # noqa: B026
                **kw,
            )
            for chunk in element._data
            for elem in chunk
        )

        structure_literal = self.render_literal_value(structure, sqltypes.String())
        v = f"VALUES({structure_literal}, {tuples})"

        # SA 2.x has _unnamed; SA 1.4 uses name=None for unnamed values
        is_unnamed = getattr(element, "_unnamed", element.name is None)
        if is_unnamed:
            name = None
        elif isinstance(element.name, elements._truncated_label):
            name = self._truncated_identifier("values", element.name)
        else:
            name = element.name

        lateral = "LATERAL " if element._is_lateral else ""

        if asfrom:
            if from_linter:
                # SA 2.x has _de_clone(); SA 1.4 doesn't
                key = element._de_clone() if hasattr(element, "_de_clone") else element
                from_linter.froms[key] = name if name is not None else "(unnamed VALUES element)"

            if visiting_cte is not None and visiting_cte.element is element:
                if element._is_lateral:
                    raise CompileError("Can't use a LATERAL VALUES expression inside of a CTE")
                v = f"SELECT * FROM {v}"
            elif name:
                kw["include_table"] = False
                v = f"{lateral}{v}{self.get_render_as_alias_suffix(self.preparer.quote(name))}"
            else:
                v = f"{lateral}{v}"

        return v

    def visit_join(self, join, **kw):
        left = self.process(join.left, **kw)
        right = self.process(join.right, **kw)
        onclause = join.onclause

        is_cross = getattr(join, "_is_cross", False) or onclause is None
        if getattr(join, "full", False):
            join_type = "FULL OUTER JOIN"
        elif is_cross:
            join_type = "CROSS JOIN"
        elif join.isouter:
            join_type = "LEFT OUTER JOIN"
        else:
            join_type = "INNER JOIN"

        # ClickHouse modifiers: [GLOBAL] [ALL|ANY|ASOF] <join_type>
        distribution = getattr(join, "distribution", None)
        strictness = getattr(join, "strictness", None)
        parts = []
        if distribution:
            parts.append(distribution)
        if strictness:
            parts.append(strictness)
        parts.append(join_type)
        join_kw = " ".join(parts)

        text = f"{left} {join_kw} {right}"

        using_columns = getattr(join, "using_columns", None)
        if using_columns:
            # Process the onclause so the from-linter registers the
            # table relationship, but render USING syntax instead.
            if onclause is not None:
                self.process(onclause, **kw)
            quoted = ", ".join(self.preparer.quote(col) for col in using_columns)
            text += f" USING ({quoted})"
        elif not is_cross and onclause is not None:
            text += " ON " + self.process(onclause, **kw)

        return text

    def visit_column(self, column, add_to_result_map=None, include_table=True, result_map_targets=(), ambiguous_table_name_map=None, **kw):
        if getattr(self, "_in_delete_where", False):
            return self.preparer.quote(column.name)

        return super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=include_table,
            result_map_targets=result_map_targets,
            **kw,
        )

    # Abstract methods required by SQLCompiler
    def delete_extra_from_clause(self, delete_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support DELETE with extra FROM clause")

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support UPDATE with FROM clause")

    def visit_empty_set_expr(self, element_types, **kw):
        return "SELECT 1 WHERE 1=0"

    def visit_sequence(self, sequence, **kw):
        raise NotImplementedError("ClickHouse doesn't support sequences")

    def group_by_clause(self, select, **kw):
        """Render GROUP BY using label aliases instead of full expressions."""
        kw["_ch_group_by"] = True
        return super().group_by_clause(select, **kw)

    def visit_label(
        self,
        label,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        ch_group_by = kw.pop("_ch_group_by", False)
        if ch_group_by and not within_columns_clause and render_label_as_label is None:
            if isinstance(label.name, elements._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name
            return self.preparer.format_label(label, labelname)
        return super().visit_label(
            label,
            within_columns_clause=within_columns_clause,
            render_label_as_label=render_label_as_label,
            **kw,
        )

    def _ch_modifier_attr(self, select, compile_state, attr, default):
        """Read a CH modifier attribute."""
        val = getattr(select, attr, None)
        if val is not None:
            return val
        if compile_state is not None:
            orig = getattr(compile_state, "select_statement", None)
            if orig is not None and orig is not select:
                return getattr(orig, attr, default)
        return default

    def _compose_select_body(self, text, select, compile_state, inner_columns, froms, byfrom, toplevel, kwargs):
        ch_final = self._ch_modifier_attr(select, compile_state, "_ch_final", set())
        ch_sample = self._ch_modifier_attr(select, compile_state, "_ch_sample", {})
        ch_prewhere = self._ch_modifier_attr(select, compile_state, "_ch_prewhere", None)
        ch_limit_by = self._ch_modifier_attr(select, compile_state, "_ch_limit_by", None)

        prev_lb = getattr(self, "_ch_active_limit_by", None)
        self._ch_active_limit_by = ch_limit_by

        try:
            if ch_final or ch_sample:
                mods = {}
                for target in ch_final | set(ch_sample):
                    parts = []
                    if target in ch_final:
                        parts.append("FINAL")
                    if target in ch_sample:
                        parts.append(f"SAMPLE {ch_sample[target]}")
                    mods[target] = " ".join(parts)

                prev = getattr(self, "_ch_from_modifiers", None)
                self._ch_from_modifiers = mods
                try:
                    result = super()._compose_select_body(text, select, compile_state, inner_columns, froms, byfrom, toplevel, kwargs)
                finally:
                    self._ch_from_modifiers = prev
            else:
                result = super()._compose_select_body(text, select, compile_state, inner_columns, froms, byfrom, toplevel, kwargs)
        finally:
            self._ch_active_limit_by = prev_lb

        if ch_prewhere is not None:
            prewhere_text = self.process(ch_prewhere.whereclause, **kwargs)
            prewhere_segment = f" \nPREWHERE {prewhere_text}"
            markers = (" \nWHERE ", " GROUP BY ", " \nHAVING ", " ORDER BY ", "\n LIMIT ")
            insert_at = _find_outermost_marker(result, markers)
            if insert_at == -1:
                result = result + prewhere_segment
            else:
                result = result[:insert_at] + prewhere_segment + result[insert_at:]

        # LIMIT BY: SA calls limit_clause() only when there's a regular LIMIT/OFFSET.
        # Without one, it's never called, so append the LIMIT BY here instead.
        if ch_limit_by is not None and not select._has_row_limiting_clause:
            result += self._render_ch_limit_by(ch_limit_by, kwargs)

        return result

    def _render_ch_limit_by(self, ch_limit_by, kw):
        by_text = ", ".join(self.process(col, **kw) for col in ch_limit_by.by_clauses)
        offset_prefix = f"{ch_limit_by.offset}, " if ch_limit_by.offset is not None else ""
        return f"\n LIMIT {offset_prefix}{ch_limit_by.limit} BY {by_text}"

    def limit_clause(self, select, **kw):
        text = ""
        ch_limit_by = getattr(select, "_ch_limit_by", None)
        if ch_limit_by is None:
            ch_limit_by = getattr(self, "_ch_active_limit_by", None)
        if ch_limit_by is not None:
            text += self._render_ch_limit_by(ch_limit_by, kw)
        text += super().limit_clause(select, **kw)
        return text

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False, fromhints=None, enclosing_alias=None, **kwargs):
        result = super().visit_table(
            table, asfrom=asfrom, iscrud=iscrud, ashint=ashint, fromhints=fromhints, enclosing_alias=enclosing_alias, **kwargs
        )
        if asfrom and enclosing_alias is None:
            mods = getattr(self, "_ch_from_modifiers", None)
            if mods and table in mods:
                result += " " + mods[table]
        return result

    def visit_alias(self, alias, asfrom=False, **kwargs):
        result = super().visit_alias(alias, asfrom=asfrom, **kwargs)
        if asfrom:
            mods = getattr(self, "_ch_from_modifiers", None)
            if mods and alias in mods:
                result += " " + mods[alias]
        return result

    def visit_like_op_binary(self, binary, operator, **kw):
        self._raise_on_escape(binary, "LIKE")
        return super().visit_like_op_binary(binary, operator, **kw)

    def visit_not_like_op_binary(self, binary, operator, **kw):
        self._raise_on_escape(binary, "LIKE")
        return super().visit_not_like_op_binary(binary, operator, **kw)

    def visit_ilike_op_binary(self, binary, operator, **kw):
        self._raise_on_escape(binary, "ILIKE")
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)
        return f"{left} ILIKE {right}"

    def visit_not_ilike_op_binary(self, binary, operator, **kw):
        self._raise_on_escape(binary, "ILIKE")
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)
        return f"{left} NOT ILIKE {right}"
