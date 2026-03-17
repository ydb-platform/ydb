import enum

from sqlalchemy import schema, types as sqltypes, exc, util as sa_util, text
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import (
    compiler, expression, type_api, literal_column, elements, ClauseElement
)
from sqlalchemy.sql.ddl import CreateColumn
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.util.compat import inspect_getfullargspec
from sqlalchemy.util import (
    warn,
    to_list,
)

from sqlalchemy.sql.compiler import crud

from .. import Table, types
from .. import engines
from ..util import compat

# Column specifications
colspecs = {}


# Type converters
ischema_names = {
    'Int64': types.Int64,
    'Int32': types.Int32,
    'Int16': types.Int16,
    'Int8': types.Int8,
    'UInt64': types.UInt64,
    'UInt32': types.UInt32,
    'UInt16': types.UInt16,
    'UInt8': types.UInt8,
    'Date': types.Date,
    'DateTime': types.DateTime,
    'DateTime64': types.DateTime64,
    'Float64': types.Float64,
    'Float32': types.Float32,
    'Decimal': types.Decimal,
    'String': types.String,
    'UUID': types.UUID,
    'IPv4': types.IPv4,
    'IPv6': types.IPv6,
    'FixedString': types.String,
    'Enum8': types.Enum8,
    'Enum16': types.Enum16,
    '_array': types.Array,
    '_nullable': types.Nullable,
    '_lowcardinality': types.LowCardinality,
    '_tuple': types.Tuple,
    '_map': types.Map,
}


class ClickHouseIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = compiler.IdentifierPreparer.reserved_words | set((
        'index',  # reserved in the 'create table' syntax, at least.
    ))
    # Alternatively, use `_requires_quotes = lambda self, value: True`

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')


class ClickHouseCompiler(compiler.SQLCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + ' %% ' + \
            self.process(binary.right, **kw)

    def visit_isnot_distinct_from_binary(self, binary, operator, **kw):
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
        return "NOT %s" % self.visit_isnot_distinct_from_binary(
            binary, operator, **kw
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
        if clause.else_ is None:
            raise exc.CompileError('ELSE clause is required in CASE')

        text += 'ELSE ' + clause.else_._compiler_dispatch(
            self, **kwargs
        ) + ' END'
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

        # need to make a variable to prevent leaks in some debuggers
        join_type = getattr(join, 'type', None)
        if join_type is None:
            if join.isouter:
                join_type = 'LEFT OUTER'
            else:
                join_type = 'INNER'
        elif join_type is not None:
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
        if join.full and 'FULL' not in join_type:
            join_type = 'FULL ' + join_type

        if getattr(join, 'strictness', None):
            join_type = join.strictness.upper() + ' ' + join_type

        if getattr(join, 'distribution', None):
            join_type = join.distribution.upper() + ' ' + join_type

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
        return ' \nARRAY JOIN {columns}'.format(
            columns=', '.join(
                col._compiler_dispatch(self,
                                       within_label_clause=False,
                                       within_columns_clause=True,
                                       **kwargs)
                for col in array_join.clauses

            )
        )

    def visit_label(self,
                    label,
                    from_labeled_label=False,
                    **kw):
        if from_labeled_label:
            return super(ClickHouseCompiler, self).visit_label(
                label,
                render_label_as_label=label
            )
        else:
            return super(ClickHouseCompiler, self).visit_label(
                label,
                **kw
            )

    def _compose_select_body(
            self, text, select, inner_columns, froms, byfrom, kwargs):
        text += ', '.join(inner_columns)

        if froms:
            text += " \nFROM "

            if select._hints:
                text += ', '.join(
                    [
                        f._compiler_dispatch(
                                self,
                                asfrom=True,
                                fromhints=byfrom,
                                **kwargs
                        )
                        for f in froms
                    ]
                )
            else:
                text += ', '.join(
                    [f._compiler_dispatch(self, asfrom=True, **kwargs)
                     for f in froms]
                )
        else:
            text += self.default_from()

        if getattr(select, '_array_join', None) is not None:
            text += select._array_join._compiler_dispatch(self, **kwargs)

        sample_clause = getattr(select, '_sample_clause', None)

        if sample_clause is not None:
            text += self.sample_clause(select, **kwargs)

        final_clause = getattr(select, '_final_clause', None)

        if final_clause is not None:
            text += self.final_clause()

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nWHERE " + t

        if select._group_by_clause.clauses:
            text += self.group_by_clause(select, **kwargs)

        if select._having is not None:
            t = select._having._compiler_dispatch(self, **kwargs)
            if t:
                text += " \nHAVING " + t

        if select._order_by_clause.clauses:
            text += self.order_by_clause(select, **kwargs)

        limit_by_clause = getattr(select, '_limit_by_clause', None)

        if limit_by_clause is not None:
            text += self.limit_by_clause(select, **kwargs)

        if (select._limit_clause is not None or
                select._offset_clause is not None):
            text += self.limit_clause(select, **kwargs)

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

            if getattr(select, '_with_totals', False):
                text += " WITH TOTALS"

        return text

    def visit_delete(self, delete_stmt, asfrom=False, **kw):
        if not self.dialect.supports_delete:
            raise exc.CompileError(
                'ALTER DELETE is not supported by this server version'
            )

        extra_froms = delete_stmt._extra_froms

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

        if delete_stmt._whereclause is not None:
            # Do not include table name.
            # ClickHouse doesn't expect tablename in where.
            t = delete_stmt._whereclause._compiler_dispatch(
                self, include_table=False, **kw
            )
            if t:
                text += " WHERE " + t
        else:
            raise exc.CompileError('WHERE clause is required')

        self.stack.pop(-1)

        return text

    def visit_update(self, update_stmt, asfrom=False, **kw):
        if not self.dialect.supports_update:
            raise exc.CompileError(
                'ALTER UPDATE is not supported by this server version'
            )

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
        crud_params = crud._setup_crud_params(
            self, update_stmt, crud.ISUPDATE, **kw
        )

        text += table_text
        text += " UPDATE "

        text += ", ".join(
            c[0]._compiler_dispatch(self, include_table=False) +
            "=" + c[1]
            for c in crud_params
        )

        if update_stmt._whereclause is not None:
            # Do not include table name.
            # ClickHouse doesn't expect tablename in where.
            t = self.process(update_stmt._whereclause, include_table=False,
                             **kw)
            if t:
                text += " WHERE " + t
        else:
            raise exc.CompileError('WHERE clause is required')

        self.stack.pop(-1)

        return text


class ClickHouseDDLCompiler(compiler.DDLCompiler):
    def _get_default_string(self, default, name):
        sa_util.assert_arg_type(
            default, (sa_util.string_types[0], ClauseElement, TextClause), name
        )

        if isinstance(default, sa_util.string_types):
            return self.sql_compiler.render_literal_value(
                default, sqltypes.STRINGTYPE
            )
        else:
            return self.sql_compiler.process(
                default, literal_binds=True, include_table=False
            )

    def get_column_specification(self, column, **kw):
        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler.process(
                column.type, type_expression=column
            )
        )

        opts = column.dialect_options['clickhouse']

        if column.server_default is not None:
            colspec += " DEFAULT " + self._get_default_string(
                column.server_default.arg, 'server_default'
            )
        elif opts['materialized'] is not None:
            colspec += " MATERIALIZED " + self._get_default_string(
                opts['materialized'], 'clickhouse_materialized'
            )
        elif opts['alias'] is not None:
            colspec += " ALIAS " + self._get_default_string(
                opts['alias'], 'clickhouse_alias'
            )

        codec = opts['codec']
        if codec is not None:
            if isinstance(codec, (list, tuple)):
                codec = ', '.join(codec)
            colspec += " CODEC({0})".format(codec)

        if opts['after'] is not None:
            colspec += " AFTER " + self._get_default_string(
                opts['after'], 'clickhouse_after'
            )

        return colspec

    def visit_create_column(self, create, **kw):
        column = create.element
        nullable = column.nullable

        # All columns including synthetic PKs must be 'nullable'
        column.nullable = True

        rv = super(ClickHouseDDLCompiler, self).visit_create_column(
            create, **kw
        )
        column.nullable = nullable

        return rv

    def visit_primary_key_constraint(self, constraint):
        # Do not render PKs.
        return ''

    def _compile_param(self, expr, opt_list=False):
        compiler = self.sql_compiler

        # Do not render unnecessary brackets.
        if isinstance(expr, (list, tuple)) and len(expr) == 1 and opt_list:
            expr = expr[0]

        if isinstance(expr, (list, tuple)):
            return '(' + ', '.join(
                self._compile_param(el) for el in expr
            ) + ')'
        if not isinstance(expr, expression.ColumnClause):
            if not hasattr(expr, 'self_group'):
                # assuming base type (int, string, etc.)
                return compat.text_type(expr)
            else:
                expr = expr.self_group()
        return compiler.process(
            expr, include_table=False, literal_binds=True
        )

    def visit_merge_tree(self, engine):
        param = engine.get_parameters()
        if param is None:
            param = '()'
        else:
            param = self._compile_param(to_list(param))

        text = '{0}{1}\n'.format(engine.name, param)
        if engine.partition_by:
            text += ' PARTITION BY {0}\n'.format(
                self._compile_param(
                    engine.partition_by.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.order_by:
            text += ' ORDER BY {0}\n'.format(
                self._compile_param(
                    engine.order_by.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.primary_key:
            text += ' PRIMARY KEY {0}\n'.format(
                self._compile_param(
                    engine.primary_key.get_expressions_or_columns(),
                    opt_list=True
                )
            )
        if engine.sample_by:
            text += ' SAMPLE BY {0}\n'.format(
                self._compile_param(
                    engine.sample_by.get_expressions_or_columns()[0]
                )
            )
        if engine.ttl:
            compile = self.sql_compiler.process
            text += ' TTL {0}\n'.format(
                ',\n     '.join(
                    compile(i, include_table=False, literal_binds=True)
                    for i in engine.ttl.get_expressions_or_columns()
                )
            )
        if engine.settings:
            text += ' SETTINGS ' + ', '.join(
                '{key}={value}'.format(
                    key=key,
                    value=value
                )
                for key, value in sorted(engine.settings.items())
            )
        return text

    def visit_engine(self, engine):
        engine_params = engine.get_parameters()
        text = engine.name
        if not engine_params:
            return text

        text += '('

        compiled_params = []
        for param in engine_params:
            if isinstance(param, tuple):
                compiled = (
                    '('
                    + ', '.join(self._compile_param(p) for p in param)
                    + ')'
                )
            else:
                compiled = self._compile_param(param)

            compiled_params.append(compiled)

        text += ', '.join(compiled_params)

        return text + ')'

    def create_table_suffix(self, table):
        cluster = table.dialect_options['clickhouse']['cluster']
        if cluster:
            return 'ON CLUSTER ' + self.preparer.quote(cluster)

        return ''

    def post_create_table(self, table):
        engine = getattr(table, 'engine', None)

        if not engine:
            raise exc.CompileError("No engine for table '%s'" % table.name)

        return ' ENGINE = ' + self.process(engine)

    def visit_create_materialized_view(self, create):
        mv = create.element
        text = '\nCREATE MATERIALIZED VIEW '

        if create.if_not_exists:
            text += 'IF NOT EXISTS '

        text += self.preparer.format_table(mv)

        if mv.cluster:
            text += ' ON CLUSTER ' + self.preparer.quote(mv.cluster)

        if mv.to:
            text += ' TO ' + self.preparer.quote(mv.inner_table.name)

        else:
            text += ' ('

            separator = ''

            for column in mv.inner_table.columns:
                processed = self.process(CreateColumn(column))
                if processed is not None:
                    text += separator
                    separator = ', '
                    text += processed

            text += ')'

            text += self.post_create_table(mv.inner_table)

        text += ' '

        if mv.populate:
            text += 'POPULATE '

        text += 'AS ' + self.sql_compiler.process(
            mv.mv_selectable, literal_binds=True
        )

        return text

    def visit_drop_table(self, drop):
        text = '\nDROP TABLE '

        if getattr(drop, "if_exists", False):
            text += 'IF EXISTS '

        rv = text + self.preparer.format_table(drop.element)

        if getattr(drop, "on_cluster", False):
            rv += (
                ' ON CLUSTER ' +
                self.preparer.quote(drop.on_cluster)
            )

        return rv


class ClickHouseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_string(self, type_, **kw):
        if type_.length is None:
            return 'String'
        else:
            return 'FixedString(%s)' % type_.length

    def visit_array(self, type_, **kw):
        item_type = type_api.to_instance(type_.item_type)
        return 'Array(%s)' % self.process(item_type, **kw)

    def visit_nullable(self, type_, **kw):
        nested_type = type_api.to_instance(type_.nested_type)
        return 'Nullable(%s)' % self.process(nested_type, **kw)

    def visit_lowcardinality(self, type_, **kw):
        nested_type = type_api.to_instance(type_.nested_type)
        return "LowCardinality(%s)" % self.process(nested_type, **kw)

    def visit_int8(self, type_, **kw):
        return 'Int8'

    def visit_uint8(self, type_, **kw):
        return 'UInt8'

    def visit_int16(self, type_, **kw):
        return 'Int16'

    def visit_uint16(self, type_, **kw):
        return 'UInt16'

    def visit_int32(self, type_, **kw):
        return 'Int32'

    def visit_uint32(self, type_, **kw):
        return 'UInt32'

    def visit_int64(self, type_, **kw):
        return 'Int64'

    def visit_uint64(self, type_, **kw):
        return 'UInt64'

    def visit_date(self, type_, **kw):
        return 'Date'

    def visit_datetime(self, type_, **kw):
        return 'DateTime'

    def visit_datetime64(self, type_, **kw):
        if type_.timezone:
            return 'DateTime64(%s, \'%s\')' % (type_.precision, type_.timezone)
        else:
            return 'DateTime64(%s)' % type_.precision

    def visit_float32(self, type_, **kw):
        return 'Float32'

    def visit_float64(self, type_, **kw):
        return 'Float64'

    def visit_numeric(self, type_, **kw):
        return 'Decimal(%s, %s)' % (type_.precision, type_.scale)

    def visit_boolean(self, type_, **kw):
        return 'UInt8'

    def visit_nested(self, nested, **kwargs):
        ddl_compiler = self.dialect.ddl_compiler(self.dialect, None)
        cols_create = [
            ddl_compiler.visit_create_column(CreateColumn(nested_child))
            for nested_child in nested.columns
        ]
        return 'Nested(%s)' % ', '.join(cols_create)

    def _render_enum(self, db_type, type_, **kw):
        choices = (
            "'%s' = %d" %
            (x.name.replace("'", r"\'"), x.value) for x in type_.enum_class
        )
        return '%s(%s)' % (db_type, ', '.join(choices))

    def visit_enum(self, type_, **kw):
        return self._render_enum('Enum', type_, **kw)

    def visit_enum8(self, type_, **kw):
        return self._render_enum('Enum8', type_, **kw)

    def visit_enum16(self, type_, **kw):
        return self._render_enum('Enum16', type_, **kw)

    def visit_uuid(self, type_, **kw):
        return 'UUID'

    def visit_ipv4(self, type_, **kw):
        return 'IPv4'

    def visit_ipv6(self, type_, **kw):
        return 'IPv6'

    def visit_tuple(self, type_, **kw):
        cols = (
            self.process(type_api.to_instance(nested_type), **kw)
            for nested_type in type_.nested_types
        )
        return 'Tuple(%s)' % ', '.join(cols)

    def visit_map(self, type_, **kw):
        key_type = type_api.to_instance(type_.key_type)
        value_type = type_api.to_instance(type_.value_type)
        return 'Map(%s, %s)' % (
            self.process(key_type, **kw),
            self.process(value_type, **kw)
        )


class ClickHouseExecutionContextBase(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False  # No DML supported, never autocommit


class ClickHouseDialect(default.DefaultDialect):
    name = 'clickhouse'
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_native_boolean = False
    non_native_boolean_check_constraint = False
    supports_alter = True
    supports_sequences = False
    supports_native_enum = True  # Do not render check constraints on enums.
    supports_multivalues_insert = True

    # Dialect related-features
    supports_delete = True
    supports_update = True
    supports_engine_reflection = True

    engine_reflection = True  # Disables engine reflection from URL.

    max_identifier_length = 127
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False
    forced_server_version_string = None

    preparer = ClickHouseIdentifierPreparer
    type_compiler = ClickHouseTypeCompiler
    statement_compiler = ClickHouseCompiler
    ddl_compiler = ClickHouseDDLCompiler

    construct_arguments = [
        (schema.Table, {
            'data': [],
            'cluster': None,
        }),
        (schema.Column, {
            'codec': None,
            'materialized': None,
            'alias': None,
            'after': None,
        }),
    ]

    def initialize(self, connection):
        super(ClickHouseDialect, self).initialize(connection)

        version = self.server_version_info

        self.supports_delete = version >= (1, 1, 54388)
        self.supports_update = version >= (18, 12, 14)
        self.supports_engine_reflection = version >= (18, 16, 0)

    def _execute(self, connection, sql, scalar=False, **kwargs):
        raise NotImplementedError

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        query = text(
            "SELECT name FROM system.tables WHERE engine LIKE '%View' "
            "AND database = :database"
        )

        database = schema or connection.engine.url.database
        rows = self._execute(connection, query, database=database)
        return [row.name for row in rows]

    def has_table(self, connection, table_name, schema=None):
        quote = self._quote_table_name
        if schema:
            qualified_name = quote(schema) + '.' + quote(table_name)
        else:
            qualified_name = quote(table_name)
        query = 'EXISTS TABLE {}'.format(qualified_name)
        for r in self._execute(connection, query):
            if r.result == 1:
                return True
        return False

    def reflecttable(self, connection, table, include_columns, exclude_columns,
                     resolve_fks, **opts):
        """
        Hack to ensure the autoloaded table class is
        `clickhouse_sqlalchemy.Table`
        (to support CH-specific features e.g. joins).
        """
        # This check is necessary to support direct instantiation of
        # `clickhouse_sqlalchemy.Table` and then reflection of it.
        if not isinstance(table, Table):
            table.metadata.remove(table)
            ch_table = Table._make_from_standard(
                table, _extend_on=opts.get('_extend_on')
            )
        else:
            ch_table = table

        rv = super(ClickHouseDialect, self).reflecttable(
            connection, ch_table, include_columns, exclude_columns,
            resolve_fks, **opts)

        self._reflect_engine(connection, table.name, table)

        return rv

    def _reflect_engine(self, connection, table_name, table):
        if not self.supports_engine_reflection or not self.engine_reflection:
            return
        engine_cls_by_name = {e.__name__: e for e in engines.__all__}

        e = self.get_engine(connection, table_name, schema=table.schema)
        if not e:
            raise ValueError("Cannot find engine for table '%s'" % table_name)

        engine_cls = engine_cls_by_name.get(e['engine'])
        if engine_cls is not None:
            engine = engine_cls.reflect(table, **e)
            engine._set_parent(table)

    def _quote_table_name(self, table_name):
        # Use case: `describe table (select ...)`, over a TextClause.
        if isinstance(table_name, elements.TextClause):
            return str(table_name)
        return self.identifier_preparer.quote_identifier(table_name)

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        quote = self._quote_table_name
        if schema:
            qualified_name = quote(schema) + '.' + quote(table_name)
        else:
            qualified_name = quote(table_name)
        query = 'DESCRIBE TABLE {}'.format(qualified_name)
        rows = self._execute(connection, query)

        return [
            self._get_column_info(
                r.name, r.type, r.default_type, r.default_expression
            ) for r in rows
        ]

    def _get_column_info(self, name, format_type, default_type,
                         default_expression):
        col_type = self._get_column_type(name, format_type)
        col_default = self._get_column_default(default_type,
                                               default_expression)
        result = {
            'name': name,
            'type': col_type,
            'nullable': format_type.startswith('Nullable('),
            'default': col_default,
        }
        return result

    def _get_column_default(self, default_type, default_expression):
        if default_type == 'DEFAULT':
            return default_expression
        return None

    def _get_column_type(self, name, spec):
        if spec.startswith('Array'):
            inner = spec[6:-1]
            coltype = self.ischema_names['_array']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('FixedString'):
            length = int(spec[12:-1])
            return self.ischema_names['FixedString'](length)

        elif spec.startswith('Nullable'):
            inner = spec[9:-1]
            coltype = self.ischema_names['_nullable']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('LowCardinality'):
            inner = spec[15:-1]
            coltype = self.ischema_names['_lowcardinality']
            return coltype(self._get_column_type(name, inner))

        elif spec.startswith('Tuple'):
            inner = spec[6:-1]
            coltype = self.ischema_names['_tuple']
            inner_types = [
                self._get_column_type(name, t.strip())
                for t in inner.split(',')
            ]
            return coltype(*inner_types)

        elif spec.startswith('Map'):
            inner = spec[4:-1]
            coltype = self.ischema_names['_map']
            inner_types = [
                self._get_column_type(name, t.strip())
                for t in inner.split(',')
            ]
            return coltype(*inner_types)

        elif spec.startswith('Enum'):
            pos = spec.find('(')
            type = spec[:pos]
            coltype = self.ischema_names[type]

            options = dict()
            if pos >= 0:
                options = self._parse_options(
                    spec[pos + 1: spec.rfind(')')]
                )
            if not options:
                return sqltypes.NullType

            type_enum = enum.Enum('%s_enum' % name, options)
            return lambda: coltype(type_enum)

        elif spec.lower().startswith('decimal'):
            coltype = self.ischema_names['Decimal']
            return coltype(*self._parse_decimal_params(spec))
        else:
            try:
                return self.ischema_names[spec]
            except KeyError:
                warn("Did not recognize type '%s' of column '%s'" %
                     (spec, name))
                return sqltypes.NullType

    @staticmethod
    def _parse_decimal_params(spec):
        ints = spec.split('(')[-1].split(')')[0]  # get all data in brackets
        precision, scale = ints.split(',')
        return int(precision.strip()), int(scale.strip())

    @staticmethod
    def _parse_options(option_string):
        options = dict()
        after_name = False
        escaped = False
        quote_character = None
        name = ''
        value = ''

        for ch in option_string:
            if escaped:
                name += ch
                escaped = False  # Accepting escaped character

            elif after_name:
                if ch in (' ', '='):
                    pass
                elif ch == ',':
                    options[name] = int(value)
                    after_name = False
                    name = ''
                    value = ''  # Reset before collecting new option
                else:
                    value += ch

            elif quote_character:
                if ch == '\\':
                    escaped = True
                elif ch == quote_character:
                    quote_character = None
                    after_name = True  # Start collecting option value
                else:
                    name += ch

            else:
                if ch == "'":
                    quote_character = ch

        if after_name:
            options.setdefault(name, int(value))  # Word after last comma

        return options

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        rows = self._execute(connection, 'SHOW DATABASES')
        return [row.name for row in rows]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        # No support for indexes.
        return []

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = text(
            "SELECT name FROM system.tables "
            "WHERE engine NOT LIKE '%View' "
            "AND name NOT LIKE '.inner%' "
            "AND database = :database"
        )

        database = schema or connection.engine.url.database
        rows = self._execute(connection, query, database=database)
        return [row.name for row in rows]

    @reflection.cache
    def get_engine(self, connection, table_name, schema=None, **kw):
        columns = [
            'name', 'engine_full', 'engine', 'partition_key', 'sorting_key',
            'primary_key', 'sampling_key'
        ]

        if schema:
            database = schema
        else:
            database = connection.engine.url.database

        query = text(
            'SELECT {} FROM system.tables '
            'WHERE database = :database AND name = :name'
            .format(', '.join(columns))
        )

        rows = self._execute(
            connection, query, database=database, name=table_name
        )

        row = next(rows, None)

        if row:
            return {x: getattr(row, x, None) for x in columns}

    def do_rollback(self, dbapi_connection):
        # No support for transactions.
        pass

    def do_executemany(self, cursor, statement, parameters, context=None):
        # render single insert inplace
        if (
            context
            and context.isinsert
            and context.compiled.insert_single_values_expr
            and not len(context.compiled_parameters[0])
        ):
            parameters = None

        cursor.executemany(statement, parameters, context=context)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters, context=context)

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    def _get_server_version_info(self, connection):
        version = self.forced_server_version_string

        if version is None:
            version = self._execute(
                connection, 'select version()', scalar=True
            )

        # The first three are numeric, but the last is an alphanumeric build.
        return tuple(int(p) if p.isdigit() else p for p in version.split('.'))

    def _get_default_schema_name(self, connection):
        return self._execute(
            connection, 'select currentDatabase()', scalar=True
        )

    def connect(self, *cargs, **cparams):
        self.forced_server_version_string = cparams.pop(
            'server_version', self.forced_server_version_string)
        return super(ClickHouseDialect, self).connect(*cargs, **cparams)


clickhouse_dialect = ClickHouseDialect()
