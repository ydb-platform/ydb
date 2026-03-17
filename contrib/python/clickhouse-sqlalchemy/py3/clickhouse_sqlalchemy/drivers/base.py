import enum

from sqlalchemy import schema, types as sqltypes, util as sa_util, text
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import (
    compiler, elements
)
from sqlalchemy.util import (
    warn,
)

from .compilers.ddlcompiler import ClickHouseDDLCompiler
from .compilers.sqlcompiler import ClickHouseSQLCompiler
from .compilers.typecompiler import ClickHouseTypeCompiler
from .reflection import ClickHouseInspector
from .util import get_inner_spec
from .. import types

# Column specifications
colspecs = {}


# Type converters
ischema_names = {
    'Int256': types.Int256,
    'Int128': types.Int128,
    'Int64': types.Int64,
    'Int32': types.Int32,
    'Int16': types.Int16,
    'Int8': types.Int8,
    'UInt256': types.UInt256,
    'UInt128': types.UInt128,
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
    'Bool': types.Boolean,
    'Boolean': types.Boolean,
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
    supports_native_boolean = True
    non_native_boolean_check_constraint = False
    supports_alter = True
    supports_sequences = False
    supports_native_enum = True  # Do not render check constraints on enums.
    supports_multivalues_insert = True
    supports_statement_cache = True

    supports_comments = True
    inline_comments = True

    # Dialect related-features
    supports_delete = True
    supports_update = True
    supports_engine_reflection = True
    supports_table_comment_reflection = True

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
    statement_compiler = ClickHouseSQLCompiler
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

    inspector = ClickHouseInspector

    def initialize(self, connection):
        super(ClickHouseDialect, self).initialize(connection)

        version = self.server_version_info

        self.supports_delete = version >= (1, 1, 54388)
        self.supports_update = version >= (18, 12, 14)
        self.supports_engine_reflection = version >= (18, 16)
        self.supports_table_comment_reflection = version >= (21, 6)

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
                r.name, r.type, r.default_type, r.default_expression,
                getattr(r, 'comment', None)
            ) for r in rows
        ]

    def _get_column_info(self, name, format_type, default_type,
                         default_expression, comment):
        col_type = self._get_column_type(name, format_type)
        col_default = self._get_column_default(default_type,
                                               default_expression)
        result = {
            'name': name,
            'type': col_type,
            'nullable': format_type.startswith('Nullable('),
            'default': col_default,
            'comment': comment or None
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

        elif spec.startswith('Decimal'):
            coltype = self.ischema_names['Decimal']
            return coltype(*self._parse_decimal_params(spec))
        elif spec.startswith('DateTime64'):
            coltype = self.ischema_names['DateTime64']
            return coltype(*self._parse_detetime64_params(spec))
        elif spec.startswith('DateTime'):
            coltype = self.ischema_names['DateTime']
            return coltype(*self._parse_detetime_params(spec))
        else:
            try:
                return self.ischema_names[spec]
            except KeyError:
                warn("Did not recognize type '%s' of column '%s'" %
                     (spec, name))
                return sqltypes.NullType

    @staticmethod
    def _parse_decimal_params(spec):
        inner_spec = get_inner_spec(spec)
        precision, scale = inner_spec.split(',')
        return int(precision.strip()), int(scale.strip())

    @staticmethod
    def _parse_detetime64_params(spec):
        inner_spec = get_inner_spec(spec)
        if not inner_spec:
            return []
        params = inner_spec.split(',', 1)
        params[0] = int(params[0])
        if len(params) > 1:
            params[1] = params[1].strip()
        return params

    @staticmethod
    def _parse_detetime_params(spec):
        inner_spec = get_inner_spec(spec)
        if not inner_spec:
            return []
        return [inner_spec]

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
        should_reflect = (
            self.supports_engine_reflection and
            self.engine_reflection
        )
        if not should_reflect:
            return {}

        if schema:
            query = (("SELECT primary_key FROM system.tables "
                     "WHERE database='{}' AND name='{}'")
                     .format(schema, table_name))
        else:
            query = (
                "SELECT primary_key FROM system.tables WHERE name='{}'"
            ).format(table_name)

        rows = self._execute(connection, query)
        for r in rows:
            primary_keys = r.primary_key
            if primary_keys:
                return {
                    "constrained_columns": tuple(primary_keys.split(", ")),
                }
        return {}

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

        database = schema if schema else connection.engine.url.database

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

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        if not self.supports_table_comment_reflection:
            raise NotImplementedError()

        if not self.engine_reflection:
            return {}

        database = schema if schema else connection.engine.url.database

        query = text(
            'SELECT comment FROM system.tables '
            'WHERE database = :database AND name = :name'
        )
        comment = self._execute(
            connection, query, database=database, name=table_name, scalar=True
        )
        return {'text': comment or None}

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
