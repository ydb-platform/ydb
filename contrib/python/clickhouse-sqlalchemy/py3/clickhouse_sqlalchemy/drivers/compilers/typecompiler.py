from sqlalchemy.sql import compiler, type_api
from sqlalchemy.sql.ddl import CreateColumn


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

    def visit_int128(self, type_, **kw):
        return 'Int128'

    def visit_uint128(self, type_, **kw):
        return 'UInt128'

    def visit_int256(self, type_, **kw):
        return 'Int256'

    def visit_uint256(self, type_, **kw):
        return 'UInt256'

    def visit_date(self, type_, **kw):
        return 'Date'

    def visit_datetime(self, type_, **kw):
        if type_.timezone:
            return "DateTime('%s')" % type_.timezone
        else:
            return 'DateTime'

    def visit_datetime64(self, type_, **kw):
        if type_.timezone:
            return "DateTime64(%s, '%s')" % (type_.precision, type_.timezone)
        else:
            return 'DateTime64(%s)' % type_.precision

    def visit_float32(self, type_, **kw):
        return 'Float32'

    def visit_float64(self, type_, **kw):
        return 'Float64'

    def visit_numeric(self, type_, **kw):
        return 'Decimal(%s, %s)' % (type_.precision, type_.scale)

    def visit_boolean(self, type_, **kw):
        return 'Bool'

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
