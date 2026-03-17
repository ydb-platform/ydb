from sqlalchemy.sql.type_api import to_instance
from sqlalchemy import types


class ClickHouseTypeEngine(types.TypeEngine):
    def compile(self, dialect=None):
        from clickhouse_sqlalchemy.drivers.base import clickhouse_dialect

        return super(ClickHouseTypeEngine, self).compile(
            dialect=clickhouse_dialect
        )


class String(types.String, ClickHouseTypeEngine):
    pass


class Int(types.Integer, ClickHouseTypeEngine):
    pass


class Float(types.Float, ClickHouseTypeEngine):
    pass


class Array(ClickHouseTypeEngine):
    __visit_name__ = 'array'

    def __init__(self, item_type):
        self.item_type = item_type
        self.item_type_impl = to_instance(item_type)
        super(Array, self).__init__()

    def literal_processor(self, dialect):
        item_processor = self.item_type_impl.literal_processor(dialect)

        def process(value):
            processed_value = []
            for x in value:
                if item_processor:
                    x = item_processor(x)
                processed_value.append(x)
            return '[' + ', '.join(processed_value) + ']'
        return process


class Nullable(ClickHouseTypeEngine):
    __visit_name__ = 'nullable'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(Nullable, self).__init__()


class UUID(String):
    __visit_name__ = 'uuid'


class LowCardinality(ClickHouseTypeEngine):
    __visit_name__ = 'lowcardinality'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(LowCardinality, self).__init__()


class Int8(Int):
    __visit_name__ = 'int8'


class UInt8(Int):
    __visit_name__ = 'uint8'


class Int16(Int):
    __visit_name__ = 'int16'


class UInt16(Int):
    __visit_name__ = 'uint16'


class Int32(Int):
    __visit_name__ = 'int32'


class UInt32(Int):
    __visit_name__ = 'uint32'


class Int64(Int):
    __visit_name__ = 'int64'


class UInt64(Int):
    __visit_name__ = 'uint64'


class Float32(Float):
    __visit_name__ = 'float32'


class Float64(Float):
    __visit_name__ = 'float64'


class Date(types.Date, ClickHouseTypeEngine):
    __visit_name__ = 'date'


class DateTime(types.Date, ClickHouseTypeEngine):
    __visit_name__ = 'datetime'


class DateTime64(DateTime, ClickHouseTypeEngine):
    __visit_name__ = 'datetime64'

    def __init__(self, precision=3, timezone=None):
        self.precision = precision
        self.timezone = timezone
        super(DateTime64, self).__init__()


class Enum(types.Enum, ClickHouseTypeEngine):
    __visit_name__ = 'enum'

    def __init__(self, *enums, **kw):
        if not enums:
            enums = kw.get('_enums', ())  # passed as keyword

        super(Enum, self).__init__(*enums, **kw)


class Enum8(Enum):
    __visit_name__ = 'enum8'


class Enum16(Enum):
    __visit_name__ = 'enum16'


class Decimal(types.Numeric, ClickHouseTypeEngine):
    __visit_name__ = 'numeric'


class Tuple(ClickHouseTypeEngine):
    __visit_name__ = 'tuple'

    def __init__(self, *nested_types):
        self.nested_types = nested_types
        super(Tuple, self).__init__()


class Map(ClickHouseTypeEngine):
    __visit_name__ = 'map'

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
        super(Map, self).__init__()
