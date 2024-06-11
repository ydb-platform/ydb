import abc


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__

    @classmethod
    def to_non_nullable(cls):
        return NonNullable(cls())


class Bool(PrimitiveType):
    pass


class Int8(PrimitiveType):
    pass


class Int16(PrimitiveType):
    pass


class Int32(PrimitiveType):
    pass


class Int64(PrimitiveType):
    pass


class Uint8(PrimitiveType):
    pass


class Uint16(PrimitiveType):
    pass


class Uint32(PrimitiveType):
    pass


class Uint64(PrimitiveType):
    pass


class Float(PrimitiveType):
    pass


class Double(PrimitiveType):
    pass


class String(PrimitiveType):
    pass


class Utf8(PrimitiveType):
    pass


class Date(PrimitiveType):
    pass


class Datetime(PrimitiveType):
    pass


class Timestamp(PrimitiveType):
    pass


class Interval(PrimitiveType):
    pass


class Json(PrimitiveType):
    pass


class NonNullable(Type):
    '''
    See https://ydb.tech/docs/ru/yql/reference/types/optional#tipy-dannyh,-dopuskayushie-znachenie-null
    '''

    primitive: PrimitiveType

    def __init__(self, primitive: PrimitiveType):
        self.primitive = primitive

    def to_sql(self) -> str:
        return f'{self.primitive.to_sql()} NOT NULL'
