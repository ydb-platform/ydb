import abc


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__

    @classmethod
    def to_nullable(cls):
        return Nullable(cls())


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


class UInt8(PrimitiveType):
    pass


class UInt16(PrimitiveType):
    pass


class UInt32(PrimitiveType):
    pass


class UInt64(PrimitiveType):
    pass


class Float(PrimitiveType):
    pass


class Double(PrimitiveType):
    pass


class String(PrimitiveType):
    pass


class FixedString(PrimitiveType):
    def to_sql(self) -> str:
        return "FixedString(5)"


class Nullable(Type):
    primitive: PrimitiveType

    def __init__(self, primitive: PrimitiveType):
        self.primitive = primitive

    def to_sql(self) -> str:
        return f'Nullable({self.primitive.to_sql()})'
