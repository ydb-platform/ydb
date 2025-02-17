import abc


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__.lower()


class Bit(PrimitiveType):
    pass


class TinyInt(PrimitiveType):
    pass


class SmallInt(PrimitiveType):
    pass


class Int(PrimitiveType):
    pass


class BigInt(PrimitiveType):
    pass


class Real(PrimitiveType):
    pass


class Float(PrimitiveType):
    pass


class Binary(PrimitiveType):
    pass


class VarBinary(PrimitiveType):
    pass


class Image(PrimitiveType):
    pass


class Char(PrimitiveType):
    pass


class VarChar(PrimitiveType):
    pass


class Text(PrimitiveType):
    pass


class NChar(PrimitiveType):
    pass


class NVarChar(PrimitiveType):
    pass


class NText(PrimitiveType):
    pass


class Date(PrimitiveType):
    pass


class SmallDatetime(PrimitiveType):
    pass


class Datetime(PrimitiveType):
    pass


class Datetime2(PrimitiveType):
    pass
