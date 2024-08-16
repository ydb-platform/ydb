import abc


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__.lower()


class Bool(PrimitiveType):
    pass


class TinyInt(PrimitiveType):
    pass


class TinyIntUnsigned(PrimitiveType):
    pass


class SmallInt(PrimitiveType):
    pass


class SmallIntUnsigned(PrimitiveType):
    pass


class MediumInt(PrimitiveType):
    pass


class MediumIntUnsigned(PrimitiveType):
    pass


class Integer(PrimitiveType):
    pass


class IntegerUnsigned(PrimitiveType):
    pass


class BigInt(PrimitiveType):
    pass


class BigIntUnsigned(PrimitiveType):
    pass


class Float(PrimitiveType):
    pass


class Real(PrimitiveType):
    pass


class Double(PrimitiveType):
    pass


class Date(PrimitiveType):
    pass


class Datetime(PrimitiveType):
    pass


class Timestamp(PrimitiveType):
    pass


class TinyBlob(PrimitiveType):
    pass


class Blob(PrimitiveType):
    pass


class MediumBlob(PrimitiveType):
    pass


class LongBlob(PrimitiveType):
    pass


class TinyText(PrimitiveType):
    pass


class Text(PrimitiveType):
    pass


class MediumText(PrimitiveType):
    pass


class LongText(PrimitiveType):
    pass


class Char(PrimitiveType):
    pass


class VarChar(PrimitiveType):
    pass


class Binary(PrimitiveType):
    pass


class VarBinary(PrimitiveType):
    pass


class Json(PrimitiveType):
    pass
