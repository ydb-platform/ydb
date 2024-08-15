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

class Tinyint(PrimitiveType):
    pass

class Smallint(PrimitiveType):
    pass

class Int(PrimitiveType):
    pass

class Bigint(PrimitiveType):
    pass

class Real(PrimitiveType):
    pass

class Float(PrimitiveType):
    pass

class Binary(PrimitiveType):
    pass

class Varbinary(PrimitiveType):
    pass

class Image(PrimitiveType):
    pass

class Char(PrimitiveType):
    pass

class Varchar(PrimitiveType):
    pass

class Text(PrimitiveType):
    pass

class NChar(PrimitiveType):
    pass

class NVarchar(PrimitiveType):
    pass

class NText(PrimitiveType):
    pass

class Date(PrimitiveType):
    pass

class Smalldatetime(PrimitiveType):
    pass

class Datetime(PrimitiveType):
    pass

class Datetime2(PrimitiveType):
    pass
