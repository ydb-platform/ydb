import abc


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__.upper()


class Number(PrimitiveType):
    # self.p = 38
    # self.s = 0

    # def to_sql(self):
    #     return f'DECIMAL({self.p},{self.s})'
    pass


class Json(PrimitiveType):
    pass


class Decimal(PrimitiveType):
    pass


class BinaryFloat(PrimitiveType):
    def to_sql(self):
        return 'BINARY_FLOAT'


class BinaryDouble(PrimitiveType):
    def to_sql(self):
        return 'BINARY_DOUBLE'


class VarChar(PrimitiveType):
    pass


class VarChar2(PrimitiveType):
    pass


class NVarChar2(PrimitiveType):
    pass


class Char(PrimitiveType):
    pass


class NChar(PrimitiveType):
    pass


class CLob(PrimitiveType):
    pass


class NCLob(PrimitiveType):
    pass


class Raw(PrimitiveType):
    pass


class Blob(PrimitiveType):
    pass


class Date(PrimitiveType):
    pass


class Timestamp(PrimitiveType):
    pass


class TimestampWTZ(PrimitiveType):
    def to_sql(self):
        return 'TIMESTAMP WITH TIME ZONE'


class TimestampWLocalTZ(PrimitiveType):
    def to_sql(self):
        return 'TIMESTAMP WITH LOCAL TIME ZONE'


class Long(PrimitiveType):
    pass


class LongRaw(PrimitiveType):
    def to_sql(self):
        return 'LONG RAW'
