from contextlib import contextmanager
import abc

import pg8000.dbapi

from utils.settings import Settings


class Client:
    # database name -> pool
    settings: Settings.PostgreSQL

    def __init__(self, settings: Settings.PostgreSQL):
        self.settings = settings
        self.pools = dict()

    @contextmanager
    def get_cursor(self, dbname: str):
        conn = pg8000.dbapi.Connection(
            user=self.settings.username,
            password=self.settings.password,
            host=self.settings.host,
            port=self.settings.port,
            database=dbname,
        )
        conn.autocommit = True

        cur = conn.cursor()
        yield conn, cur


class Type(abc.ABC):
    @abc.abstractmethod
    def to_sql(self) -> str:
        pass


class PrimitiveType(Type):
    def to_sql(self):
        return type(self).__name__.lower()


class Boolean(PrimitiveType):
    pass


class Bool(PrimitiveType):
    pass


class SmallInt(PrimitiveType):
    pass


class Int2(PrimitiveType):
    pass


class SmallSerial(PrimitiveType):
    pass


class Serial2(PrimitiveType):
    pass


class Integer(PrimitiveType):
    pass


class Int(PrimitiveType):
    pass


class Int4(PrimitiveType):
    pass


class Serial(PrimitiveType):
    pass


class Serial4(PrimitiveType):
    pass


class BigInt(PrimitiveType):
    pass


class Int8(PrimitiveType):
    pass


class BigSerial(PrimitiveType):
    pass


class Serial8(PrimitiveType):
    pass


class Real(PrimitiveType):
    pass


class Float4(PrimitiveType):
    pass


class DoublePrecision(PrimitiveType):
    def to_sql(self):
        return 'double precision'


class Float8(PrimitiveType):
    pass


class Bytea(PrimitiveType):
    pass


class Character(PrimitiveType):
    def to_sql(self):
        return 'character (5)'


class CharacterVarying(PrimitiveType):
    def to_sql(self):
        return 'character varying (5)'


class Text(PrimitiveType):
    pass


class TimestampWithoutTimeZone(PrimitiveType):
    def to_sql(self):
        return 'timestamp without time zone'


class Date(PrimitiveType):
    pass


class Time(PrimitiveType):
    pass
