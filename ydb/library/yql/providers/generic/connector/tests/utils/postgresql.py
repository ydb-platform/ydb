from contextlib import contextmanager
import abc
import time
from datetime import datetime
from typing import Tuple
import sys

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
        conn, cursor = self._make_cursor(dbname=dbname)
        yield conn, cursor
        cursor.close()
        conn.close()

    def _make_cursor(self, dbname: str) -> Tuple[pg8000.dbapi.Connection, pg8000.dbapi.Cursor]:
        start = datetime.now()
        attempt = 0

        while (datetime.now() - start).total_seconds() < 10:
            attempt += 1
            try:
                sys.stdout.write(
                    f"Trying to connect PostgreSQL: {self.settings.host_external}:{self.settings.port_external}\n"
                )
                conn = pg8000.dbapi.Connection(
                    user=self.settings.username,
                    password=self.settings.password,
                    host=self.settings.host_external,
                    port=self.settings.port_external,
                    database=dbname,
                    timeout=10,
                )
                conn.autocommit = True

                cur = conn.cursor()
                return conn, cur
            except Exception as e:
                sys.stderr.write(f"attempt #{attempt} failed: {e} {e.args}\n")
                time.sleep(3)
                continue

        ss = self.settings
        params = f'{ss.username} {ss.password} {ss.host_external} {ss.port_external} {dbname}'
        raise Exception(f"Failed to connect PostgreSQL in {attempt} attempt(s) with params: {params}")


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
