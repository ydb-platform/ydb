try:
    from sqlalchemy.types import Integer
    from sqlalchemy.sql import type_api
    from sqlalchemy.sql.elements import ColumnElement
    from sqlalchemy import util, exc
except ImportError:
    Integer = object
    ColumnElement = object


class UInt32(Integer):
    __visit_name__ = "uint32"


class UInt64(Integer):
    __visit_name__ = "uint64"


class UInt8(Integer):
    __visit_name__ = "uint8"


class Lambda(ColumnElement):

    __visit_name__ = "lambda"

    def __init__(self, func):
        if not util.callable(func):
            raise exc.ArgumentError("func must be callable")

        self.type = type_api.NULLTYPE
        self.func = func
