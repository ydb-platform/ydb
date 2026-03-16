from django.db.models import Value

from clickhouse_backend.models import fields

from .base import Func

__all__ = [
    "currentDatabase",
    "hostName",
    "generateSerialID",
]


class currentDatabase(Func):
    arity = 0
    output_field = fields.StringField()


class hostName(Func):
    arity = 0
    output_field = fields.StringField()


class generateSerialID(Func):
    """
    https://clickhouse.com/docs/sql-reference/functions/other-functions#generateSerialID
    """

    output_field = fields.UInt64Field()

    def __init__(self, *expressions):
        arity = len(expressions)
        if arity < 1 or arity > 2:
            raise TypeError(
                "'%s' takes 1 or 2 arguments (%s given)"
                % (
                    self.__class__.__name__,
                    len(expressions),
                )
            )

        if isinstance(expressions[0], str):
            expressions = (Value(expressions[0]), *expressions[1:])
        super().__init__(*expressions)
