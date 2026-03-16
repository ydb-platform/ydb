from django.db import models

from clickhouse_backend.models import fields
from clickhouse_backend.utils.timezone import get_timezone

from .base import Func

__all__ = [
    "toStartOfMinute",
    "toStartOfFiveMinutes",
    "toStartOfTenMinutes",
    "toStartOfFifteenMinutes",
    "toStartOfHour",
    "toYYYYMM",
    "toYYYYMMDD",
    "toYYYYMMDDhhmmss",
    "toYearWeek",
]


class toYYYYMM(Func):
    output_field = fields.UInt32Field()

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
        if arity == 2 and isinstance(expressions[1], str):
            expressions = (expressions[0], models.Value(expressions[1]))
        else:
            expressions = (expressions[0], models.Value(get_timezone()))

        super().__init__(*expressions)


class toYYYYMMDD(toYYYYMM):
    pass


class toYYYYMMDDhhmmss(toYYYYMM):
    output_field = fields.UInt64Field()


class toStartOfMinute(Func):
    output_field = models.fields.DateTimeField()

    def __init__(self, *expressions):
        arity = len(expressions)
        if arity < 1 or arity > 1:
            raise TypeError(
                "'%s' takes 1 argument (%s given)"
                % (
                    self.__class__.__name__,
                    len(expressions),
                )
            )

        super().__init__(*expressions)


class toStartOfFiveMinutes(toStartOfMinute):
    pass


class toStartOfTenMinutes(toStartOfMinute):
    pass


class toStartOfFifteenMinutes(toStartOfMinute):
    pass


class toStartOfHour(toStartOfMinute):
    pass


class toYearWeek(Func):
    output_field = fields.UInt32Field()

    def __init__(self, *expressions):
        arity = len(expressions)
        if arity < 1 or arity > 3:
            raise TypeError(
                "'%s' takes between 1 and 3 arguments (%s given)"
                % (
                    self.__class__.__name__,
                    len(expressions),
                )
            )

        expressions = (
            expressions[0],
            *(models.Value(expr) for expr in expressions[1:]),
        )

        super().__init__(*expressions)
