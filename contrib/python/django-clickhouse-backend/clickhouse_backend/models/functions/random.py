from clickhouse_backend.models import fields

from .base import Func

__all__ = [
    "Rand",
]


class Rand(Func):
    arity = 0
    output_field = fields.UInt32Field()
