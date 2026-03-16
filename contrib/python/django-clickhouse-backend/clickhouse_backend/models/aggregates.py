from django.db.models import aggregates
from django.db.models.expressions import Star

from clickhouse_backend.models.fields import UInt64Field

__all__ = [
    "uniq",
    "uniqExact",
    "uniqCombined",
    "uniqCombined64",
    "uniqHLL12",
    "uniqTheta",
    "anyLast",
    "argMax",
]


class Aggregate(aggregates.Aggregate):
    @property
    def function(self):
        return self.__class__.__name__

    @property
    def name(self):
        return self.__class__.__name__

    def deconstruct(self):
        module_name = self.__module__
        name = self.__class__.__name__
        if module_name.startswith("clickhouse_backend.models.aggregates"):
            module_name = "clickhouse_backend.models"
        return (
            f"{module_name}.{name}",
            self._constructor_args[0],
            self._constructor_args[1],
        )


class uniq(Aggregate):
    output_field = UInt64Field()
    allow_distinct = True
    empty_result_set_value = 0

    def __init__(self, *expressions, distinct=False, filter=None, **extra):
        expressions = [Star() if exp == "*" else exp for exp in expressions]
        super().__init__(*expressions, distinct=distinct, filter=filter, **extra)


class uniqExact(uniq):
    pass


class uniqCombined(uniq):
    pass


class uniqCombined64(uniq):
    pass


class uniqHLL12(uniq):
    pass


class uniqTheta(uniq):
    pass


class anyLast(Aggregate):
    pass


class argMax(Aggregate):
    arity = 2

    def _resolve_output_field(self):
        return self.get_source_fields()[0]
