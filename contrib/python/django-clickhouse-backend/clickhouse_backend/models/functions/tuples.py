from django.db.models.expressions import Value

from clickhouse_backend.models import fields

from .base import Func

__all__ = [
    "Tuple",
    "tupleElement",
]


class Tuple(Func):
    function = "tuple"

    def _resolve_output_field(self):
        """
        Attempt to infer the output type of the expression.
        """

        sources_iter = (
            source for source in self.get_source_fields() if source is not None
        )
        return fields.TupleField(tuple(sources_iter))


class tupleElement(Func):
    def __init__(self, *expressions, output_field=None, **extra):
        if len(expressions) == 2:
            expressions = (
                expressions[0],
                Value(expressions[1] + 1)
                if isinstance(expressions[1], int)
                else expressions[1],
            )
        elif len(expressions) == 3:
            expressions = (
                expressions[0],
                Value(expressions[1] + 1)
                if isinstance(expressions[1], int)
                else expressions[1],
                expressions[2],
            )
        else:
            raise TypeError(
                "'%s' takes 2 or 3 arguments (%s given)"
                % (
                    self.__class__.__name__,
                    len(expressions),
                )
            )
        super().__init__(*expressions, output_field=output_field, **extra)
