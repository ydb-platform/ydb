from django.db.models import FloatField, Func, Value
from .. import Vector, HalfVector, SparseVector


class DistanceBase(Func):
    output_field = FloatField()

    def __init__(self, expression, vector, **extra):
        if not hasattr(vector, 'resolve_expression'):
            if isinstance(vector, HalfVector):
                vector = Value(HalfVector._to_db(vector))
            elif isinstance(vector, SparseVector):
                vector = Value(SparseVector._to_db(vector))
            else:
                vector = Value(Vector._to_db(vector))

            # prevent error with unhashable types
            self._constructor_args = ((expression, vector), extra)

        super().__init__(expression, vector, **extra)


class BitDistanceBase(Func):
    output_field = FloatField()

    def __init__(self, expression, vector, **extra):
        if not hasattr(vector, 'resolve_expression'):
            vector = Value(vector)
        super().__init__(expression, vector, **extra)


class L2Distance(DistanceBase):
    function = ''
    arg_joiner = ' <-> '


class MaxInnerProduct(DistanceBase):
    function = ''
    arg_joiner = ' <#> '


class CosineDistance(DistanceBase):
    function = ''
    arg_joiner = ' <=> '


class L1Distance(DistanceBase):
    function = ''
    arg_joiner = ' <+> '


class HammingDistance(BitDistanceBase):
    function = ''
    arg_joiner = ' <~> '


class JaccardDistance(BitDistanceBase):
    function = ''
    arg_joiner = ' <%%> '
