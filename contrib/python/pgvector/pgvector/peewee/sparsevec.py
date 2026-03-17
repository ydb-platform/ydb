from peewee import Expression, Field
from .. import SparseVector


class SparseVectorField(Field):
    field_type = 'sparsevec'

    def __init__(self, dimensions=None, *args, **kwargs):
        self.dimensions = dimensions
        super(SparseVectorField, self).__init__(*args, **kwargs)

    def get_modifiers(self):
        return self.dimensions and [self.dimensions] or None

    def db_value(self, value):
        return SparseVector._to_db(value)

    def python_value(self, value):
        return SparseVector._from_db(value)

    def _distance(self, op, vector):
        return Expression(lhs=self, op=op, rhs=self.to_value(vector))

    def l2_distance(self, vector):
        return self._distance('<->', vector)

    def max_inner_product(self, vector):
        return self._distance('<#>', vector)

    def cosine_distance(self, vector):
        return self._distance('<=>', vector)

    def l1_distance(self, vector):
        return self._distance('<+>', vector)
