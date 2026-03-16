from peewee import Expression, Field


class FixedBitField(Field):
    field_type = 'bit'

    def __init__(self, max_length=None, *args, **kwargs):
        self.max_length = max_length
        super(FixedBitField, self).__init__(*args, **kwargs)

    def get_modifiers(self):
        return self.max_length and [self.max_length] or None

    def _distance(self, op, vector):
        return Expression(lhs=self, op=op, rhs=self.to_value(vector))

    def hamming_distance(self, vector):
        return self._distance('<~>', vector)

    def jaccard_distance(self, vector):
        return self._distance('<%%>', vector)
