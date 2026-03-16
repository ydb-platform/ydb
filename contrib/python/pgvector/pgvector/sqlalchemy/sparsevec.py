from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.types import UserDefinedType, Float, String
from .. import SparseVector


class SPARSEVEC(UserDefinedType):
    cache_ok = True
    _string = String()

    def __init__(self, dim=None):
        super(UserDefinedType, self).__init__()
        self.dim = dim

    def get_col_spec(self, **kw):
        if self.dim is None:
            return 'SPARSEVEC'
        return 'SPARSEVEC(%d)' % self.dim

    def bind_processor(self, dialect):
        def process(value):
            return SparseVector._to_db(value, self.dim)
        return process

    def literal_processor(self, dialect):
        string_literal_processor = self._string._cached_literal_processor(dialect)

        def process(value):
            return string_literal_processor(SparseVector._to_db(value, self.dim))
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return SparseVector._from_db(value)
        return process

    class comparator_factory(UserDefinedType.Comparator):
        def l2_distance(self, other):
            return self.op('<->', return_type=Float)(other)

        def max_inner_product(self, other):
            return self.op('<#>', return_type=Float)(other)

        def cosine_distance(self, other):
            return self.op('<=>', return_type=Float)(other)

        def l1_distance(self, other):
            return self.op('<+>', return_type=Float)(other)


# for reflection
ischema_names['sparsevec'] = SPARSEVEC
