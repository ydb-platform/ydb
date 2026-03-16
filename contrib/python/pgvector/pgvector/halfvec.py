import numpy as np
from struct import pack, unpack_from


class HalfVector:
    def __init__(self, value):
        # asarray still copies if same dtype
        if not isinstance(value, np.ndarray) or value.dtype != '>f2':
            value = np.asarray(value, dtype='>f2')

        if value.ndim != 1:
            raise ValueError('expected ndim to be 1')

        self._value = value

    def __repr__(self):
        return f'HalfVector({self.to_list()})'

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return np.array_equal(self.to_numpy(), other.to_numpy())
        return False

    def dimensions(self):
        return len(self._value)

    def to_list(self):
        return self._value.tolist()

    def to_numpy(self):
        return self._value

    def to_text(self):
        return '[' + ','.join([str(float(v)) for v in self._value]) + ']'

    def to_binary(self):
        return pack('>HH', self.dimensions(), 0) + self._value.tobytes()

    @classmethod
    def from_text(cls, value):
        return cls([float(v) for v in value[1:-1].split(',')])

    @classmethod
    def from_binary(cls, value):
        dim, unused = unpack_from('>HH', value)
        return cls(np.frombuffer(value, dtype='>f2', count=dim, offset=4))

    @classmethod
    def _to_db(cls, value, dim=None):
        if value is None:
            return value

        if not isinstance(value, cls):
            value = cls(value)

        if dim is not None and value.dimensions() != dim:
            raise ValueError('expected %d dimensions, not %d' % (dim, value.dimensions()))

        return value.to_text()

    @classmethod
    def _to_db_binary(cls, value):
        if value is None:
            return value

        if not isinstance(value, cls):
            value = cls(value)

        return value.to_binary()

    @classmethod
    def _from_db(cls, value):
        if value is None or isinstance(value, cls):
            return value

        return cls.from_text(value)

    @classmethod
    def _from_db_binary(cls, value):
        if value is None or isinstance(value, cls):
            return value

        return cls.from_binary(value)
