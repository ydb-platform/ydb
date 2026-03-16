import numpy as np
from struct import pack, unpack_from

NO_DEFAULT = object()


class SparseVector:
    def __init__(self, value, dimensions=NO_DEFAULT, /):
        if value.__class__.__module__.startswith('scipy.sparse.'):
            if dimensions is not NO_DEFAULT:
                raise ValueError('extra argument')

            self._from_sparse(value)
        elif isinstance(value, dict):
            if dimensions is NO_DEFAULT:
                raise ValueError('missing dimensions')

            self._from_dict(value, dimensions)
        else:
            if dimensions is not NO_DEFAULT:
                raise ValueError('extra argument')

            self._from_dense(value)

    def __repr__(self):
        elements = dict(zip(self._indices, self._values))
        return f'SparseVector({elements}, {self._dim})'

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dimensions() == other.dimensions() and self.indices() == other.indices() and self.values() == other.values()
        return False

    def dimensions(self):
        return self._dim

    def indices(self):
        return self._indices

    def values(self):
        return self._values

    def to_coo(self):
        from scipy.sparse import coo_array

        coords = ([0] * len(self._indices), self._indices)
        return coo_array((self._values, coords), shape=(1, self._dim))

    def to_list(self):
        vec = [0.0] * self._dim
        for i, v in zip(self._indices, self._values):
            vec[i] = v
        return vec

    def to_numpy(self):
        vec = np.repeat(0.0, self._dim).astype(np.float32)
        for i, v in zip(self._indices, self._values):
            vec[i] = v
        return vec

    def to_text(self):
        return '{' + ','.join([f'{int(i) + 1}:{float(v)}' for i, v in zip(self._indices, self._values)]) + '}/' + str(int(self._dim))

    def to_binary(self):
        nnz = len(self._indices)
        return pack(f'>iii{nnz}i{nnz}f', self._dim, nnz, 0, *self._indices, *self._values)

    def _from_dict(self, d, dim):
        elements = [(i, v) for i, v in d.items() if v != 0]
        elements.sort()

        self._dim = int(dim)
        self._indices = [int(v[0]) for v in elements]
        self._values = [float(v[1]) for v in elements]

    def _from_sparse(self, value):
        value = value.tocoo()

        if value.ndim == 1:
            self._dim = value.shape[0]
        elif value.ndim == 2 and value.shape[0] == 1:
            self._dim = value.shape[1]
        else:
            raise ValueError('expected ndim to be 1')

        if hasattr(value, 'coords'):
            # scipy 1.13+
            self._indices = value.coords[-1].tolist()
        else:
            self._indices = value.col.tolist()
        self._values = value.data.tolist()

    def _from_dense(self, value):
        self._dim = len(value)
        self._indices = [i for i, v in enumerate(value) if v != 0]
        self._values = [float(value[i]) for i in self._indices]

    @classmethod
    def from_text(cls, value):
        elements, dim = value.split('/', 2)
        indices = []
        values = []
        # split on empty string returns single element list
        if len(elements) > 2:
            for e in elements[1:-1].split(','):
                i, v = e.split(':', 2)
                indices.append(int(i) - 1)
                values.append(float(v))
        return cls._from_parts(int(dim), indices, values)

    @classmethod
    def from_binary(cls, value):
        dim, nnz, unused = unpack_from('>iii', value)
        indices = unpack_from(f'>{nnz}i', value, 12)
        values = unpack_from(f'>{nnz}f', value, 12 + nnz * 4)
        return cls._from_parts(int(dim), list(indices), list(values))

    @classmethod
    def _from_parts(cls, dim, indices, values):
        vec = cls.__new__(cls)
        vec._dim = dim
        vec._indices = indices
        vec._values = values
        return vec

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
