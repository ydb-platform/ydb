import numpy as np


class Record(object):
    __attributes__ = []

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and all(
                (getattr(self, _) == getattr(other, _))
                for _ in self.__attributes__
            )
        )

    def __ne__(self, other):
        return not self == other

    def __iter__(self):
        return (getattr(self, _) for _ in self.__attributes__)

    def __hash__(self):
        return hash(tuple(self))

    def __repr__(self):
        name = self.__class__.__name__
        args = ', '.join(
            '{key}={value!r}'.format(
                key=_,
                value=getattr(self, _)
            )
            for _ in self.__attributes__
        )
        return '{name}({args})'.format(
            name=name,
            args=args
        )

    def _repr_pretty_(self, printer, cycle):
        name = self.__class__.__name__
        if cycle:
            printer.text('{name}(...)'.format(name=name))
        else:
            printer.text('{name}('.format(name=name))
            keys = self.__attributes__
            size = len(keys)
            if size:
                with printer.indent(4):
                    printer.break_()
                    for index, key in enumerate(keys):
                        printer.text(key + '=')
                        value = getattr(self, key)
                        printer.pretty(value)
                        if index < size - 1:
                            printer.text(',')
                            printer.break_()
                printer.break_()
            printer.text(')')


class PQ(Record):
    __attributes__ = ['vectors', 'dim', 'qdim', 'centroids', 'indexes', 'codes']

    def __init__(self, vectors, dim, qdim, centroids, indexes, codes):
        """
        :param vectors: number of rows
        :param dim: dimensionality of decompressed vectors
        :param qdim: dimensionality of compressed vectors
        :param centroids: number of centroids
        :param indexes: compressed vectors
        :param codes: code words of the PQ index
        """
        self.vectors = vectors
        self.dim = dim
        self.qdim = qdim
        self.centroids = centroids
        self.indexes = indexes
        self.codes = codes
        self.qdims = np.arange(self.qdim)

    def __getitem__(self, id):
        indexes = self.indexes[id]
        parts = self.codes[self.qdims, indexes]
        if len(parts.shape) == 2:
            return parts.reshape(self.dim)
        else:  # the index has been list or slice
            return parts.reshape(parts.shape[0], self.dim)

    def __add__(self, other):
        return self.unpack() + other

    def __sub__(self, other):
        return self.unpack() - other

    def __mul__(self, other):
        return self.unpack() * other

    def __truediv__(self, other):
        return self.unpack() / other

    def __pow__(self, other):
        return self.unpack() ** other

    def sqrt(self):
        return self ** 0.5

    def unpack(self):
        parts = self.codes[self.qdims, self.indexes]
        return parts.reshape(self.vectors, self.dim)

    def sampled(self, ids):
        vectors = len(ids)
        indexes = self.indexes[ids]
        return PQ(
            vectors, self.dim, self.qdim, self.centroids,
            indexes, self.codes
        )

    @property
    def shape(self):
        return self.vectors, self.dim

    @property
    def dtype(self):
        return self.codes.dtype

    @property
    def as_bytes(self):
        meta = self.vectors, self.dim, self.qdim, self.centroids
        meta = np.array(meta).astype(np.uint32).tobytes()
        indexes = self.indexes.astype(self.index_type(self.centroids)).tobytes()
        codes = self.codes.astype(np.float32).tobytes()
        return meta + indexes + codes

    @classmethod
    def from_file(cls, file):
        buffer = file.read(4 * 4)
        vectors, dim, qdim, centroids = np.frombuffer(buffer, np.uint32)
        buffer = file.read(vectors * qdim)
        indexes = np.frombuffer(buffer, cls.index_type(centroids)).reshape(vectors, qdim)
        buffer = file.read()
        codes = np.frombuffer(buffer, np.float32).reshape(qdim, centroids, -1)
        return cls(vectors, dim, qdim, centroids, indexes, codes)

    @classmethod
    def index_type(cls, centroids):
        return np.uint8 if centroids <= 255 else np.uint16

    def __len__(self):
        return self.vectors
