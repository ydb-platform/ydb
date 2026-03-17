from .bit import FixedBitField
from .halfvec import HalfVectorField
from .sparsevec import SparseVectorField
from .vector import VectorField

# TODO remove
from .. import HalfVector, SparseVector

__all__ = [
    'VectorField',
    'HalfVectorField',
    'FixedBitField',
    'SparseVectorField',
    'HalfVector',
    'SparseVector'
]
