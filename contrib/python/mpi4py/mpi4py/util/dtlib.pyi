from __future__ import annotations
from ..MPI import Datatype
from numpy import dtype
from numpy.typing import DTypeLike

def from_numpy_dtype(dtype: DTypeLike) -> Datatype: ...
def to_numpy_dtype(datatype: Datatype) -> dtype: ...
