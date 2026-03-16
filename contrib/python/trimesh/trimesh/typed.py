from io import BufferedRandom, BytesIO, StringIO
from pathlib import Path
from sys import version_info
from typing import (
    IO,
    Any,
    BinaryIO,
    Literal,
    Optional,
    TextIO,
    Union,
)

from numpy import float64, floating, int64, integer, unsignedinteger

# requires numpy>=1.20
from numpy.typing import ArrayLike, DTypeLike, NDArray

if version_info >= (3, 9):
    # use PEP585 hints on newer python
    List = list
    Tuple = tuple
    Dict = dict
    Set = set
    from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
else:
    from typing import (
        Callable,
        Dict,
        Hashable,
        Iterable,
        List,
        Mapping,
        Sequence,
        Set,
        Tuple,
    )

if version_info >= (3, 11):
    from typing import Self
else:
    Self = Any

# most loader routes take `file_obj` which can either be
# a file-like object or a file path, or sometimes a dict
Stream = Union[IO, BytesIO, StringIO, BinaryIO, TextIO, BufferedRandom]
Loadable = Union[str, Path, Stream, Dict, None]

# numpy integers do not inherit from python integers, i.e.
# if you type a function argument as an `int` and then pass
# a value from a numpy array like `np.ones(10, dtype=np.int64)[0]`
# you may have a type error.
# these wrappers union numpy integers and python integers
Integer = Union[int, integer, unsignedinteger]

# Numbers which can only be floats and will not accept integers
# > isinstance(np.ones(1, dtype=np.float32)[0], floating) # True
# > isinstance(np.ones(1, dtype=np.float32)[0], float) # False
Floating = Union[float, floating]

# Many arguments take "any valid number" and don't care if it
# is an integer or a floating point input.
Number = Union[Floating, Integer]

# the literals for specifying what viewer to use
ViewerType = Union[None, Callable, Literal["gl", "jupyter", "marimo"]]

# literal for color maps we include in the library
ColorMapType = Literal["viridis", "magma", "inferno", "plasma"]

# the literal for what graph backend engines are available
GraphEngineType = Literal["networkx", "scipy", None]

# what 3D boolean engines are available
BooleanEngineType = Literal["manifold", "blender", None]
# what 3D boolean operations can be passed to boolean functions
BooleanOperationType = Literal["difference", "union", "intersection"]

# what are the supported methods for converting a mesh into voxels.
VoxelizationMethodsType = Literal["subdivide", "ray", "binvox"]

__all__ = [
    "IO",
    "Any",
    "ArrayLike",
    "BinaryIO",
    "Callable",
    "DTypeLike",
    "Dict",
    "Hashable",
    "Integer",
    "Iterable",
    "List",
    "Literal",
    "Loadable",
    "Mapping",
    "NDArray",
    "Number",
    "Optional",
    "Self",
    "Sequence",
    "Set",
    "Stream",
    "Tuple",
    "ViewerType",
    "float64",
    "int64",
]
