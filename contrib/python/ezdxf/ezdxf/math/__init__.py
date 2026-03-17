# Copyright (c) 2011-2024, Manfred Moitzi
# License: MIT License
from typing import Iterable
# Using * imports to simplify namespace imports, therefore every module
# has to have an export declaration: __all__ = [...]

# Import base types as C-extensions if available:
from ._ctypes import *
# Everything else are pure Python imports:
from .construct2d import *
from .construct3d import *
from .parametrize import *
from .bspline import *
from .bezier import *
from .bezier_interpolation import *
from .eulerspiral import *
from .ucs import *
from .bulge import *
from .arc import *
from .line import *
from .circle import *
from .ellipse import *
from .box import *
from .shape import *
from .bbox import *
from .offset2d import *
from .transformtools import *
from .curvetools import *
from .polyline import *

ABS_TOL = 1e-12
REL_TOL = 1e-9


def close_vectors(a: Iterable[AnyVec], b: Iterable[UVec], *,
                  rel_tol=REL_TOL, abs_tol=ABS_TOL) -> bool:
    return all(v1.isclose(v2, rel_tol=rel_tol, abs_tol=abs_tol)
               for v1, v2 in zip(a, b))


def xround(value: float, rounding: float = 0.) -> float:
    """Extended rounding function.

    The argument `rounding` defines the rounding limit:

    ======= ======================================
    0       remove fraction
    0.1     round next to x.1, x.2, ... x.0
    0.25    round next to x.25, x.50, x.75 or x.00
    0.5     round next to x.5 or x.0
    1.0     round to a multiple of 1: remove fraction
    2.0     round to a multiple of 2: xxx2, xxx4, xxx6 ...
    5.0     round to a multiple of 5: xxx5 or xxx0
    10.0    round to a multiple of 10: xx10, xx20, ...
    ======= ======================================

    Args:
        value: float value to round
        rounding: rounding limit

    """
    if rounding == 0:
        return round(value)
    factor = 1. / rounding
    return round(value * factor) / factor
