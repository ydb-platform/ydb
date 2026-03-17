from typing import TYPE_CHECKING

import numpy

from .. import registry
from .numpy_ops import NumpyOps
from .ops import Ops

if TYPE_CHECKING:
    # Type checking does not work with dynamic base classes, since MyPy cannot
    # determine against which base class to check. So, always derive from Ops
    # during type checking.
    _Ops = Ops
else:
    try:
        from thinc_apple_ops import AppleOps

        _Ops = AppleOps
    except ImportError:
        _Ops = NumpyOps


@registry.ops("MPSOps")
class MPSOps(_Ops):
    """Ops class for Metal Performance shaders."""

    name = "mps"
    xp = numpy
