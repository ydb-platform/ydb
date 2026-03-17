# Necessary for some side-effects in Cython. Not sure I understand.
import numpy

from .about import __version__
from .config import registry

# fmt: off
__all__ = [
    "registry",
    "__version__",
]
# fmt: on
