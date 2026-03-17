"""Generate HTML5 documents directly from Python code."""

__all__ = "Builder", "Document", "E", "HTML"
__version__ = "1.2.0"

from .builder import Builder
from .util import HTML
from .makebuilder import E
from .document import Document
from . import builder, document, makebuilder, util
