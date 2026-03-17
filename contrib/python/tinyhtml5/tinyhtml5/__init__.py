"""HTML parsing library based on the WHATWG HTML specification.

The parser is designed to be compatible with existing HTML found in the wild
and implements well-defined error recovery that is largely compatible with
modern desktop web browsers.

Example usage::

    import tinyhtml5
    tree = tinyhtml5.parse("/path/to/document.html")

"""

from .parser import parse

__all__ = ["parse"]

VERSION = __version__ = "2.0.0"
