"""Tools for generate GraphQL queries from python classes."""

from .__info__ import __author__, __email__, __license__, __maintainer__
from .__version__ import __version__
from .base_model import GraphQLQueryBaseModel
from .types import Argument, Directive, Field, Fragment, InlineFragment, Operation, Query, Variable

__all__ = [
    "__version__",
    "__email__",
    "__author__",
    "__license__",
    "__maintainer__",
    "Variable",
    "Argument",
    "Directive",
    "Field",
    "InlineFragment",
    "Fragment",
    "Query",
    "Operation",
    "GraphQLQueryBaseModel",
]
