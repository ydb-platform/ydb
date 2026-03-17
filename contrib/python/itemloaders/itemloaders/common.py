"""Common functions used in Item Loaders code"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

from itemloaders.utils import get_func_args

if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping


def wrap_loader_context(
    function: Callable[..., Any], context: MutableMapping[str, Any]
) -> Callable[..., Any]:
    """Wrap functions that receive loader_context to contain the context
    "pre-loaded" and expose a interface that receives only one argument
    """
    if "loader_context" in get_func_args(function):
        return partial(function, loader_context=context)
    return function
