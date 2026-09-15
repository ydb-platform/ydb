"""Grab the global logger instance."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import logging
from typing import Any, cast

# Add a NullHandler to silence warnings about not being
# initialized, per best practice for libraries.
_fallback = logging.getLogger("traitlets")
_fallback.addHandler(logging.NullHandler())


def get_logger() -> logging.Logger | logging.LoggerAdapter[Any]:
    """Grab the global logger instance.

    If a global Application is instantiated, grab its logger.
    Otherwise, grab the 'traitlets' library logger.
    """
    from .config import Application

    if Application.initialized():
        return cast("logging.Logger | logging.LoggerAdapter[Any]", Application.instance().log)
    return _fallback
