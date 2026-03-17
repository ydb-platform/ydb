#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import logging
import traceback
from collections.abc import Callable
from functools import wraps
from typing import Any, Optional, TypeVar, Union

from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.resources import XMLResource

logger = logging.getLogger('xmlschema')

LOG_LEVELS = {'DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'CRITICAL'}


def set_logging_level(level: Union[str, int]) -> None:
    """set logging level of xmlschema's logger."""
    if isinstance(level, str):
        _level = level.strip().upper()
        if _level not in LOG_LEVELS:
            raise XMLSchemaValueError(
                _("{!r} is not a valid loglevel").format(level)
            )
        logger.setLevel(getattr(logging, _level))
    else:
        logger.setLevel(level)


RT = TypeVar('RT')


def logged(func: Callable[..., RT]) -> Callable[..., RT]:
    """
    A decorator for activating a logging level for a function. The keyword
    argument 'loglevel' is obtained from the keyword arguments and used by the
    wrapper function to set the logging level of the decorated function and
    to restore the original level after the call.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        loglevel: Optional[Union[int, str]] = kwargs.get('loglevel')
        if loglevel is None:
            return func(*args, **kwargs)
        else:
            current_level = logger.level
            set_logging_level(loglevel)
            try:
                return func(*args, **kwargs)
            finally:
                logger.setLevel(current_level)

    return wrapper


def format_xmlschema_stack(start_with: str) -> str:
    """Extract a formatted traceback for xmlschema package from current stack frame."""
    formatted_stack = traceback.format_stack()
    for k, line in enumerate(formatted_stack):
        if start_with in line:
            return ''.join(formatted_stack[k:])
    else:
        return ''.join(formatted_stack)


def dump_data(*args: Any) -> None:
    """Dump data to logger for debugging purposes."""
    if not args:
        return

    logging.basicConfig()
    chunks: list[str] = [' dump data for xmlschema debugging\n']

    for item in args:
        if isinstance(item, XMLResource):
            chunks.append(repr(item))
            if item.name:
                chunks.append(f'name: {item.name!r}')
            chunks.append(f'namespace: {item.namespace!r}')
            if item.url:
                chunks.append(f'URL: {item.url}')
            if not item.is_lazy():
                chunks.append(item.tostring())
            chunks.append('')
        else:
            chunks.append(repr(item))
            chunks.append('')

    logger.warning('\n'.join(chunks))
