#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
#
import importlib.resources
from collections.abc import Iterable
from typing import cast, Any, Optional, Union
import gettext as _gettext
from pathlib import Path

__all__ = ['activate', 'deactivate', 'gettext']

_translation: Any = None
_installed: bool = False


def activate(localedir: Union[None, str, Path] = None,
             languages: Optional[Iterable[str]] = None,
             fallback: bool = True,
             install: bool = False) -> None:
    """
    Activate translation of xmlschema parsing/validation error messages.

    :param localedir: a string or Path-like object to locale directory
    :param languages: list of language codes
    :param fallback: for default fallback mode is activated
    :param install: if `True` installs function _() in Python’s builtins namespace
    """
    global _translation
    global _installed

    if localedir is None:  # pragma: no cover
        localedir = importlib.resources.files(__package__).joinpath('locale')

    translation = _gettext.translation(
        domain='xmlschema',
        localedir=localedir,
        languages=languages,
        fallback=fallback,
    )

    deactivate()

    _translation = translation
    if install:
        _translation.install()
        _installed = True


def deactivate() -> None:
    """Deactivate translation of xmlschema parsing/validation error messages."""
    global _translation
    global _installed

    if _installed and _translation is not None:
        import builtins
        if builtins.__dict__.get('_') == _translation.gettext:  # pragma: no cover
            builtins.__dict__.pop('_')

    _translation = None
    _installed = False


def gettext(message: str) -> str:
    if _translation is None:
        return message
    return cast(str, _translation.gettext(message))
