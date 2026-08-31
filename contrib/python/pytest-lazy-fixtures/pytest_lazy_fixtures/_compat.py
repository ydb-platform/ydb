from __future__ import annotations

import inspect
import sys
from typing import Callable


def signature(func: Callable[..., object]) -> inspect.Signature:
    """Return ``func``'s signature without eagerly evaluating its annotations.

    On Python 3.14+ (PEP 649) annotations are evaluated lazily, and
    ``inspect.signature`` resolves them by default. That raises ``NameError``
    for annotations that reference names only available under
    ``if TYPE_CHECKING:``. Callers here only need the parameter names, so we ask
    for the ``FORWARDREF`` format, which leaves unresolved names as placeholders
    instead of evaluating them.
    """
    if sys.version_info >= (3, 14):
        import annotationlib  # noqa: PLC0415

        return inspect.signature(func, annotation_format=annotationlib.Format.FORWARDREF)
    return inspect.signature(func)
