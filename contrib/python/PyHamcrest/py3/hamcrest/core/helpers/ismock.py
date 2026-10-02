from typing import Any, List, Type

MOCKTYPES: List[Type] = []
try:
    from mock import Mock

    MOCKTYPES += [Mock]
except ImportError:
    pass
try:
    from unittest.mock import Mock

    MOCKTYPES += [Mock]
except ImportError:
    pass


def ismock(obj: Any) -> bool:
    return isinstance(obj, tuple(MOCKTYPES))
