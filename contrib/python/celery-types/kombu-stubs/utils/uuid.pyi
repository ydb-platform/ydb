from collections.abc import Callable
from uuid import UUID

def uuid(_uuid: Callable[[], UUID] = ...) -> str: ...
