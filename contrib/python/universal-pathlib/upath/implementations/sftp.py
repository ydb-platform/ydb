from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from typing import Any
from typing import Generator

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

from upath import UPath

_unset: Any = object()


class SFTPPath(UPath):
    __slots__ = ()

    def iterdir(self) -> Generator[Self, None, None]:
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        else:
            return super().iterdir()
