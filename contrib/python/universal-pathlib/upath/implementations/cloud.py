from __future__ import annotations

import os
from typing import Any

from upath._compat import FSSpecAccessorShim as _FSSpecAccessorShim
from upath._flavour import upath_strip_protocol
from upath.core import UPath

__all__ = [
    "CloudPath",
    "GCSPath",
    "S3Path",
    "AzurePath",
]


# accessors are deprecated
_CloudAccessor = _FSSpecAccessorShim


class CloudPath(UPath):
    __slots__ = ()

    @classmethod
    def _transform_init_args(
        cls,
        args: tuple[str | os.PathLike, ...],
        protocol: str,
        storage_options: dict[str, Any],
    ) -> tuple[tuple[str | os.PathLike, ...], str, dict[str, Any]]:
        for key in ["bucket", "netloc"]:
            bucket = storage_options.pop(key, None)
            if bucket:
                if str(args[0]).startswith("/"):
                    args = (f"{protocol}://{bucket}{args[0]}", *args[1:])
                else:
                    args0 = upath_strip_protocol(args[0])
                    args = (f"{protocol}://{bucket}/", args0, *args[1:])
                break
        return super()._transform_init_args(args, protocol, storage_options)

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if not parents and not exist_ok and self.exists():
            raise FileExistsError(self.path)
        super().mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def iterdir(self):
        if self.is_file():
            raise NotADirectoryError(str(self))
        if self.parts[-1:] == ("",):
            yield from self.parent.iterdir()
        else:
            yield from super().iterdir()

    def relative_to(self, other, /, *_deprecated, walk_up=False):
        # use the parent implementation for the ValueError logic
        super().relative_to(other, *_deprecated, walk_up=False)
        return self


class GCSPath(CloudPath):
    __slots__ = ()

    def __init__(
        self, *args, protocol: str | None = None, **storage_options: Any
    ) -> None:
        super().__init__(*args, protocol=protocol, **storage_options)
        if not self.drive and len(self.parts) > 1:
            raise ValueError("non key-like path provided (bucket/container missing)")

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        try:
            super().mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        except TypeError as err:
            if "unexpected keyword argument 'create_parents'" in str(err):
                self.fs.mkdir(self.path)


class S3Path(CloudPath):
    __slots__ = ()

    def __init__(
        self, *args, protocol: str | None = None, **storage_options: Any
    ) -> None:
        super().__init__(*args, protocol=protocol, **storage_options)
        if not self.drive and len(self.parts) > 1:
            raise ValueError("non key-like path provided (bucket/container missing)")


class AzurePath(CloudPath):
    __slots__ = ()

    def __init__(
        self, *args, protocol: str | None = None, **storage_options: Any
    ) -> None:
        super().__init__(*args, protocol=protocol, **storage_options)
        if not self.drive and len(self.parts) > 1:
            raise ValueError("non key-like path provided (bucket/container missing)")
