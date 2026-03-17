import sys
import warnings
from types import ModuleType
from typing import Any

from zarr.errors import ZarrDeprecationWarning
from zarr.storage._common import StoreLike, StorePath
from zarr.storage._fsspec import FsspecStore
from zarr.storage._local import LocalStore
from zarr.storage._logging import LoggingStore
from zarr.storage._memory import GpuMemoryStore, MemoryStore
from zarr.storage._obstore import ObjectStore
from zarr.storage._wrapper import WrapperStore
from zarr.storage._zip import ZipStore

__all__ = [
    "FsspecStore",
    "GpuMemoryStore",
    "LocalStore",
    "LoggingStore",
    "MemoryStore",
    "ObjectStore",
    "StoreLike",
    "StorePath",
    "WrapperStore",
    "ZipStore",
]


class VerboseModule(ModuleType):
    def __setattr__(self, attr: str, value: Any) -> None:
        if attr == "default_compressor":
            warnings.warn(
                "setting zarr.storage.default_compressor is deprecated, use "
                "zarr.config to configure array.v2_default_compressor "
                "e.g. config.set({'codecs.zstd':'numcodecs.Zstd', 'array.v2_default_compressor.numeric': 'zstd'})",
                ZarrDeprecationWarning,
                stacklevel=1,
            )
        else:
            super().__setattr__(attr, value)


sys.modules[__name__].__class__ = VerboseModule
