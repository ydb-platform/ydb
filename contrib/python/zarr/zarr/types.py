from typing import Any, TypeAlias

from zarr.core.array import Array, AsyncArray
from zarr.core.metadata.v2 import ArrayV2Metadata
from zarr.core.metadata.v3 import ArrayV3Metadata

AnyAsyncArray: TypeAlias = AsyncArray[Any]
"""A Zarr format 2 or 3 `AsyncArray`"""

AsyncArrayV2: TypeAlias = AsyncArray[ArrayV2Metadata]
"""A Zarr format 2 `AsyncArray`"""

AsyncArrayV3: TypeAlias = AsyncArray[ArrayV3Metadata]
"""A Zarr format 3 `AsyncArray`"""

AnyArray: TypeAlias = Array[Any]
"""A Zarr format 2 or 3 `Array`"""

ArrayV2: TypeAlias = Array[ArrayV2Metadata]
"""A Zarr format 2 `Array`"""

ArrayV3: TypeAlias = Array[ArrayV3Metadata]
"""A Zarr format 3 `Array`"""
