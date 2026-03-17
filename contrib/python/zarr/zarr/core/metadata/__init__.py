from typing import TypeAlias, TypeVar

from .v2 import ArrayV2Metadata, ArrayV2MetadataDict
from .v3 import ArrayMetadataJSON_V3, ArrayV3Metadata

ArrayMetadata: TypeAlias = ArrayV2Metadata | ArrayV3Metadata
ArrayMetadataDict: TypeAlias = ArrayV2MetadataDict | ArrayMetadataJSON_V3
T_ArrayMetadata = TypeVar("T_ArrayMetadata", ArrayV2Metadata, ArrayV3Metadata, covariant=True)

__all__ = [
    "ArrayMetadata",
    "ArrayMetadataDict",
    "ArrayMetadataJSON_V3",
    "ArrayV2Metadata",
    "ArrayV2MetadataDict",
    "ArrayV3Metadata",
]
