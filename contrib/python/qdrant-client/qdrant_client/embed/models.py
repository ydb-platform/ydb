from typing import Union

from pydantic import StrictFloat, StrictStr

from qdrant_client.http.models import ExtendedPointId, SparseVector


NumericVector = Union[
    list[StrictFloat],
    SparseVector,
    list[list[StrictFloat]],
]
NumericVectorInput = Union[
    list[StrictFloat],
    SparseVector,
    list[list[StrictFloat]],
    ExtendedPointId,
]
NumericVectorStruct = Union[
    list[StrictFloat],
    list[list[StrictFloat]],
    dict[StrictStr, NumericVector],
]

__all__ = ["NumericVector", "NumericVectorInput", "NumericVectorStruct"]
