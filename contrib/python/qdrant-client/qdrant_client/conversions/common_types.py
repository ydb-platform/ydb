import sys

import numpy as np
import numpy.typing as npt

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from typing import Union, get_args, Sequence
from uuid import UUID

from qdrant_client import grpc
from qdrant_client.http import models as rest

typing_remap = {
    rest.StrictStr: str,
    rest.StrictInt: int,
    rest.StrictFloat: float,
    rest.StrictBool: bool,
}


def remap_type(tp: type) -> type:
    """Remap type to a type that can be used in type annotations

    Pydantic uses custom types for strict types, so we need to remap them to standard types
    so that they can be used in type annotations and isinstance checks
    """
    return typing_remap.get(tp, tp)


def get_args_subscribed(tp):  # type: ignore
    """Get type arguments with all substitutions performed. Supports subscripted generics having __origin__

    Args:
        tp: type to get arguments from. Can be either a type or a subscripted generic

    Returns:
        tuple of type arguments
    """
    return tuple(
        remap_type(arg if not hasattr(arg, "__origin__") else arg.__origin__)
        for arg in get_args(tp)
    )


Filter = Union[rest.Filter, grpc.Filter]
SearchParams = Union[rest.SearchParams, grpc.SearchParams]
PayloadSelector = Union[rest.PayloadSelector, grpc.WithPayloadSelector]
Distance = Union[rest.Distance, int]  # type(grpc.Distance) == int
HnswConfigDiff = Union[rest.HnswConfigDiff, grpc.HnswConfigDiff]
VectorsConfigDiff = Union[rest.VectorsConfigDiff, grpc.VectorsConfigDiff]
QuantizationConfigDiff = Union[rest.QuantizationConfigDiff, grpc.QuantizationConfigDiff]
OptimizersConfigDiff = Union[rest.OptimizersConfigDiff, grpc.OptimizersConfigDiff]
CollectionParamsDiff = Union[rest.CollectionParamsDiff, grpc.CollectionParamsDiff]
WalConfigDiff = Union[rest.WalConfigDiff, grpc.WalConfigDiff]
QuantizationConfig = Union[rest.QuantizationConfig, grpc.QuantizationConfig]
PointId = Union[int, str, UUID, grpc.PointId]
PayloadSchemaType = Union[
    rest.PayloadSchemaType,
    rest.PayloadSchemaParams,
    int,
    grpc.PayloadIndexParams,
]  # type(grpc.PayloadSchemaType) == int
PointStruct: TypeAlias = rest.PointStruct
Batch: TypeAlias = rest.Batch
Points = Union[Batch, Sequence[Union[rest.PointStruct, grpc.PointStruct]]]
PointsSelector = Union[
    list[PointId],
    rest.Filter,
    grpc.Filter,
    rest.PointsSelector,
    grpc.PointsSelector,
]
LookupLocation = Union[rest.LookupLocation, grpc.LookupLocation]
RecommendStrategy: TypeAlias = rest.RecommendStrategy
OrderBy = Union[rest.OrderByInterface, grpc.OrderBy]
ShardingMethod: TypeAlias = rest.ShardingMethod
ShardKey: TypeAlias = rest.ShardKey
ShardKeySelector: TypeAlias = rest.ShardKeySelector

AliasOperations = Union[
    rest.CreateAliasOperation,
    rest.RenameAliasOperation,
    rest.DeleteAliasOperation,
    grpc.AliasOperations,
]
Payload: TypeAlias = rest.Payload

ScoredPoint: TypeAlias = rest.ScoredPoint
UpdateResult: TypeAlias = rest.UpdateResult
Record: TypeAlias = rest.Record
CollectionsResponse: TypeAlias = rest.CollectionsResponse
CollectionInfo: TypeAlias = rest.CollectionInfo
CountResult: TypeAlias = rest.CountResult
SnapshotDescription: TypeAlias = rest.SnapshotDescription
NamedVector: TypeAlias = rest.NamedVector
NamedSparseVector: TypeAlias = rest.NamedSparseVector
SparseVector: TypeAlias = rest.SparseVector
PointVectors: TypeAlias = rest.PointVectors
Vector: TypeAlias = rest.Vector
VectorInput: TypeAlias = rest.VectorInput
VectorStruct: TypeAlias = rest.VectorStruct
VectorParams: TypeAlias = rest.VectorParams
SparseVectorParams: TypeAlias = rest.SparseVectorParams
SnapshotPriority: TypeAlias = rest.SnapshotPriority
CollectionsAliasesResponse: TypeAlias = rest.CollectionsAliasesResponse
UpdateOperation: TypeAlias = rest.UpdateOperation
Query: TypeAlias = rest.Query
Prefetch: TypeAlias = rest.Prefetch
Document: TypeAlias = rest.Document
Image: TypeAlias = rest.Image
InferenceObject: TypeAlias = rest.InferenceObject
StrictModeConfig: TypeAlias = rest.StrictModeConfig

QueryRequest: TypeAlias = rest.QueryRequest

Mmr: TypeAlias = rest.Mmr

ReadConsistency: TypeAlias = rest.ReadConsistency
WriteOrdering: TypeAlias = rest.WriteOrdering
WithLookupInterface: TypeAlias = rest.WithLookupInterface

GroupsResult: TypeAlias = rest.GroupsResult
QueryResponse: TypeAlias = rest.QueryResponse

FacetValue: TypeAlias = rest.FacetValue
FacetResponse: TypeAlias = rest.FacetResponse
SearchMatrixRequest = Union[rest.SearchMatrixRequest, grpc.SearchMatrixPoints]
SearchMatrixOffsetsResponse: TypeAlias = rest.SearchMatrixOffsetsResponse
SearchMatrixPairsResponse: TypeAlias = rest.SearchMatrixPairsResponse
SearchMatrixPair: TypeAlias = rest.SearchMatrixPair

VersionInfo: TypeAlias = rest.VersionInfo

ReplicaState: TypeAlias = rest.ReplicaState
ClusterOperations: TypeAlias = rest.ClusterOperations
ClusterStatus: TypeAlias = rest.ClusterStatus
CollectionClusterInfo: TypeAlias = rest.CollectionClusterInfo

# we can't use `nptyping` package due to numpy/python-version incompatibilities
# thus we need to define precise type annotations while we support python3.7
_np_numeric = Union[
    np.bool_,  # pylance can't handle np.bool8 alias
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.uint8,
    np.uint16,
    np.uint32,
    np.uint64,
    np.intp,
    np.uintp,
    np.float16,
    np.float32,
    np.float64,
    np.longdouble,  # np.float96 and np.float128 are platform dependant aliases for longdouble
]

NumpyArray: TypeAlias = npt.NDArray[_np_numeric]
