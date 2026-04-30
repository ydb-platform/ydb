#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TVChunkConfig;

class TVChunk;
using TVChunkPtr = std::shared_ptr<TVChunk>;

struct TDBGReadBlocksResponse;
struct TDBGWriteBlocksResponse;
struct TDBGWriteBlocksToManyPBuffersResponse;
struct TDBGFlushResponse;
struct TDBGEraseResponse;
struct TDBGRestoreResponse;
struct TListPBufferResponse;
struct TAggregatedListPBufferResponse;
class IDirectBlockGroup;
using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
