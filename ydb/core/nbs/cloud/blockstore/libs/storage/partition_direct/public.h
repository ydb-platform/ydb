#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunkConfig;

class TVChunk;
using TVChunkPtr = std::shared_ptr<TVChunk>;
using TVChunkWeakPtr = std::weak_ptr<TVChunk>;

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

class TRegion;
using TRegionPtr = std::shared_ptr<TRegion>;

class TDDiskDataCopier;
using TDDiskDataCopierPtr = std::shared_ptr<TDDiskDataCopier>;

class TWriteRequestBundle;
using TWriteRequestBundlePtr = std::shared_ptr<TWriteRequestBundle>;

struct IWriteClient;
using IWriteClientWeakPtr = std::weak_ptr<IWriteClient>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
