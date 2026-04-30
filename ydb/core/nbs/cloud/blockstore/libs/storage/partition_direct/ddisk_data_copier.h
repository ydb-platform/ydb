#pragma once

#include "direct_block_group.h"
#include "read_request.h"
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_status.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBlocksDirtyMap;
class TDDiskStateList;

class TDDiskDataCopier: public std::enable_shared_from_this<TDDiskDataCopier>
{
public:
    enum class EResult
    {
        Ok,
        Error,
        Interrupted,
    };

    enum class EState
    {
        Stopped,
        Stopping,
        Running,
    };

    TDDiskDataCopier(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IPartitionDirectServicePtr partitionDirectService,
        IDirectBlockGroupPtr directBlockGroup,
        TBlocksDirtyMap* dirtyMap,
        TDDiskStateList* ddiskStates,
        THostIndex destination);

    // Starts processing from the FreshWatermark position, which is stored in
    // ddiskStates.
    NThreading::TFuture<EResult> Start();
    // Stops processing. After stopping, the processing can be started again.
    NThreading::TFuture<EResult> Stop();

private:
    struct TCopyRangeRequestState;
    using TCopyRangeRequestStatePtr = std::shared_ptr<TCopyRangeRequestState>;

    NWilson::TSpan CreateSpan() const;
    void StartCopyRange();
    void OnRangeRead(
        TCopyRangeRequestStatePtr copyRangeState,
        const TReadRequestExecutor::TResponse& response);
    void OnRangeWritten(
        TCopyRangeRequestStatePtr copyRangeState,
        const TDBGWriteBlocksResponse& response);

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TVChunkConfig VChunkConfig;
    const TVolumeConfigPtr VolumeConfig;
    const IPartitionDirectServicePtr PartitionDirectService;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const THostIndex Destination;
    TBlocksDirtyMap* const DirtyMap;
    TDDiskStateList* const DDiskStates;

    EState State = EState::Stopped;
    size_t FreshWatermark = 0;
    NThreading::TPromise<EResult> Complete;
};

using TDDiskDataCopierPtr = std::shared_ptr<TDDiskDataCopier>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
