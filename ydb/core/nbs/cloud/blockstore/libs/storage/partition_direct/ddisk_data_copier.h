#pragma once

#include "direct_block_group.h"
#include "read_request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBlocksDirtyMap;

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
        ITraceService* traceService,
        IPartitionDirectService* partitionDirectService,
        const TDiskDescription& diskDescription,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlocksDirtyMapPtr dirtyMap,
        THostIndex destination);

    // Starts processing from the FreshWatermark position, which is stored in
    // dirtyMap.
    NThreading::TFuture<EResult> Start();
    // Stops processing. After stopping, the processing can be started again.
    NThreading::TFuture<EResult> Stop();

private:
    struct TCopyRangeRequestState;
    using TCopyRangeRequestStatePtr = std::shared_ptr<TCopyRangeRequestState>;

    std::optional<TBlockRange64> GetFreshRange() const;
    NWilson::TSpan CreateSpan(TBlockRange64 range) const;
    void StartCopyRange();
    void CopyRange(
        TDuration timeWaitBeforeExecution,
        ui64 syncId,
        TBlockRange64 range);
    void OnRangeRead(
        TCopyRangeRequestStatePtr copyRangeState,
        const IReadRequestExecutor::TResponse& response);
    void OnRangeWritten(
        TCopyRangeRequestStatePtr copyRangeState,
        const TDBGWriteBlocksResponse& response);
    void ScheduleStartCopyRange(TDuration delay);

    NActors::TActorSystem* const ActorSystem = nullptr;
    ITraceService* const TraceService = nullptr;
    const TVChunkConfig VChunkConfig;
    const TVolumeConfigPtr VolumeConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const THostIndex Destination;
    const TBlocksDirtyMapPtr DirtyMap;

    TLogTitle LogTitle;
    EState State = EState::Stopped;
    TBackoffDelayProvider BackoffDelayProvider;
    NThreading::TPromise<EResult> Complete;
};

using TDDiskDataCopierPtr = std::shared_ptr<TDDiskDataCopier>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
