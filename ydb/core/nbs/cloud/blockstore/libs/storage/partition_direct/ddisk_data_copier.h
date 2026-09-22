#pragma once

#include "direct_block_group.h"
#include "read_request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD
class TBlocksDirtyMap;
=======
struct IRangeSyncClient
{
    virtual ~IRangeSyncClient() = default;

    [[nodiscard]] virtual std::optional<TBlockRange16> GetFreshRange(
        THostIndex host) const = 0;
    [[nodiscard]] virtual TReadHint MakeReadHint(TBlockRange16 range) = 0;
    [[nodiscard]] virtual TRangeLock MakeDDiskRangeLock(
        TBlockRange16 range,
        THostMask mask) = 0;
    virtual TSyncHint BeginRangeSync(THostIndex host, TBlockRange16 range) = 0;
    virtual void EndRangeSync(ui64 syncId, bool success) = 0;
    virtual void OnCopyProgress(ui64 totalBytes) = 0;
};

////////////////////////////////////////////////////////////////////////////////
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

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
        IPartitionDirectService* partitionDirectService,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlocksDirtyMap* dirtyMap,
        THostIndex destination);

    // Starts processing at the first range in DirtyMap's Behind field.
    NThreading::TFuture<EResult> Start();
    // Stops processing. After stopping, the processing can be started again.
    NThreading::TFuture<EResult> Stop();

private:
    struct TCopyRangeRequestState;
    using TCopyRangeRequestStatePtr = std::shared_ptr<TCopyRangeRequestState>;

<<<<<<< HEAD
    NWilson::TSpan CreateSpan() const;
    void StartCopyRange();
=======
    std::optional<TBlockRange16> GetFreshRange() const;
    NWilson::TSpan CreateSpan(TBlockRange16 range) const;
    void StartCopyRange();
    void CopyRange(
        TDuration timeWaitBeforeExecution,
        ui64 syncId,
        TBlockRange16 range);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    void OnRangeRead(
        TCopyRangeRequestStatePtr copyRangeState,
        const IReadRequestExecutor::TResponse& response);
    void OnRangeWritten(
        TCopyRangeRequestStatePtr copyRangeState,
        const TDBGWriteBlocksResponse& response);

    NActors::TActorSystem* const ActorSystem = nullptr;
    IPartitionDirectService* const PartitionDirectService = nullptr;
    const TVChunkConfig VChunkConfig;
    const TVolumeConfigPtr VolumeConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const THostIndex Destination;
    TBlocksDirtyMap* const DirtyMap;

    TLogTitle LogTitle;
    EState State = EState::Stopped;
    size_t FreshWatermark = 0;
    NThreading::TPromise<EResult> Complete;
};

using TDDiskDataCopierPtr = std::shared_ptr<TDDiskDataCopier>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
