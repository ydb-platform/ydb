#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/disk_state_provider.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionDirectService: public IDiskStateProvider
{
    virtual ~IPartitionDirectService() = default;

    [[nodiscard]] virtual TVolumeConfigPtr GetVolumeConfig() const = 0;

    virtual void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) = 0;

    // Asynchronously persists the vchunk config and dirty-map state in one
    // partition Local DB transaction. Caller must ensure cfg.IsValid().
    virtual TPersistResultFuture UpdateVChunkState(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg,
        TDirtyMapStateProto state) = 0;

    // Asynchronously persists the given TDirtyMapStateProto to the partition's
    // local DB.
    virtual TPersistResultFuture UpdateDirtyMapState(
        ui32 vChunkIndex,
        TDirtyMapStateProto state) = 0;

    // Marks the vchunk as touched in the partition's local DB. A touched bit
    // is never cleared.
    virtual TPersistResultFuture SetVChunkTouched(ui32 vChunkIndex) = 0;

    // Query the addition of a new host to the group. The request is idempotent
    // and can be repeated multiple times. A request with an outdated
    // generation is rejected.
    virtual void QueryAddHost(
        size_t directBlockGroupId,
        ui32 dbgConnectionsConfigGeneration) = 0;

    // Query the removal of the host in that slot. A request with an outdated
    // generation is rejected.
    virtual void QueryRemoveHost(
        size_t directBlockGroupId,
        size_t hostIndex,
        ui32 dbgConnectionsConfigGeneration) = 0;

    // Registers a starting vchunk write and mints its lsn. Called by a vchunk
    // on its executor thread when it starts processing a write, so generation
    // and dirty-map registration happen on the same thread. Every call must
    // be paired with OnWriteFinished().
    virtual ui64 OnWriteStarted() = 0;

    // Releases the in-flight write registered by OnWriteStarted().
    virtual void OnWriteFinished() = 0;

    // Called when DDisk replied BLOCKED, meaning DDisk has already
    // seen a newer tablet generation. The current tablet instance must suicide.
    virtual void StopTablet(const TString& reason) = 0;

    // Reserves byteCount from the disk-wide range-copy bandwidth budget.
    // Returns the delay before the operation may start. Zero means it may start
    // immediately or throttling is disabled. Called from DBG executor threads.
    virtual TDuration TakeVolumeCopyRangeBudget(ui64 byteCount) = 0;

    // Store changes host health in partition's local DB
    virtual void PersistHostHealth(
        size_t directBlockGroupId,
        THostIndex hostIndex,
        EHostHealth oldHealth,
        EHostHealth newHealth) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
