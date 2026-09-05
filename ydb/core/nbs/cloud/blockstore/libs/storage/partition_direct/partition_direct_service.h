#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/mind/bscontroller/types.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Result of an asynchronous request to persist partition state.
// Cancelled means the partition stopped before it could confirm completion.
enum class EPersistResult
{
    Success,
    Cancelled,
};
using TPersistResultFuture = NThreading::TFuture<EPersistResult>;
using TPersistResultPromise = NThreading::TPromise<EPersistResult>;

////////////////////////////////////////////////////////////////////////////////

struct IPartitionDirectService
{
    virtual ~IPartitionDirectService() = default;

    [[nodiscard]] virtual TVolumeConfigPtr GetVolumeConfig() const = 0;

    virtual void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) = 0;

    // Asynchronously persists the given vchunk config to the partition's
    // local DB. Caller must ensure cfg.IsValid().
    virtual TPersistResultFuture UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) = 0;

    // Asynchronously persists the given TDirtyMapStateProto to the partition's
    // local DB.
    virtual TPersistResultFuture UpdateDirtyMapState(
        ui32 vChunkIndex,
        TDirtyMapStateProto state) = 0;

    // Query the addition of a new host to the group. The request is idempotent
    // and can be repeated multiple times. A request with an outdated
    // generation is rejected.
    virtual void QueryAddHost(
        size_t directBlockGroupId,
        ui32 dbgConnectionsConfigGeneration) = 0;

    // Generates the next tablet-wide write LSN. Called by a vchunk on its
    // executor thread when it starts processing a write, so generation and
    // dirty-map registration happen on the same thread. Also drives periodic
    // persistent buffer cleanup.
    virtual ui64 GenerateLsn() = 0;

    // Called when DDisk replied BLOCKED, meaning DDisk has already
    // seen a newer tablet generation. The current tablet instance must suicide.
    virtual void StopTablet(const TString& reason) = 0;

    // Several DBGs of the tablet may share one pbuffer ddisk, and each of them
    // broadcasts the same tablet-wide barrier, so the ddisk may have already
    // received this lsn. Called from DBG executor threads.
    // True: the lsn advances the ddisk's barrier - send it.
    // False: the ddisk already holds a barrier >= lsn; re-sending it would be
    // a non-advancing MoveBarrier that DDisk logs as an error - skip the send.
    virtual bool TryAdvancePBufferBarrier(
        const NKikimr::NBsController::TDDiskId& pbufferDDiskId,
        ui64 lsn) = 0;

    // Reserves byteCount from the disk-wide range-copy bandwidth budget.
    // Returns the delay before the operation may start. Zero means it may start
    // immediately or throttling is disabled. Called from DBG executor threads.
    virtual TDuration TakeVolumeCopyRangeBudget(ui64 byteCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
