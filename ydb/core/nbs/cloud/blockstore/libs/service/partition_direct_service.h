#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace NStorage::NPartitionDirect {
class TVChunkConfig;
}   // namespace NStorage::NPartitionDirect

////////////////////////////////////////////////////////////////////////////////

struct IPartitionDirectService
{
    virtual ~IPartitionDirectService() = default;

    [[nodiscard]] virtual TVolumeConfigPtr GetVolumeConfig() const = 0;

    [[nodiscard]] virtual NWilson::TSpan CreteRootSpan(TStringBuf name) = 0;

    virtual void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) = 0;

    // Asynchronously persists the given vchunk config to the partition's
    // local DB. Caller must ensure cfg.IsValid().
    virtual void UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) = 0;

    // Query the addition of a new host to the group. The request is idempotent
    // and can be repeated multiple times.
    virtual void QueryAddHost(
        size_t directBlockGroupId,
        size_t newHostIndex) = 0;

    // Query the removal of host `hostIndex` from the group. The deletion is
    // idempotent on the BSController side and can be repeated multiple times.
    virtual void QueryRemoveHost(
        size_t directBlockGroupId,
        size_t hostIndex) = 0;

    // Generates the next tablet-wide write LSN. Called by a vchunk on its
    // executor thread when it starts processing a write, so generation and
    // dirty-map registration happen on the same thread. Also drives periodic
    // persistent buffer cleanup.
    virtual ui64 GenerateLsn() = 0;

    // Called when DDisk replied BLOCKED, meaning DDisk has already
    // seen a newer tablet generation. The current tablet instance must suicide.
    virtual void StopTablet(const TString& reason) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
