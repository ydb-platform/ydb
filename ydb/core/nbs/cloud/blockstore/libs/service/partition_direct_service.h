#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
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

    virtual void ReportCleanupBound(ui32 vChunkIndex, ui64 bound) = 0;

    // Releases lsns that a vchunk has taken ownership of (covered by its
    // cleanup bound), so they no longer pin the global cleanup watermark.
    virtual void CompleteOutstandingLsns(const TVector<ui64>& lsns) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
