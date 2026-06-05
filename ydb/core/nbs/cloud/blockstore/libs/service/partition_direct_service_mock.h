#pragma once

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectServiceMock: public IPartitionDirectService
{
    TVolumeConfigPtr VolumeConfig;

    [[nodiscard]] TVolumeConfigPtr GetVolumeConfig() const override
    {
        return VolumeConfig;
    }

    NWilson::TSpan CreteRootSpan(TStringBuf name) override
    {
        Y_UNUSED(name);
        return {};
    }

    void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) override
    {
        Y_UNUSED(delay);
        executor->ExecuteSimple(std::move(callback));
    }

    void UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) override
    {
        Y_UNUSED(cfg);
    }

    void ReportCleanupBound(ui32 vChunkIndex, ui64 bound) override
    {
        Y_UNUSED(vChunkIndex);
        Y_UNUSED(bound);
    }

    void CompleteOutstandingLsns(const TVector<ui64>& lsns) override
    {
        Y_UNUSED(lsns);
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
