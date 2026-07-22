#pragma once

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectServiceMock: public IPartitionDirectService
{
    struct TAddHostRequest
    {
        size_t DirectBlockGroupId = 0;
        size_t NewHostIndex = 0;
    };

    explicit TPartitionDirectServiceMock(bool dropScheduledCallbacks = false)
        : DropScheduledCallbacks(dropScheduledCallbacks)
    {}

    TVolumeConfigPtr VolumeConfig;
    bool DropScheduledCallbacks = false;
    TVector<TAddHostRequest> AddHostRequests;
    ui64 LsnGenerator = 0;
    size_t BlockedGenerationCount = 0;
    TString LastBlockedReason;

    [[nodiscard]] TVolumeConfigPtr GetVolumeConfig() const override
    {
        return VolumeConfig;
    }

    void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) override
    {
        Y_UNUSED(delay);
        if (DropScheduledCallbacks) {
            return;
        }
        executor->ExecuteSimple(std::move(callback));
    }

    void UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) override
    {
        Y_UNUSED(cfg);
    }

    void QueryAddHost(size_t directBlockGroupId, size_t newHostIndex) override
    {
        AddHostRequests.push_back(TAddHostRequest{
            .DirectBlockGroupId = directBlockGroupId,
            .NewHostIndex = newHostIndex});
    }

    ui64 GenerateLsn() override
    {
        return ++LsnGenerator;
    }

    void StopTablet(const TString& reason) override
    {
        ++BlockedGenerationCount;
        LastBlockedReason = reason;
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
