#pragma once

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectServiceMock: public IPartitionDirectService
{
    struct TAddHostRequest
    {
        size_t DirectBlockGroupId = 0;
        ui32 ConnectionConfigGeneration = 0;
    };

    struct TUpdateConfigRequest
    {
        NStorage::NPartitionDirect::TVChunkConfig Config;
        TPersistResultPromise Promise;
    };

    struct TUpdateDirtyMapStateRequest
    {
        ui32 VChunkIndex = 0;
        TDirtyMapStateProto Proto;
        TPersistResultPromise Promise;
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
    size_t CopyRangeBudgetRequestCount = 0;
    ui64 LastCopyRangeBudgetByteCount = 0;
    TDuration CopyRangeBudgetDelay;
    TVector<TUpdateConfigRequest> UpdateConfigRequests;
    TVector<TUpdateDirtyMapStateRequest> UpdateDirtyMapStateRequests;

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

    TPersistResultFuture UpdateVChunkConfig(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg) override
    {
        UpdateConfigRequests.emplace_back(
            cfg,
            NThreading::NewPromise<EPersistResult>());
        return UpdateConfigRequests.back().Promise.GetFuture();
    }

    TPersistResultFuture UpdateDirtyMapState(
        ui32 vChunkIndex,
        TDirtyMapStateProto state) override
    {
        UpdateDirtyMapStateRequests.emplace_back(TUpdateDirtyMapStateRequest{
            .VChunkIndex = vChunkIndex,
            .Proto = std::move(state),
            .Promise = NThreading::NewPromise<EPersistResult>()});
        return UpdateDirtyMapStateRequests.back().Promise.GetFuture();
    }

    void QueryAddHost(
        size_t directBlockGroupId,
        ui32 connectionConfigGeneration) override
    {
        AddHostRequests.push_back(TAddHostRequest{
            .DirectBlockGroupId = directBlockGroupId,
            .ConnectionConfigGeneration = connectionConfigGeneration});
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

    bool TryAdvancePBufferBarrier(
        const NKikimr::NBsController::TDDiskId& pbufferDDiskId,
        ui64 lsn) override
    {
        Y_UNUSED(pbufferDDiskId);
        Y_UNUSED(lsn);
        return true;
    }

    TDuration TakeVolumeCopyRangeBudget(ui64 byteCount) override
    {
        ++CopyRangeBudgetRequestCount;
        LastCopyRangeBudgetByteCount = byteCount;
        return CopyRangeBudgetDelay;
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
