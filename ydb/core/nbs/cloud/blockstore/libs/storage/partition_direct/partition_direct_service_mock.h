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
        ui32 DBGConnectionsConfigGeneration = 0;
    };

    struct TRemoveHostRequest
    {
        size_t DirectBlockGroupId = 0;
        size_t HostIndex = 0;
        ui32 DBGConnectionsConfigGeneration = 0;
    };

    struct TUpdateConfigRequest
    {
        NStorage::NPartitionDirect::TVChunkConfig Config;
        TDirtyMapStateProto Proto;
        TPersistResultPromise Promise;
    };

    struct TUpdateDirtyMapStateRequest
    {
        ui32 VChunkIndex = 0;
        TDirtyMapStateProto Proto;
        TPersistResultPromise Promise;
    };

    struct TPersistHostHealthRequest
    {
        size_t DirectBlockGroupId = 0;
        size_t HostIndex = 0;
        EHostHealth OldHealth = EHostHealth::Online;
        EHostHealth NewHealth = EHostHealth::Online;
    };

    explicit TPartitionDirectServiceMock(bool dropScheduledCallbacks = false)
        : DropScheduledCallbacks(dropScheduledCallbacks)
    {}

    TVolumeConfigPtr VolumeConfig;
    bool DropScheduledCallbacks = false;
    TVector<TAddHostRequest> AddHostRequests;
    TVector<TRemoveHostRequest> RemoveHostRequests;
    ui64 LsnGenerator = 0;
    size_t InflightWriteCount = 0;
    size_t BlockedGenerationCount = 0;
    TString LastBlockedReason;
    size_t CopyRangeBudgetRequestCount = 0;
    ui64 LastCopyRangeBudgetByteCount = 0;
    TDuration CopyRangeBudgetDelay;
    TVector<TUpdateConfigRequest> UpdateConfigRequests;
    TVector<TUpdateDirtyMapStateRequest> UpdateDirtyMapStateRequests;
    TVector<ui32> TouchedVChunkIndices;
    TVector<TPersistHostHealthRequest> PersistHostHealthRequests;

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

    TPersistResultFuture UpdateVChunkState(
        const NStorage::NPartitionDirect::TVChunkConfig& cfg,
        TDirtyMapStateProto state) override
    {
        UpdateConfigRequests.emplace_back(TUpdateConfigRequest{
            .Config = cfg,
            .Proto = std::move(state),
            .Promise = NThreading::NewPromise<EPersistResult>()});
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

    TPersistResultFuture SetVChunkTouched(ui32 vChunkIndex) override
    {
        TouchedVChunkIndices.push_back(vChunkIndex);
        auto promise = NThreading::NewPromise<EPersistResult>();
        promise.SetValue(EPersistResult::Success);
        return promise.GetFuture();
    }

    void QueryAddHost(
        size_t directBlockGroupId,
        ui32 dbgConnectionsConfigGeneration) override
    {
        AddHostRequests.push_back(TAddHostRequest{
            .DirectBlockGroupId = directBlockGroupId,
            .DBGConnectionsConfigGeneration = dbgConnectionsConfigGeneration});
    }

    void QueryRemoveHost(
        size_t directBlockGroupId,
        size_t hostIndex,
        ui32 dbgConnectionsConfigGeneration) override
    {
        RemoveHostRequests.push_back(TRemoveHostRequest{
            .DirectBlockGroupId = directBlockGroupId,
            .HostIndex = hostIndex,
            .DBGConnectionsConfigGeneration = dbgConnectionsConfigGeneration});
    }

    ui64 OnWriteStarted() override
    {
        ++InflightWriteCount;
        return ++LsnGenerator;
    }

    void OnWriteFinished() override
    {
        Y_ABORT_UNLESS(InflightWriteCount > 0);
        --InflightWriteCount;
    }

    [[nodiscard]] size_t GetInflightWriteCount() const override
    {
        return InflightWriteCount;
    }

    void StopTablet(const TString& reason) override
    {
        ++BlockedGenerationCount;
        LastBlockedReason = reason;
    }

    TDuration TakeVolumeCopyRangeBudget(ui64 byteCount) override
    {
        ++CopyRangeBudgetRequestCount;
        LastCopyRangeBudgetByteCount = byteCount;
        return CopyRangeBudgetDelay;
    }

    void PersistHostHealth(
        size_t directBlockGroupId,
        THostIndex hostIndex,
        EHostHealth oldHealth,
        EHostHealth newHealth) override
    {
        PersistHostHealthRequests
            .emplace_back(directBlockGroupId, hostIndex, oldHealth, newHealth);
    }
};

using TPartitionDirectServiceMockPtr =
    std::shared_ptr<TPartitionDirectServiceMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
