#pragma once

#include "public.h"

#include "volume_stats.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultRequestProcessingPolicy
{
    void RequestPostponed(EBlockStoreRequest requestType)
    {
        Y_UNUSED(requestType);
    }

    void RequestPostponedServer(EBlockStoreRequest requestType)
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvanced(EBlockStoreRequest requestType)
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType)
    {
        Y_UNUSED(requestType);
    }
};

template <typename TRequestProcessingPolicy = TDefaultRequestProcessingPolicy>
class TTestVolumeInfo final
    : public IVolumeInfo
    , public TRequestProcessingPolicy
{
public:
    NProto::TVolume Volume;

    const NProto::TVolume& GetInfo() const override
    {
        return Volume;
    }

    TDuration GetPossiblePostponeDuration() const override
    {
        return TDuration::Zero();
    }

    ui64 RequestStarted(
        EBlockStoreRequest requestType,
        ui64 requestBytes) override
    {
        Y_UNUSED(requestType);
        Y_UNUSED(requestBytes);
        return 0;
    }

    TDuration RequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        TDuration backoffTime,
        TDuration shapingTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ui64 responseSent) override
    {
        Y_UNUSED(requestType);
        Y_UNUSED(requestStarted);
        Y_UNUSED(postponedTime);
        Y_UNUSED(backoffTime);
        Y_UNUSED(shapingTime);
        Y_UNUSED(requestBytes);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
        Y_UNUSED(unaligned);
        Y_UNUSED(responseSent);
        return TDuration::Zero();
    }

    void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TRequestTime requestTime) override
    {
        Y_UNUSED(requestType);
        Y_UNUSED(requestTime);
    }

    void AddRetryStats(
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        Y_UNUSED(requestType);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        TRequestProcessingPolicy::RequestPostponed(requestType);
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        TRequestProcessingPolicy::RequestPostponedServer(requestType);
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        TRequestProcessingPolicy::RequestAdvanced(requestType);
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        TRequestProcessingPolicy::RequestAdvancedServer(requestType);
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void BatchCompleted(
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Y_UNUSED(requestType);
        Y_UNUSED(count);
        Y_UNUSED(bytes);
        Y_UNUSED(errors);
        Y_UNUSED(timeHist);
        Y_UNUSED(sizeHist);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDefaultVolumeProcessingPolicy
{
    TSet<TString> DiskIds;

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId)
    {
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        UNIT_ASSERT(!DiskIds.contains(volume.GetDiskId()));

        DiskIds.insert(volume.GetDiskId());
        return false;
    }

    void UnmountVolume(const TString& diskId, const TString& clientId)
    {
        Y_UNUSED(clientId);

        auto it = DiskIds.find(diskId);
        UNIT_ASSERT(it != DiskIds.end());

        DiskIds.erase(it);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId)
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
    }
};

template <typename TVolumeProcessingPolicy = TDefaultVolumeProcessingPolicy>
class TTestVolumeStats final
    : public IVolumeStats
    , public TVolumeProcessingPolicy
{
public:
    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        return TVolumeProcessingPolicy::MountVolume(
            volume,
            clientId,
            instanceId);
    }

    void UnmountVolume(const TString& diskId, const TString& clientId) override
    {
        TVolumeProcessingPolicy::UnmountVolume(diskId, clientId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        TVolumeProcessingPolicy::AlterVolume(diskId, cloudId, folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        return TVolumeProcessingPolicy::GetVolumeInfo(diskId, clientId);
    }

    IVolumeInfoPinPtr PinVolumeInfo(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
    }

    NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        Y_UNUSED(diskId);

        return 0;
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

    void TrimVolumes() override
    {}

    TVolumePerfStatuses GatherVolumePerfStatuses() override
    {
        return {};
    }

    TDowntimeHistory GetDowntimeHistory(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }

    bool HasStorageConfigPatch(const TString& diskId) const override
    {
        Y_UNUSED(diskId);
        return {};
    }
};

}   // namespace NCloud::NBlockStore
