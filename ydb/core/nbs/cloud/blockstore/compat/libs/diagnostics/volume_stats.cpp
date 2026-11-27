#include "volume_stats.h"

#include "config.h"
#include "stats_helpers.h"
#include "volume_perf.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/user_counter.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/volume_label.h>

#include <ydb/core/nbs/cloud/storage/core/compat/libs/common/media.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/verify.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/busy_idle_calculator.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/monitoring.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/postpone_time_predictor.h>

#include <ydb/core/util/tuples.h>

#include <library/cpp/containers/sorted_vector/sorted_vector.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/rwlock.h>

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace NCloud::NBlockStore {

using namespace NMonitoring;
using namespace NYdb::NBS::NStorage::NUserStats;

namespace NUserCounter = NYdb::NBS::NBlockStore::NUserCounter;

using NYdb::NBS::DEFAULT_BUCKET_COUNT;
using NYdb::NBS::DefaultBlockSize;
using NYdb::NBS::ECalcMaxTime;
using NYdb::NBS::IPostponeTimePredictorPtr;
using NYdb::NBS::TBusyIdleTimeCalculatorDynamicCounters;
using NYdb::NBS::TMaxCalculator;
using NYdb::NBS::TWellKnownEntityTypes;
using NYdb::NBS::UpdateCountersInterval;
using NYdb::NBS::NBlockStore::EDowntimeStateChange;
using NYdb::NBS::NBlockStore::TDowntimeHistoryHolder;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPostponeTimePredictorStats final
{
    TDynamicCounters::TCounterPtr MaxPredictedPostponeTimeCounter;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxPredictedPostponeTimeCalc;

public:
    TPostponeTimePredictorStats(
        TDynamicCountersPtr volumeGroup,
        ITimerPtr timer)
        : MaxPredictedPostponeTimeCounter(
              volumeGroup->GetCounter("MaxPredictedPostponeTime"))
        , MaxPredictedPostponeTimeCalc(std::move(timer))
    {}

    void OnRequestStarted(ui64 predictedPostponeTime)
    {
        MaxPredictedPostponeTimeCalc.Add(predictedPostponeTime);
    }

    void OnUpdateStats()
    {
        *MaxPredictedPostponeTimeCounter =
            MaxPredictedPostponeTimeCalc.NextValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDowntimeCalculator
{
private:
    using TMaxTimeCalculator = TMaxCalculator<DEFAULT_BUCKET_COUNT>;

    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const NProto::EStorageMediaKind MediaKind;

    TMaxTimeCalculator Read;
    TMaxTimeCalculator Write;
    TMaxTimeCalculator Zero;

public:
    TDowntimeCalculator(
        TDiagnosticsConfigPtr diagnosticsConfig,
        const NProto::TVolume& volume,
        ITimerPtr timer)
        : DiagnosticsConfig(std::move(diagnosticsConfig))
        , MediaKind(volume.GetStorageMediaKind())
        , Read(timer)
        , Write(timer)
        , Zero(timer)
    {}

    void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TDuration requestTime)
    {
        if (!IsReadWriteRequest(requestType)) {
            return;
        }

        auto& calc = GetCalculator(requestType);

        calc.Add(requestTime.MicroSeconds());
    }

    void RequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime)
    {
        if (!IsReadWriteRequest(requestType)) {
            return;
        }

        auto& calc = GetCalculator(requestType);

        auto requestTime =
            CyclesToDurationSafe(GetCycleCount() - requestStarted);

        calc.Add((requestTime - postponedTime).MicroSeconds());
    }

    bool OnUpdateStats()
    {
        auto readTime = Read.NextValue();
        auto writeTime = Write.NextValue();
        auto zeroTime = Zero.NextValue();

        auto maxTime = Max(readTime, Max(writeTime, zeroTime));

        return GetDowntimeThreshold(*DiagnosticsConfig, MediaKind) <=
               TDuration::MicroSeconds(maxTime);
    }

private:
    TMaxTimeCalculator& GetCalculator(EBlockStoreRequest requestType)
    {
        switch (requestType) {
            case EBlockStoreRequest::ReadBlocks: {
                return Read;
            }
            case EBlockStoreRequest::WriteBlocks: {
                return Write;
            }
            case EBlockStoreRequest::ZeroBlocks: {
                return Zero;
            }
            default: {
                Y_DEBUG_ABORT_UNLESS(
                    0,
                    "Unexpected requestType %d",
                    requestType);
                return Read;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeInfoBase
{
    const ITimerPtr Timer;
    const NProto::TVolume Volume;
    TBusyIdleTimeCalculatorDynamicCounters BusyIdleCalc;
    TVolumePerformanceCalculator PerfCalc;
    IPostponeTimePredictorPtr PostponeTimePredictor;
    TPostponeTimePredictorStats PostponeTimePredictorStats;
    TDowntimeCalculator DowntimeCalculator;
    TDowntimeHistoryHolder DowntimeHistory;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> ThrottlerRejects;
    TMaxCalculator<DEFAULT_BUCKET_COUNT> CheckpointRejects;
    TDynamicCounters::TCounterPtr HasStorageConfigPatchCounter;

    TVolumeInfoBase(
        NProto::TVolume volume,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IPostponeTimePredictorPtr postponeTimePredictor,
        TDynamicCountersPtr volumeGroup,
        ITimerPtr timer)
        : Timer(timer)
        , Volume(std::move(volume))
        , PerfCalc(Volume, diagnosticsConfig)
        , PostponeTimePredictor(std::move(postponeTimePredictor))
        , PostponeTimePredictorStats(volumeGroup, timer)
        , DowntimeCalculator(diagnosticsConfig, Volume, timer)
        , ThrottlerRejects(timer)
        , CheckpointRejects(timer)
        , HasStorageConfigPatchCounter(
              volumeGroup->GetCounter("HasStorageConfigPatch"))
    {
        BusyIdleCalc.Register(volumeGroup);
        PerfCalc.Register(*volumeGroup, Volume);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRealInstanceId
{
private:
    const TString ClientId;
    const TString InstanceId;
    const TString RealInstanceId;

public:
    TRealInstanceId(TString clientId, TString instanceId)
        : ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
        // in case of multi mount for empty instance, centers override itself
        // to avoid it use client ID for subgroup
        , RealInstanceId(InstanceId.empty() ? ClientId : InstanceId)
    {}

    const TString& GetClientId() const
    {
        return ClientId;
    }

    const TString& GetInstanceId() const
    {
        return InstanceId;
    }

    const TString& GetRealInstanceId() const
    {
        return RealInstanceId;
    }
};

struct TRealInstanceKeyHash
{
    std::size_t operator()(const TRealInstanceId& instance) const
    {
        return std::hash<TString>{}(instance.GetRealInstanceId());
    }
};

struct TRealInstanceKeyEqual
{
    bool operator()(
        const TRealInstanceId& lhs,
        const TRealInstanceId& rhs) const
    {
        return lhs.GetRealInstanceId() == rhs.GetRealInstanceId();
    }
};

class TVolumeInfo final: public IVolumeInfo
{
    friend class TVolumeStats;

private:
    const std::shared_ptr<TVolumeInfoBase> VolumeBase;
    const TRealInstanceId RealInstanceId;

    TRequestCounters RequestCounters;
    TDynamicCounters::TCounterPtr HasDowntimeCounter;

    // Cumulative per-volume availability counters (derivative/RATE, seconds).
    // Nested: ObservedSeconds >= AvailableSeconds >= HealthySeconds. Consumers
    // compute availability = Available/Observed and quality = Healthy/Observed
    // over a window. Advance only while the volume is being served,
    // until the counters are trimmed.
    TDynamicCounters::TCounterPtr ObservedSecondsCounter;
    TDynamicCounters::TCounterPtr AvailableSecondsCounter;
    TDynamicCounters::TCounterPtr HealthySecondsCounter;

    // Wall-clock time up to which the availability counters have been credited
    // for this instance. Seeded at construction (mount time) so that time
    // before the volume was served is never counted.
    TInstant AvailabilityLastUpdateTime;

    TInstant LastRemountTime;

    // Number of pins on the object.
    // An object with PinCount > 0 must not be removed by
    // InactiveClientsTimeout. Note: access to this field must be protected by
    // TVolumeStats::Lock
    size_t PinCount = 0;

    static TRequestCounters::EOptions GetRequestCountersOptions(
        const TVolumeInfoBase& volumeBase)
    {
        TRequestCounters::EOptions options =
            TRequestCounters::EOption::OnlyReadWriteRequests;

        auto mediaKind = volumeBase.Volume.GetStorageMediaKind();
        if (IsDiskRegistryMediaKind(mediaKind)) {
            options |= TRequestCounters::EOption::AddSpecialCounters;
        }

        return options;
    }

public:
    TVolumeInfo(
        std::shared_ptr<TVolumeInfoBase> volumeBase,
        ITimerPtr timer,
        TRealInstanceId realInstanceId,
        EHistogramCounterOptions histogramCounterOptions,
        const TVector<TSizeInterval>& executionTimeSizeClasses)
        : VolumeBase(std::move(volumeBase))
        , RealInstanceId(std::move(realInstanceId))
        , RequestCounters(MakeRequestCounters(
              std::move(timer),
              GetRequestCountersOptions(*VolumeBase),
              histogramCounterOptions,
              executionTimeSizeClasses))
        , AvailabilityLastUpdateTime(VolumeBase->Timer->Now())
    {}

    bool IsPinned() const noexcept
    {
        return PinCount > 0;
    }

    const NProto::TVolume& GetInfo() const override
    {
        return VolumeBase->Volume;
    }

    TDuration GetPossiblePostponeDuration() const override
    {
        return VolumeBase->PostponeTimePredictor->GetPossiblePostponeDuration();
    }

    ui64 RequestStarted(
        EBlockStoreRequest requestType,
        ui64 requestBytes) override
    {
        VolumeBase->PostponeTimePredictorStats.OnRequestStarted(
            GetPossiblePostponeDuration().MilliSeconds());
        VolumeBase->BusyIdleCalc.OnRequestStarted();
        return RequestCounters.RequestStarted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestBytes);
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
        VolumeBase->BusyIdleCalc.OnRequestCompleted();
        VolumeBase->PerfCalc.OnRequestCompleted(
            TranslateLocalRequestType(requestType),
            requestStarted,
            GetCycleCount(),   // requestCompleted
            DurationToCyclesSafe(
                postponedTime + backoffTime + shapingTime),   // waitTime
            requestBytes);
        VolumeBase->PostponeTimePredictor->Register(postponedTime);
        VolumeBase->DowntimeCalculator.RequestCompleted(
            TranslateLocalRequestType(requestType),
            requestStarted,
            postponedTime);

        if (errorKind == EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint)
        {
            VolumeBase->CheckpointRejects.Add(1);
        } else if (errorKind == EDiagnosticsErrorKind::ErrorThrottling) {
            VolumeBase->ThrottlerRejects.Add(1);
        }

        return RequestCounters
            .RequestCompleted(
                static_cast<TRequestCounters::TRequestType>(
                    TranslateLocalRequestType(requestType)),
                requestStarted,
                postponedTime,
                backoffTime,
                shapingTime,
                requestBytes,
                errorKind,
                errorFlags,
                unaligned,
                ECalcMaxTime::ENABLE,
                responseSent)
            .Time;
    }

    void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TRequestTime requestTime) override
    {
        RequestCounters.AddIncompleteStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            requestTime.ExecutionTime,
            requestTime.TotalTime,
            ECalcMaxTime::ENABLE);
        VolumeBase->DowntimeCalculator.AddIncompleteStats(
            TranslateLocalRequestType(requestType),
            requestTime.ExecutionTime);
    }

    void AddRetryStats(
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        if (errorKind == EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint)
        {
            VolumeBase->CheckpointRejects.Add(1);
        } else if (errorKind == EDiagnosticsErrorKind::ErrorThrottling) {
            VolumeBase->ThrottlerRejects.Add(1);
        }

        RequestCounters.AddRetryStats(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            errorKind,
            errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestPostponed(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestPostponedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestAdvanced(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestAdvancedServer(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        RequestCounters.RequestFastPathHit(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)));
    }

    void BatchCompleted(
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        return RequestCounters.BatchCompleted(
            static_cast<TRequestCounters::TRequestType>(
                TranslateLocalRequestType(requestType)),
            count,
            bytes,
            errors,
            timeHist,
            sizeHist);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeStats;
using TVolumeStatsPtr = std::shared_ptr<TVolumeStats>;

class TVolumeStats final
    : public IVolumeStats
    , public std::enable_shared_from_this<TVolumeStats>
{
    using TClientVolume = std::pair<TString, TString>;   // [clientId, diskId]
    using TVolumeBasePtr = std::shared_ptr<TVolumeInfoBase>;
    using TVolumeInfoPtr = std::shared_ptr<TVolumeInfo>;
    using TVolumeMap = std::unordered_map<
        TRealInstanceId,
        TVolumeInfoPtr,
        TRealInstanceKeyHash,
        TRealInstanceKeyEqual>;

    struct TVolumeInfoHolder
    {
        TVolumeBasePtr VolumeBase;
        TVolumeMap VolumeInfos;
    };

    using TVolumeHolderMap = std::unordered_map<TString, TVolumeInfoHolder>;

    class TVolumeInfoPin: public IVolumeInfoPin
    {
    public:
        TVolumeInfoPin(
            TVolumeStatsPtr volumeStats,
            TString diskId,
            TString clientId)
            : VolumeStats(std::move(volumeStats))
            , DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
        {
            Pinned = VolumeStats->IncVolumeInfoPinCounter(DiskId, ClientId);
        }

        ~TVolumeInfoPin() override
        {
            if (Pinned) {
                VolumeStats->DecVolumeInfoPinCounter(DiskId, ClientId);
            }
        }

        bool IsPinned() const noexcept
        {
            return Pinned;
        }

    private:
        TVolumeStatsPtr VolumeStats;
        const TString DiskId;
        const TString ClientId;
        bool Pinned = false;
    };

private:
    const IMonitoringServicePtr Monitoring;
    const TDuration InactiveClientsTimeout;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const EVolumeStatsType Type;
    const ITimerPtr Timer;
    const THashSet<TString> CloudIdsWithStrictSLA;

    TVector<TSizeInterval> ExecutionTimeSizeClasses =
        DiagnosticsConfig->GetExecutionTimeSizeClasses();

    TDynamicCountersPtr Counters;
    // Separate, narrow counters tree (component=sli_volume) for the
    // cumulative availability counters (ObservedSeconds/AvailableSeconds/
    // HealthySeconds) only. The main per-volume tree (component=
    // server_volume / client_volume) also carries ~200-280 other per-volume
    // perf counters, so scraping the whole subtree just to reach our ~3
    // sensors is wasteful; this sibling group lets a monitoring agent pull
    // only these specific counters. Populated for both EServerStats and
    // EClientStats in InitCounters; see RegisterInstance() for the per-volume
    // subgroup layout.
    TDynamicCountersPtr AvailabilityCounters;
    std::shared_ptr<NUserCounter::IUserCounterSupplier> UserCounters;
    std::unique_ptr<TSufferCounters> SufferCounters;
    std::unique_ptr<TSufferCounters> SmoothSufferCounters;
    std::unique_ptr<TSufferCounters> StrictSLASufferCounters;
    std::unique_ptr<TSufferCounters> CriticalSufferCounters;

    std::unordered_map<TClientVolume, TRealInstanceId>
        ClientVolumeToRealInstance;
    TVolumeHolderMap Volumes;
    TRWMutex Lock;

    using TDownDisksCounters = std::array<
        TDynamicCounters::TCounterPtr,
        NProto::EStorageMediaKind_ARRAYSIZE>;
    TDownDisksCounters DownDisksCounters;
    TDynamicCounters::TCounterPtr TotalDownDisksCounter;

public:
    TVolumeStats(
        IMonitoringServicePtr monitoring,
        TDuration inactiveClientsTimeout,
        TDiagnosticsConfigPtr diagnosticsConfig,
        EVolumeStatsType type,
        ITimerPtr timer)
        : Monitoring(std::move(monitoring))
        , InactiveClientsTimeout(inactiveClientsTimeout)
        , DiagnosticsConfig(std::move(diagnosticsConfig))
        , Type(type)
        , Timer(std::move(timer))
        , CloudIdsWithStrictSLA(
              [](const TVector<TString>& v)
              {
                  return THashSet<TString>(v.begin(), v.end());
              }(DiagnosticsConfig->GetCloudIdsWithStrictSLA()))
        , UserCounters(CreateUserCounterSupplier())
    {}

    // Not thread-safe
    bool MountVolumeImpl(
        NProto::TVolume volume,
        const TRealInstanceId& realInstanceId,
        size_t pinCountForNewInstance)
    {
        bool inserted = false;

        volume.SetDiskId(NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(
            volume.GetDiskId()));

        auto volumeIt = Volumes.find(volume.GetDiskId());
        if (volumeIt == Volumes.end()) {
            volumeIt =
                Volumes.emplace(volume.GetDiskId(), RegisterVolume(volume))
                    .first;
        }

        TVolumeMap& infos = volumeIt->second.VolumeInfos;

        auto volumeInfoIt = infos.find(realInstanceId);
        if (volumeInfoIt == infos.end()) {
            auto volumeInfo =
                RegisterInstance(volumeIt->second.VolumeBase, realInstanceId);
            volumeInfoIt = infos.emplace(realInstanceId, volumeInfo).first;
            inserted = true;
            volumeInfoIt->second->PinCount = pinCountForNewInstance;
        }

        volumeInfoIt->second->LastRemountTime = Timer->Now();

        AlterVolumeImpl(
            volume.GetDiskId(),
            volume.GetCloudId(),
            volume.GetFolderId(),
            volume.GetStorageMediaKind());

        return inserted;
    }

    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        TWriteGuard guard(Lock);

        const auto& diskId = NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(
            volume.GetDiskId());
        auto [it, _] = ClientVolumeToRealInstance.try_emplace(
            {clientId, diskId},
            clientId,
            instanceId);

        return MountVolumeImpl(
            volume,
            it->second,
            0 /* pinCountForNewInstance */);
    }

    void UnmountVolume(const TString& diskId, const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        // No actions.

        // VolumeInfos are removed only by timeout via TrimVolumes(), since
        // multiple endpoints can exist for a single VolumeInfo (diskId -
        // clientId pair). Each endpoint may be either durable (with guaranteed
        // UnmountVolume) or non-durable (gRPC-like).
        // E.g. multiple diskId-clientId pairs may appear due to live local
        // migration within the same host, where both read-only and read-write
        // mounts are allowed simultaneously
    }

    // Not thread-safe
    void AlterVolumeImpl(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId,
        NProto::EStorageMediaKind storageMediaKind)
    {
        const auto volumeIt = Volumes.find(diskId);
        if (volumeIt == Volumes.end()) {
            return;
        }

        NProto::TVolume volumeConfig = volumeIt->second.VolumeBase->Volume;
        if (volumeConfig.GetCloudId() == cloudId &&
            volumeConfig.GetFolderId() == folderId &&
            volumeConfig.GetStorageMediaKind() == storageMediaKind)
        {
            return;
        }

        volumeConfig.SetCloudId(cloudId);
        volumeConfig.SetFolderId(folderId);
        volumeConfig.SetStorageMediaKind(storageMediaKind);

        TVolumeInfoHolder holder = std::move(volumeIt->second);
        Volumes.erase(volumeIt);

        for (const auto& item: holder.VolumeInfos) {
            const TVolumeInfo& info = *item.second;
            UnregisterInstance(info.VolumeBase, info.RealInstanceId);
        }
        UnregisterVolume(holder.VolumeBase);

        for (const auto& item: holder.VolumeInfos) {
            const TVolumeInfo& info = *item.second;
            MountVolumeImpl(volumeConfig, info.RealInstanceId, info.PinCount);
        }
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        TWriteGuard guard(Lock);

        const auto& logicalDiskId =
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId);
        const auto volumeIt = Volumes.find(logicalDiskId);
        if (volumeIt == Volumes.end()) {
            return;
        }

        AlterVolumeImpl(
            logicalDiskId,
            cloudId,
            folderId,
            volumeIt->second.VolumeBase->Volume.GetStorageMediaKind());
    }

    // Not thread-safe
    TVolumeInfoPtr GetVolumeInfoImpl(
        const TString& diskId,
        const TString& clientId) const
    {
        const auto& logicalDiskId =
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId);

        const auto volumeIt = Volumes.find(logicalDiskId);
        if (volumeIt == Volumes.end()) {
            return nullptr;
        }

        const TVolumeMap& infos = volumeIt->second.VolumeInfos;

        const auto realInstanceIt =
            ClientVolumeToRealInstance.find(std::tie(clientId, logicalDiskId));

        if (realInstanceIt == ClientVolumeToRealInstance.end()) {
            return nullptr;
        }
        const auto infoIt = infos.find(realInstanceIt->second);
        if (infoIt == infos.end()) {
            return nullptr;
        }
        return infoIt->second;
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        TReadGuard guard(Lock);

        return GetVolumeInfoImpl(diskId, clientId);
    }

    /**
     * Increment corresponding VolumeInfo::PinCounter
     *
     * Thread-safe
     *
     * @return
     *  true  - increment succeeded
     *  false - no VolumeInfo found for the specified [diskId, clientId]
     */
    bool IncVolumeInfoPinCounter(const TString& diskId, const TString& clientId)
    {
        TWriteGuard guard(Lock);

        auto volumeInfo = GetVolumeInfoImpl(diskId, clientId);
        if (!volumeInfo) {
            return false;
        }

        STORAGE_VERIFY(   // PinCount corruption
            volumeInfo->PinCount <
                std::numeric_limits<decltype(volumeInfo->PinCount)>::max(),
            TWellKnownEntityTypes::DISK,
            diskId);

        volumeInfo->PinCount++;

        return true;
    }

    /**
     * Decrement corresponding VolumeInfo::PinCounter
     *
     * Thread-safe
     *
     * VolumeInfo with specified [diskId, clientId] must exist and be pinned.
     */
    void DecVolumeInfoPinCounter(const TString& diskId, const TString& clientId)
    {
        TWriteGuard guard(Lock);

        auto volumeInfo = GetVolumeInfoImpl(diskId, clientId);

        STORAGE_VERIFY(volumeInfo, TWellKnownEntityTypes::DISK, diskId);

        STORAGE_VERIFY(
            volumeInfo->PinCount > 0,
            TWellKnownEntityTypes::DISK,
            diskId);

        volumeInfo->PinCount--;
    }

    [[nodiscard]] IVolumeInfoPinPtr PinVolumeInfo(
        const TString& diskId,
        const TString& clientId) override
    {
        if (!DiagnosticsConfig->GetEnableDurableVolumeInfo()) {
            // Stub
            return MakeIntrusive<IVolumeInfoPin>();
        }

        auto pin = MakeIntrusive<TVolumeInfoPin>(
            shared_from_this(),
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId),
            clientId);

        return pin->IsPinned() ? pin : nullptr;
    }

    NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId));
        return volumeIt != Volumes.end()
                   ? volumeIt->second.VolumeBase->Volume.GetStorageMediaKind()
                   : NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    }

    ui32 GetBlockSize(const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId));
        return volumeIt != Volumes.end()
                   ? volumeIt->second.VolumeBase->Volume.GetBlockSize()
                   : DefaultBlockSize;
    }

    bool TrimInstance(TInstant now, TVolumeMap& infos)
    {
        std::erase_if(
            infos,
            [this, now](const auto& item)
            {
                const TVolumeInfo& info = *item.second;

                // clang-format off
                const bool removeInstance =
                       !info.IsPinned()
                    && InactiveClientsTimeout
                    && now - info.LastRemountTime > InactiveClientsTimeout;
                // clang-format on

                if (removeInstance) {
                    UnregisterInstance(info.VolumeBase, info.RealInstanceId);

                    const auto& diskId = info.VolumeBase->Volume.GetDiskId();
                    std::erase_if(
                        ClientVolumeToRealInstance,
                        [&info, &diskId](const auto& mapElement)
                        {
                            const TClientVolume& clientVolume =
                                mapElement.first;
                            const TRealInstanceId& realInstanceId =
                                mapElement.second;
                            const bool erase = clientVolume.second == diskId &&
                                               TRealInstanceKeyEqual()(
                                                   realInstanceId,
                                                   info.RealInstanceId);
                            return erase;
                        });
                    return true;
                }
                return false;
            });
        return infos.empty();
    }

    // Not thread-safe
    void TrimVolumesImpl()
    {
        const auto now = Timer->Now();

        std::erase_if(
            Volumes,
            [this, now](auto& item)
            {
                TVolumeInfoHolder& holder = item.second;
                if (TrimInstance(now, holder.VolumeInfos)) {
                    UnregisterVolume(holder.VolumeBase);
                    return true;
                }
                return false;
            });
    }

    void TrimVolumes() override
    {
        TWriteGuard guard(Lock);

        TrimVolumesImpl();
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        TReadGuard guard(Lock);

        ui32 totalDownDisks = 0;
        std::array<ui32, NProto::EStorageMediaKind_ARRAYSIZE>
            downDisksCounters{};

        // Wall-clock time of this stats tick. The cumulative availability
        // counters advance on every tick (not only on the publish tick) by the
        // real per-volume elapsed time, so state changes are sampled at tick
        // resolution and newly mounted volumes are not over-credited.
        const auto now = Timer->Now();

        for (auto& [logicalDiskId, holder]: Volumes) {
            TVolumeInfoBase& volumeBase = *holder.VolumeBase;

            volumeBase.PostponeTimePredictorStats.OnUpdateStats();
            volumeBase.BusyIdleCalc.OnUpdateStats();
            volumeBase.PerfCalc.UpdateStats();

            const auto hasDowntime =
                volumeBase.DowntimeCalculator.OnUpdateStats();
            if (hasDowntime) {
                ++totalDownDisks;
                ++downDisksCounters[volumeBase.Volume.GetStorageMediaKind()];
            }

            if (updateIntervalFinished) {
                volumeBase.DowntimeHistory.PushBack(
                    now,
                    hasDowntime ? EDowntimeStateChange::DOWN
                                : EDowntimeStateChange::UP);
            }

            const bool isSufferingCritically =
                volumeBase.PerfCalc.IsSufferingCritically();
            const bool isAvailable = !hasDowntime;
            const bool isHealthy = isAvailable && !isSufferingCritically;

            for (auto& [key, instance]: holder.VolumeInfos) {
                instance->RequestCounters.UpdateStats(updateIntervalFinished);
                if (updateIntervalFinished) {
                    Y_DEBUG_ABORT_UNLESS(instance->HasDowntimeCounter);
                    if (instance->HasDowntimeCounter) {
                        *instance->HasDowntimeCounter = hasDowntime;
                    }
                }

                // Advance the cumulative availability counters by the real time
                // this instance has been served since the last accounted tick.
                // Sampling every tick (not only on the publish tick) tracks the
                // downtime/suffering signal at tick resolution; the
                // per-instance timestamp (seeded at mount) avoids crediting
                // time before the volume was served; advancing the timestamp
                // only by the whole seconds credited keeps the sub-second
                // remainder and avoids drift.
                if (instance->ObservedSecondsCounter &&
                    now > instance->AvailabilityLastUpdateTime)
                {
                    const auto elapsed =
                        now - instance->AvailabilityLastUpdateTime;
                    if (elapsed > UpdateCountersInterval) {
                        // A forward gap larger than the publish interval means
                        // stats were not updated for a long time (thread
                        // starvation, a suspended process, or a wall-clock
                        // jump). Crediting the whole gap would count that
                        // "stall" time as availability, so drop this increment
                        // and just resync the timestamp.
                        instance->AvailabilityLastUpdateTime = now;
                    } else {
                        const ui64 seconds = elapsed.Seconds();
                        if (seconds) {
                            *instance->ObservedSecondsCounter += seconds;
                            if (isAvailable &&
                                instance->AvailableSecondsCounter)
                            {
                                *instance->AvailableSecondsCounter += seconds;
                            }
                            if (isHealthy && instance->HealthySecondsCounter) {
                                *instance->HealthySecondsCounter += seconds;
                            }
                            instance->AvailabilityLastUpdateTime +=
                                TDuration::Seconds(seconds);
                        }
                    }
                }
            }
            if (SufferCounters && volumeBase.PerfCalc.IsSuffering()) {
                SufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());
            }
            if (SmoothSufferCounters && volumeBase.PerfCalc.IsSufferingSmooth())
            {
                SmoothSufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());

                const auto& cloudId = volumeBase.Volume.GetCloudId();
                if (StrictSLASufferCounters &&
                    CloudIdsWithStrictSLA.contains(cloudId))
                {
                    StrictSLASufferCounters->OnDiskSuffer(
                        volumeBase.Volume.GetStorageMediaKind());
                }
            }
            if (CriticalSufferCounters &&
                volumeBase.PerfCalc.IsSufferingCritically())
            {
                CriticalSufferCounters->OnDiskSuffer(
                    volumeBase.Volume.GetStorageMediaKind());
            }
        }

        if (SufferCounters) {
            SufferCounters->PublishCounters();
        }
        if (SmoothSufferCounters) {
            SmoothSufferCounters->PublishCounters();
        }
        if (StrictSLASufferCounters) {
            StrictSLASufferCounters->PublishCounters();
        }
        if (CriticalSufferCounters) {
            CriticalSufferCounters->PublishCounters();
        }

        if (updateIntervalFinished) {
            if (TotalDownDisksCounter) {
                *TotalDownDisksCounter = totalDownDisks;
            }

            // Two-phase set to combine counters instead of overwriting them
            // (and hide prev value) in case of some NProto::EStorageMediaKind
            // attached to a single DownDisksCounters (e.g. HYBRID attached to
            // HDD counters, see MediaKindToStatsString())

            for (size_t i = 0; i < DownDisksCounters.size(); i++) {
                if (DownDisksCounters[i]) {
                    *DownDisksCounters[i] = 0;
                }
            }

            for (size_t i = 0; i < DownDisksCounters.size(); i++) {
                if (DownDisksCounters[i]) {
                    *DownDisksCounters[i] += downDisksCounters[i];
                }
            }
        }
    }

    TVolumePerfStatuses GatherVolumePerfStatuses() override
    {
        TReadGuard guard(Lock);
        TVolumePerfStatuses ans(Reserve(Volumes.size()));

        for (const auto& [logicalDiskId, holder]: Volumes) {
            const TVolumeInfoBase& volumeBase = *holder.VolumeBase;
            ans.emplace_back(
                volumeBase.Volume.GetDiskId(),
                volumeBase.PerfCalc.GetSufferCount());
        }
        return ans;
    }

    NCloud::NStorage::IUserMetricsSupplierPtr GetUserCounters() const override
    {
        return UserCounters;
    }

    TDowntimeHistory GetDowntimeHistory(const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId));
        if (volumeIt == Volumes.end()) {
            return {};
        }

        return volumeIt->second.VolumeBase->DowntimeHistory.RecentEvents(
            Timer->Now());
    }

    bool HasStorageConfigPatch(const TString& diskId) const override
    {
        TReadGuard guard(Lock);

        const auto volumeIt = Volumes.find(
            NYdb::NBS::NBlockStore::NStorage::GetLogicalDiskId(diskId));
        if (volumeIt == Volumes.end()) {
            return {};
        }

        return volumeIt->second.VolumeBase->HasStorageConfigPatchCounter->Val();
    }

private:
    TVolumeInfoHolder RegisterVolume(NProto::TVolume volume)
    {
        if (!Counters) {
            InitCounters();
        }

        auto volumeGroup = Counters->GetSubgroup("volume", volume.GetDiskId());

        auto volumeBase = std::make_shared<TVolumeInfoBase>(
            std::move(volume),
            DiagnosticsConfig,
            CreatePostponeTimePredictor(
                Timer,
                DiagnosticsConfig->GetPostponeTimePredictorInterval(),
                DiagnosticsConfig->GetPostponeTimePredictorPercentage(),
                DiagnosticsConfig->GetPostponeTimePredictorMaxTime()),
            volumeGroup,
            Timer);

        return TVolumeInfoHolder{
            .VolumeBase = std::move(volumeBase),
            .VolumeInfos = {}};
    }

    TVolumeInfoPtr RegisterInstance(
        TVolumeBasePtr volumeBase,
        const TRealInstanceId& realInstanceId)
    {
        auto info = std::make_shared<TVolumeInfo>(
            volumeBase,
            Timer,
            realInstanceId,
            DiagnosticsConfig->GetHistogramCounterOptions(),
            ExecutionTimeSizeClasses);

        if (!Counters) {
            InitCounters();
        }

        const NProto::TVolume& volumeConfig = volumeBase->Volume;

        auto volumeGroup =
            Counters->GetSubgroup("volume", volumeConfig.GetDiskId());
        auto countersGroup =
            volumeGroup
                ->GetSubgroup("instance", realInstanceId.GetRealInstanceId())
                ->GetSubgroup("cloud", volumeConfig.GetCloudId())
                ->GetSubgroup("folder", volumeConfig.GetFolderId())
                ->GetSubgroup(
                    "type",
                    MediaKindToStatsString(volumeConfig.GetStorageMediaKind()));
        info->RequestCounters.Register(*countersGroup);
        info->HasDowntimeCounter = countersGroup->GetCounter("HasDowntime");

        // Register the cumulative counters in the narrow component=sli_volume
        // tree (see AvailabilityCounters comment).
        // "type" uses MediaKindToComputeType (not MediaKindToStatsString, the
        // convention used by the DownDisks aggregate below) to produce the
        // same disk-type spelling ("network-ssd", ...) this helper already
        // produces elsewhere in the codebase.
        // All per-instance trees are rebuilt when a remount changes any label
        // used in their subgroup chains.
        auto availabilityCountersGroup =
            AvailabilityCounters
                ->GetSubgroup("volume", volumeConfig.GetDiskId())
                ->GetSubgroup("instance", realInstanceId.GetRealInstanceId())
                ->GetSubgroup("cloud", volumeConfig.GetCloudId())
                ->GetSubgroup("folder", volumeConfig.GetFolderId())
                ->GetSubgroup(
                    "type",
                    MediaKindToComputeType(volumeConfig.GetStorageMediaKind()));
        info->ObservedSecondsCounter =
            availabilityCountersGroup->GetCounter("ObservedSeconds", true);
        info->AvailableSecondsCounter =
            availabilityCountersGroup->GetCounter("AvailableSeconds", true);
        info->HealthySecondsCounter =
            availabilityCountersGroup->GetCounter("HealthySeconds", true);

        auto reportZeroBlocksMetrics =
            !DiagnosticsConfig
                 ->GetSkipReportingZeroBlocksMetricsForYDBBasedDisks() ||
            IsDiskRegistryMediaKind(volumeConfig.GetStorageMediaKind());
        NUserCounter::RegisterServerVolumeInstance(
            *UserCounters,
            volumeConfig.GetCloudId(),
            volumeConfig.GetFolderId(),
            volumeConfig.GetDiskId(),
            realInstanceId.GetInstanceId(),
            reportZeroBlocksMetrics,
            DiagnosticsConfig->GetHistogramCounterOptions(),
            countersGroup);

        return info;
    }

    void UnregisterInstance(
        TVolumeBasePtr volumeBase,
        const TRealInstanceId& realInstanceId)
    {
        if (!Counters) {
            InitCounters();
        }

        Counters->GetSubgroup("volume", volumeBase->Volume.GetDiskId())
            ->RemoveSubgroup("instance", realInstanceId.GetRealInstanceId());

        AvailabilityCounters
            ->GetSubgroup("volume", volumeBase->Volume.GetDiskId())
            ->RemoveSubgroup("instance", realInstanceId.GetRealInstanceId());

        NUserCounter::UnregisterServerVolumeInstance(
            *UserCounters,
            volumeBase->Volume.GetCloudId(),
            volumeBase->Volume.GetFolderId(),
            volumeBase->Volume.GetDiskId(),
            realInstanceId.GetInstanceId());
    }

    void UnregisterVolume(TVolumeBasePtr volumeBase)
    {
        if (!Counters) {
            InitCounters();
        }

        Counters->RemoveSubgroup("volume", volumeBase->Volume.GetDiskId());

        AvailabilityCounters->RemoveSubgroup(
            "volume",
            volumeBase->Volume.GetDiskId());
    }

    void InitCounters()
    {
        Counters =
            Monitoring->GetCounters()->GetSubgroup("counters", "blockstore");

        switch (Type) {
            case EVolumeStatsType::EServerStats: {
                SufferCounters = std::make_unique<TSufferCounters>(
                    "DisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                SmoothSufferCounters = std::make_unique<TSufferCounters>(
                    "SmoothDisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                StrictSLASufferCounters = std::make_unique<TSufferCounters>(
                    "StrictSLADisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                CriticalSufferCounters = std::make_unique<TSufferCounters>(
                    "CriticalDisksSuffer",
                    Counters->GetSubgroup("component", "server"));

                TotalDownDisksCounter =
                    Counters->GetSubgroup("component", "server")
                        ->GetCounter("DownDisks");

                ui32 mk = NProto::EStorageMediaKind_MIN;
                while (mk < NProto::EStorageMediaKind_ARRAYSIZE) {
                    DownDisksCounters[mk] =
                        Counters->GetSubgroup("component", "server")
                            ->GetSubgroup(
                                "type",
                                MediaKindToStatsString(
                                    static_cast<NProto::EStorageMediaKind>(mk)))
                            ->GetCounter("DownDisks");
                    ++mk;
                }

                AvailabilityCounters =
                    Counters->GetSubgroup("component", "sli_volume")
                        ->GetSubgroup("host", "cluster");

                Counters = Counters->GetSubgroup("component", "server_volume");
                break;
            }
            case EVolumeStatsType::EClientStats: {
                AvailabilityCounters =
                    Counters->GetSubgroup("component", "sli_volume")
                        ->GetSubgroup("host", "cluster");

                Counters = Counters->GetSubgroup("component", "client_volume");
                break;
            }
        }

        Counters = Counters->GetSubgroup("host", "cluster");
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeStatsStub final: public IVolumeStats
{
    bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);

        return true;
    }

    void UnmountVolume(const TString& diskId, const TString& clientId) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(diskId);
    }

    void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(cloudId);
        Y_UNUSED(folderId);
    }

    IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);

        return nullptr;
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

        return DefaultBlockSize;
    }

    void TrimVolumes() override
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer)
{
    Y_DEBUG_ABORT_UNLESS(diagnosticsConfig);
    return std::make_shared<TVolumeStats>(
        std::move(monitoring),
        inactiveClientsTimeout,
        std::move(diagnosticsConfig),
        type,
        std::move(timer));
}

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer)
{
    NProto::TDiagnosticsConfig diagnosticsConfig;
    return std::make_shared<TVolumeStats>(
        std::move(monitoring),
        inactiveClientsTimeout,
        std::make_shared<TDiagnosticsConfig>(diagnosticsConfig),
        type,
        std::move(timer));
}

IVolumeStatsPtr CreateVolumeStatsStub()
{
    return std::make_shared<TVolumeStatsStub>();
}

}   // namespace NCloud::NBlockStore
