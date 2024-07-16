#pragma once

#include "immediate_control_board_control.h"
#include <optional>
#include <string_view>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NKikimr {

template <typename First, typename Second>
struct SwitchTypesDetected final {
    using first_type = First;
    using second_type = Second;
    constexpr SwitchTypesDetected& Case(First, Second) noexcept { 
        return *this;
    }
};
struct SwitchTypesDetector final {
    template <typename First, typename Second>
    constexpr auto Case(First, Second) noexcept {
        using first_type = std::conditional_t<std::is_convertible_v<First, std::string_view>, std::string_view, First>;
        using second_type = std::conditional_t<std::is_convertible_v<Second, std::string_view>, std::string_view, Second>;
        return SwitchTypesDetected<first_type, second_type>{};
    }
};

namespace {
template <typename First, typename Second>
class SwitchByFirst final {
public:
    constexpr explicit SwitchByFirst(First search) noexcept : search_(search) {}
    constexpr SwitchByFirst& Case(First first, Second second) noexcept {
        if (!result_ && search_ == first) {
            result_.emplace(second);
        }
        return *this;
    }
    [[nodiscard]] constexpr std::optional<Second> Extract() noexcept {
        return result_;
    }

private:
    const First search_;
    std::optional<Second> result_{};
};

template <typename BuilderFunc>
class TrivialBiMap final {
    using TypesPair = std::invoke_result_t<const BuilderFunc&, SwitchTypesDetector>;

public:
    using First = typename TypesPair::first_type;
    using Second = typename TypesPair::second_type;
    constexpr TrivialBiMap(BuilderFunc func) noexcept
        : func_{std::move(func)}
    {}
    constexpr std::optional<Second> TryFindByFirst(First value) const noexcept {
        return func_(SwitchByFirst<First, Second>{value}).Extract();
    }

private:
    const BuilderFunc func_;
};

} // namespace

class TControlBoard : public TThrRefBase {

    #define REGISTER_IMMEDIATE_SHARED_CONTROL(icb, valueWrapper, name) \
        { \
            TGuard guard((icb). name##Mutex ); \
            if (!(icb).(name)) { \
                (name) = (valueWrapper).Control; \
            } else {\
                (valueWrapper).Control = (name) \
            } \
        }
    
    #define REGISTER_IMMEDIATE_LOCAL_CONTROL(icb, valueWrapper, name) \
        { \
            TGuard guard((icb). name##Mutex ); \
            if (!(icb).(name)) { \
                (name) = (valueWrapper).Control; \
            } \
        }

private:
    enum class EControls : ui8 {
        DataShardControlsDisableByKeyFilter = 0,
        DataShardControlsMaxTxInFly = 1,
        DataShardControlsMaxTxLagMilliseconds = 2,
        DataShardControlsDataTxProfileLogThresholdMs = 3,
        DataShardControlsDataTxProfileBufferThresholdMs = 4,
        DataShardControlsDataTxProfileBufferSize = 5,
        DataShardControlsCanCancelROWithReadSets = 6,
        TxLimitControlsPerShardReadSizeLimit = 7,
        DataShardControlsCpuUsageReportThreshlodPercent = 8,
        DataShardControlsCpuUsageReportIntervalSeconds = 9,
        DataShardControlsHighDataSizeReportThreshlodBytes = 10,
        DataShardControlsHighDataSizeReportIntervalSeconds = 11,
        DataShardControlsBackupReadAheadLo = 12,
        DataShardControlsBackupReadAheadHi = 13,
        DataShardControlsTtlReadAheadLo = 14,
        DataShardControlsTtlReadAheadHi = 15,
        DataShardControlsEnableLockedWrites = 16,
        DataShardControlsMaxLockedWritesPerKey = 17,
        DataShardControlsEnableLeaderLeases = 18,
        DataShardControlsMinLeaderLeaseDurationUs = 19,
        DataShardControlsChangeRecordDebugPrint = 20,
        
        BlobStorageEnablePutBatching = 21,
        BlobStorageEnableVPatch = 22,
        VDiskControlsEnableLocalSyncLogDataCutting = 23,
        VDiskControlsEnableSyncLogChunkCompressionHDD = 24,
        VDiskControlsEnableSyncLogChunkCompressionSSD = 25,
        VDiskControlsMaxSyncLogChunksInFlightHDD = 26,
        VDiskControlsMaxSyncLogChunksInFlightSSD = 27,

        VDiskControlsBurstThresholdNsHDD = 28,
        VDiskControlsBurstThresholdNsSSD = 29,
        VDiskControlsBurstThresholdNsNVME = 30,
        VDiskControlsDiskTimeAvailableScaleHDD = 31,
        VDiskControlsDiskTimeAvailableScaleSSD = 32,
        VDiskControlsDiskTimeAvailableScaleNVME = 33,

        SchemeShardSplitMergePartCountLimit = 34,
        SchemeShardFastSplitSizeThreshold = 35,
        SchemeShardFastSplitRowCountThreshold = 36,
        SchemeShardFastSplitCpuPercentageThreshold = 37,
        
        SchemeShardSplitByLoadEnabled = 38,
        SchemeShardSplitByLoadMaxShardsDefault = 39,
        SchemeShardMergeByLoadMinUptimeSec = 40,
        SchemeShardMergeByLoadMinLowLoadDurationSec = 41,

        SchemeShardControlsForceShardSplitDataSize = 42,
        SchemeShardControlsDisableForceShardSplit = 43,

        TCMallocControlsProfileSamplingRate = 44,
        TCMallocControlsGuardedSamplingRate = 45,
        TCMallocControlsMemoryLimit = 46,
        TCMallocControlsPageCacheTargetSize = 47,
        TCMallocControlsPageCacheReleaseRate = 48,

        ColumnShardControlsMinBytesToIndex = 49,
        ColumnShardControlsMaxBytesToIndex = 50,
        ColumnShardControlsInsertTableCommittedSize = 51,

        ColumnShardControlsIndexGoodBlobSize = 52,
        ColumnShardControlsGranuleOverloadBytes = 53,
        ColumnShardControlsCompactionDelaySec = 54,
        ColumnShardControlsGranuleIndexedPortionsSizeLimit = 55,
        ColumnShardControlsGranuleIndexedPortionsCountLimit = 56,

        BlobCacheMaxCacheDataSize = 57,
        BlobCacheMaxInFlightDataSize = 58,

        ColumnShardControlsBlobWriteGrouppingEnabled = 59,
        ColumnShardControlsCacheDataAfterIndexing = 60,
        ColumnShardControlsCacheDataAfterCompaction = 61,
        
        CoordinatorControlsEnableLeaderLeases = 62,
        CoordinatorControlsMinLeaderLeaseDurationUs = 63,
        CoordinatorControlsVolatilePlanLeaseMs = 64,
        CoordinatorControlsPlanAheadTimeShiftMs = 65,
        
        SchemeShardAllowConditionalEraseOperations = 66,
        SchemeShardDisablePublicationsOfDropping = 67,
        SchemeShardFillAllocatePQ = 68,
        SchemeShardAllowDataColumnForIndexTable = 69,
        SchemeShardAllowServerlessStorageBilling = 70,

        ControlsNo = 71
    };

    constexpr static TrivialBiMap Mapping = [](auto selector) {
        return selector
            .Case("DataShardControls.DisableByKeyFilter", EControls::DataShardControlsDisableByKeyFilter)
            .Case("DataShardControls.MaxTxInFly", EControls::DataShardControlsMaxTxInFly)
            .Case("DataShardControls.MaxTxLagMilliseconds", EControls::DataShardControlsMaxTxLagMilliseconds)
            .Case("DataShardControls.DataTxProfile.LogThresholdMs", EControls::DataShardControlsDataTxProfileLogThresholdMs)
            .Case("DataShardControls.DataTxProfile.BufferThresholdMs", EControls::DataShardControlsDataTxProfileBufferThresholdMs)
            .Case("DataShardControls.DataTxProfile.BufferSize", EControls::DataShardControlsDataTxProfileBufferSize)
            .Case("DataShardControls.CanCancelROWithReadSets", EControls::DataShardControlsCanCancelROWithReadSets)
            .Case("TxLimitControls.PerShardReadSizeLimit", EControls::TxLimitControlsPerShardReadSizeLimit)
            .Case("DataShardControls.CpuUsageReportThreshlodPercent", EControls::DataShardControlsCpuUsageReportThreshlodPercent)
            .Case("DataShardControls.CpuUsageReportIntervalSeconds", EControls::DataShardControlsCpuUsageReportIntervalSeconds)
            .Case("DataShardControls.HighDataSizeReportThreshlodBytes", EControls::DataShardControlsHighDataSizeReportThreshlodBytes)
            .Case("DataShardControls.HighDataSizeReportIntervalSeconds", EControls::DataShardControlsHighDataSizeReportIntervalSeconds)
            .Case("DataShardControls.BackupReadAheadLo", EControls::DataShardControlsBackupReadAheadLo)
            .Case("DataShardControls.BackupReadAheadHi", EControls::DataShardControlsBackupReadAheadHi)
            .Case("DataShardControls.TtlReadAheadLo", EControls::DataShardControlsTtlReadAheadLo)
            .Case("DataShardControls.TtlReadAheadHi", EControls::DataShardControlsTtlReadAheadHi)
            .Case("DataShardControls.EnableLockedWrites", EControls::DataShardControlsEnableLockedWrites)
            .Case("DataShardControls.MaxLockedWritesPerKey", EControls::DataShardControlsMaxLockedWritesPerKey)
            .Case("DataShardControls.EnableLeaderLeases", EControls::DataShardControlsEnableLeaderLeases)
            .Case("DataShardControls.MinLeaderLeaseDurationUs", EControls::DataShardControlsMinLeaderLeaseDurationUs)
            .Case("DataShardControls.ChangeRecordDebugPrint", EControls::DataShardControlsChangeRecordDebugPrint)

            .Case("BlobStorage_EnablePutBatching", EControls::BlobStorageEnablePutBatching)
            .Case("BlobStorage_EnableVPatch", EControls::BlobStorageEnableVPatch)

            .Case("VDiskControls.EnableLocalSyncLogDataCutting", EControls::VDiskControlsEnableLocalSyncLogDataCutting)
            .Case("VDiskControls.EnableSyncLogChunkCompressionHDD", EControls::VDiskControlsEnableSyncLogChunkCompressionHDD)
            .Case("VDiskControls.EnableSyncLogChunkCompressionSSD", EControls::VDiskControlsEnableSyncLogChunkCompressionSSD)
            .Case("VDiskControls.MaxSyncLogChunksInFlightHDD", EControls::VDiskControlsMaxSyncLogChunksInFlightHDD)
            .Case("VDiskControls.MaxSyncLogChunksInFlightSSD", EControls::VDiskControlsMaxSyncLogChunksInFlightSSD)
            .Case("VDiskControls.BurstThresholdNsHDD", EControls::VDiskControlsBurstThresholdNsHDD)
            .Case("VDiskControls.BurstThresholdNsSSD", EControls::VDiskControlsBurstThresholdNsSSD)
            .Case("VDiskControls.BurstThresholdNsNVME", EControls::VDiskControlsBurstThresholdNsNVME)
            .Case("VDiskControls.DiskTimeAvailableScaleHDD", EControls::VDiskControlsDiskTimeAvailableScaleHDD)
            .Case("VDiskControls.DiskTimeAvailableScaleSSD", EControls::VDiskControlsDiskTimeAvailableScaleSSD)
            .Case("VDiskControls.DiskTimeAvailableScaleNVME", EControls::VDiskControlsDiskTimeAvailableScaleNVME)

            .Case("SchemeShard_SplitMergePartCountLimit", EControls::SchemeShardSplitMergePartCountLimit)
            .Case("SchemeShard_FastSplitSizeThreshold", EControls::SchemeShardFastSplitSizeThreshold)
            .Case("SchemeShard_FastSplitRowCountThreshold", EControls::SchemeShardFastSplitRowCountThreshold)
            .Case("SchemeShard_FastSplitCpuPercentageThreshold", EControls::SchemeShardFastSplitCpuPercentageThreshold)
            .Case("SchemeShard_SplitByLoadEnabled", EControls::SchemeShardSplitByLoadEnabled)
            .Case("SchemeShard_SplitByLoadMaxShardsDefault", EControls::SchemeShardSplitByLoadMaxShardsDefault)
            .Case("SchemeShard_MergeByLoadMinUptimeSec", EControls::SchemeShardMergeByLoadMinUptimeSec)
            .Case("SchemeShard_MergeByLoadMinLowLoadDurationSec", EControls::SchemeShardMergeByLoadMinLowLoadDurationSec)

            .Case("SchemeShardControls.ForceShardSplitDataSize", EControls::SchemeShardControlsForceShardSplitDataSize)
            .Case("SchemeShardControls.DisableForceShardSplit", EControls::SchemeShardControlsDisableForceShardSplit)

            .Case("TCMallocControls.ProfileSamplingRate", EControls::TCMallocControlsProfileSamplingRate)
            .Case("TCMallocControls.GuardedSamplingRate", EControls::TCMallocControlsGuardedSamplingRate)
            .Case("TCMallocControls.MemoryLimit", EControls::TCMallocControlsMemoryLimit)
            .Case("TCMallocControls.PageCacheTargetSize", EControls::TCMallocControlsPageCacheTargetSize)
            .Case("TCMallocControls.PageCacheReleaseRate", EControls::TCMallocControlsPageCacheReleaseRate)

            .Case("ColumnShardControls.MinBytesToIndex", EControls::ColumnShardControlsMinBytesToIndex)
            .Case("ColumnShardControls.MaxBytesToIndex", EControls::ColumnShardControlsMaxBytesToIndex)
            .Case("ColumnShardControls.InsertTableCommittedSize", EControls::ColumnShardControlsInsertTableCommittedSize)
            .Case("ColumnShardControls.IndexGoodBlobSize", EControls::ColumnShardControlsIndexGoodBlobSize)
            .Case("ColumnShardControls.GranuleOverloadBytes", EControls::ColumnShardControlsGranuleOverloadBytes)
            .Case("ColumnShardControls.CompactionDelaySec", EControls::ColumnShardControlsCompactionDelaySec)
            .Case("ColumnShardControls.GranuleIndexedPortionsSizeLimit", EControls::ColumnShardControlsGranuleIndexedPortionsSizeLimit)
            .Case("ColumnShardControls.GranuleIndexedPortionsCountLimit", EControls::ColumnShardControlsGranuleIndexedPortionsCountLimit)
            
            .Case("BlobCache.MaxCacheDataSize", EControls::BlobCacheMaxCacheDataSize)
            .Case("BlobCache.MaxInFlightDataSize", EControls::BlobCacheMaxInFlightDataSize)

            .Case("ColumnShardControls.BlobWriteGrouppingEnabled", EControls::ColumnShardControlsBlobWriteGrouppingEnabled)
            .Case("ColumnShardControls.CacheDataAfterIndexing", EControls::ColumnShardControlsCacheDataAfterIndexing)
            .Case("ColumnShardControls.CacheDataAfterCompaction", EControls::ColumnShardControlsCacheDataAfterCompaction)

            .Case("CoordinatorControls.EnableLeaderLeases", EControls::CoordinatorControlsEnableLeaderLeases)
            .Case("CoordinatorControls.MinLeaderLeaseDurationUs", EControls::CoordinatorControlsMinLeaderLeaseDurationUs)
            .Case("CoordinatorControls.VolatilePlanLeaseMs", EControls::CoordinatorControlsVolatilePlanLeaseMs)
            .Case("CoordinatorControls.PlanAheadTimeShiftMs", EControls::CoordinatorControlsPlanAheadTimeShiftMs)

            .Case("SchemeShard_AllowConditionalEraseOperations", EControls::SchemeShardAllowConditionalEraseOperations)
            .Case("SchemeShard_DisablePublicationsOfDropping", EControls::SchemeShardDisablePublicationsOfDropping)
            .Case("SchemeShard_FillAllocatePQ", EControls::SchemeShardFillAllocatePQ)
            .Case("SchemeShard_AllowDataColumnForIndexTable", EControls::SchemeShardAllowDataColumnForIndexTable)
            .Case("SchemeShard_AllowServerlessStorageBilling", EControls::SchemeShardAllowServerlessStorageBilling);
    };

    std::array<TIntrusivePtr<TControl>, size_t(EControls::ControlsNo)> Board;
    std::array<TMutex, size_t(EControls::ControlsNo)> BoardLocks;
};

}
