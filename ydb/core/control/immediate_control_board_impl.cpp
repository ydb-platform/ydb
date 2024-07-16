#include "immediate_control_board_impl.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

namespace {

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

template <typename First, typename Second>
class SwitchByFirst final {
public:
    constexpr explicit SwitchByFirst(First search) noexcept
        : search_(search) 
    {}
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

constexpr static TrivialBiMap MappingStrToIndex = [](auto selector) {
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

constexpr TrivialBiMap MappingIndexToStr = [](auto selector) {
    return selector
        .Case(EControls::DataShardControlsDisableByKeyFilter, "DataShardControls.DisableByKeyFilter")
        .Case(EControls::DataShardControlsMaxTxInFly, "DataShardControls.MaxTxInFly")
        .Case(EControls::DataShardControlsMaxTxLagMilliseconds, "DataShardControls.MaxTxLagMilliseconds")
        .Case(EControls::DataShardControlsDataTxProfileLogThresholdMs, "DataShardControls.DataTxProfile.LogThresholdMs")
        .Case(EControls::DataShardControlsDataTxProfileBufferThresholdMs, "DataShardControls.DataTxProfile.BufferThresholdMs")
        .Case(EControls::DataShardControlsDataTxProfileBufferSize, "DataShardControls.DataTxProfile.BufferSize")
        .Case(EControls::DataShardControlsCanCancelROWithReadSets, "DataShardControls.CanCancelROWithReadSets")
        .Case(EControls::TxLimitControlsPerShardReadSizeLimit, "TxLimitControls.PerShardReadSizeLimit")
        .Case(EControls::DataShardControlsCpuUsageReportThreshlodPercent, "DataShardControls.CpuUsageReportThreshlodPercent")
        .Case(EControls::DataShardControlsCpuUsageReportIntervalSeconds, "DataShardControls.CpuUsageReportIntervalSeconds")
        .Case(EControls::DataShardControlsHighDataSizeReportThreshlodBytes, "DataShardControls.HighDataSizeReportThreshlodBytes")
        .Case(EControls::DataShardControlsHighDataSizeReportIntervalSeconds, "DataShardControls.HighDataSizeReportIntervalSeconds")
        .Case(EControls::DataShardControlsBackupReadAheadLo, "DataShardControls.BackupReadAheadLo")
        .Case(EControls::DataShardControlsBackupReadAheadHi, "DataShardControls.BackupReadAheadHi")
        .Case(EControls::DataShardControlsTtlReadAheadLo, "DataShardControls.TtlReadAheadLo")
        .Case(EControls::DataShardControlsTtlReadAheadHi, "DataShardControls.TtlReadAheadHi")
        .Case(EControls::DataShardControlsEnableLockedWrites, "DataShardControls.EnableLockedWrites")
        .Case(EControls::DataShardControlsMaxLockedWritesPerKey, "DataShardControls.MaxLockedWritesPerKey")
        .Case(EControls::DataShardControlsEnableLeaderLeases, "DataShardControls.EnableLeaderLeases")
        .Case(EControls::DataShardControlsMinLeaderLeaseDurationUs, "DataShardControls.MinLeaderLeaseDurationUs")
        .Case(EControls::DataShardControlsChangeRecordDebugPrint, "DataShardControls.ChangeRecordDebugPrint")

        .Case(EControls::BlobStorageEnablePutBatching, "BlobStorage_EnablePutBatching")
        .Case(EControls::BlobStorageEnableVPatch, "BlobStorage_EnableVPatch")

        .Case(EControls::VDiskControlsEnableLocalSyncLogDataCutting, "VDiskControls.EnableLocalSyncLogDataCutting")
        .Case(EControls::VDiskControlsEnableSyncLogChunkCompressionHDD, "VDiskControls.EnableSyncLogChunkCompressionHDD")
        .Case(EControls::VDiskControlsEnableSyncLogChunkCompressionSSD, "VDiskControls.EnableSyncLogChunkCompressionSSD")
        .Case(EControls::VDiskControlsMaxSyncLogChunksInFlightHDD, "VDiskControls.MaxSyncLogChunksInFlightHDD")
        .Case(EControls::VDiskControlsMaxSyncLogChunksInFlightSSD, "VDiskControls.MaxSyncLogChunksInFlightSSD")
        .Case(EControls::VDiskControlsBurstThresholdNsHDD, "VDiskControls.BurstThresholdNsHDD")
        .Case(EControls::VDiskControlsBurstThresholdNsSSD, "VDiskControls.BurstThresholdNsSSD")
        .Case(EControls::VDiskControlsBurstThresholdNsNVME, "VDiskControls.BurstThresholdNsNVME")
        .Case(EControls::VDiskControlsDiskTimeAvailableScaleHDD, "VDiskControls.DiskTimeAvailableScaleHDD")
        .Case(EControls::VDiskControlsDiskTimeAvailableScaleSSD, "VDiskControls.DiskTimeAvailableScaleSSD")
        .Case(EControls::VDiskControlsDiskTimeAvailableScaleNVME, "VDiskControls.DiskTimeAvailableScaleNVME")

        .Case(EControls::SchemeShardSplitMergePartCountLimit, "SchemeShard_SplitMergePartCountLimit")
        .Case(EControls::SchemeShardFastSplitSizeThreshold, "SchemeShard_FastSplitSizeThreshold")
        .Case(EControls::SchemeShardFastSplitRowCountThreshold, "SchemeShard_FastSplitRowCountThreshold")
        .Case(EControls::SchemeShardFastSplitCpuPercentageThreshold, "SchemeShard_FastSplitCpuPercentageThreshold")
        .Case(EControls::SchemeShardSplitByLoadEnabled, "SchemeShard_SplitByLoadEnabled")
        .Case(EControls::SchemeShardSplitByLoadMaxShardsDefault, "SchemeShard_SplitByLoadMaxShardsDefault")
        .Case(EControls::SchemeShardMergeByLoadMinUptimeSec, "SchemeShard_MergeByLoadMinUptimeSec")
        .Case(EControls::SchemeShardMergeByLoadMinLowLoadDurationSec, "SchemeShard_MergeByLoadMinLowLoadDurationSec")

        .Case(EControls::SchemeShardControlsForceShardSplitDataSize, "SchemeShardControls.ForceShardSplitDataSize")
        .Case(EControls::SchemeShardControlsDisableForceShardSplit, "SchemeShardControls.DisableForceShardSplit")

        .Case(EControls::TCMallocControlsProfileSamplingRate, "TCMallocControls.ProfileSamplingRate")
        .Case(EControls::TCMallocControlsGuardedSamplingRate, "TCMallocControls.GuardedSamplingRate")
        .Case(EControls::TCMallocControlsMemoryLimit, "TCMallocControls.MemoryLimit")
        .Case(EControls::TCMallocControlsPageCacheTargetSize, "TCMallocControls.PageCacheTargetSize")
        .Case(EControls::TCMallocControlsPageCacheReleaseRate, "TCMallocControls.PageCacheReleaseRate")

        .Case(EControls::ColumnShardControlsMinBytesToIndex, "ColumnShardControls.MinBytesToIndex")
        .Case(EControls::ColumnShardControlsMaxBytesToIndex, "ColumnShardControls.MaxBytesToIndex")
        .Case(EControls::ColumnShardControlsInsertTableCommittedSize, "ColumnShardControls.InsertTableCommittedSize")
        .Case(EControls::ColumnShardControlsIndexGoodBlobSize, "ColumnShardControls.IndexGoodBlobSize")
        .Case(EControls::ColumnShardControlsGranuleOverloadBytes, "ColumnShardControls.GranuleOverloadBytes")
        .Case(EControls::ColumnShardControlsCompactionDelaySec, "ColumnShardControls.CompactionDelaySec")
        .Case(EControls::ColumnShardControlsGranuleIndexedPortionsSizeLimit, "ColumnShardControls.GranuleIndexedPortionsSizeLimit")
        .Case(EControls::ColumnShardControlsGranuleIndexedPortionsCountLimit, "ColumnShardControls.GranuleIndexedPortionsCountLimit")
        
        .Case(EControls::BlobCacheMaxCacheDataSize, "BlobCache.MaxCacheDataSize")
        .Case(EControls::BlobCacheMaxInFlightDataSize, "BlobCache.MaxInFlightDataSize")

        .Case(EControls::ColumnShardControlsBlobWriteGrouppingEnabled, "ColumnShardControls.BlobWriteGrouppingEnabled")
        .Case(EControls::ColumnShardControlsCacheDataAfterIndexing, "ColumnShardControls.CacheDataAfterIndexing")
        .Case(EControls::ColumnShardControlsCacheDataAfterCompaction, "ColumnShardControls.CacheDataAfterCompaction")

        .Case(EControls::CoordinatorControlsEnableLeaderLeases, "CoordinatorControls.EnableLeaderLeases")
        .Case(EControls::CoordinatorControlsMinLeaderLeaseDurationUs, "CoordinatorControls.MinLeaderLeaseDurationUs")
        .Case(EControls::CoordinatorControlsVolatilePlanLeaseMs, "CoordinatorControls.VolatilePlanLeaseMs")
        .Case(EControls::CoordinatorControlsPlanAheadTimeShiftMs, "CoordinatorControls.PlanAheadTimeShiftMs")

        .Case(EControls::SchemeShardAllowConditionalEraseOperations, "SchemeShard_AllowConditionalEraseOperations")
        .Case(EControls::SchemeShardDisablePublicationsOfDropping, "SchemeShard_DisablePublicationsOfDropping")
        .Case(EControls::SchemeShardFillAllocatePQ, "SchemeShard_FillAllocatePQ")
        .Case(EControls::SchemeShardAllowDataColumnForIndexTable, "SchemeShard_AllowDataColumnForIndexTable")
        .Case(EControls::SchemeShardAllowServerlessStorageBilling, "SchemeShard_AllowServerlessStorageBilling");
};

} // namespace

ui8 TControlBoard::GetIndex(const std::string_view s) const noexcept {
    auto result = MappingStrToIndex.TryFindByFirst(s);
    if (result) {
        return ui8(*result);
    } else {
        return ui8(EControls::ControlsNo);
    }
}

bool TControlBoard::RegisterLocalControl(TControlWrapper control, TString name) {
    auto idx = GetIndex(name.c_str());
    if (idx == ui8(EControls::ControlsNo)) {
        return false;
    } else {
        if (TGuard guard(BoardLocks[idx]); Board[idx]) {
            return false;
        } else {
            Board[idx] = control.Control;
            return true;
        }
    }
}

bool TControlBoard::RegisterSharedControl(TControlWrapper& control, TString name) {
    auto idx = GetIndex(name.c_str());
    if (idx == ui8(EControls::ControlsNo)) {
        return false;
    } else {
        if (TGuard guard(BoardLocks[idx]); Board[idx]) {
            control.Control = Board[idx];
            return false;
        } else {
            Board[idx] = control.Control;
            return true;
        }
    }
}

void TControlBoard::RestoreDefaults() {
    for (ui8 i = 0; i < Board.size(); ++i) {
        TGuard guard(BoardLocks[i]);
        if (!Board[i]) {
            continue;
        }
        Board[i]->RestoreDefault();
    }
}

void TControlBoard::RestoreDefault(TString name) {
    auto idx = GetIndex(name.c_str());
    if (idx == ui8(EControls::ControlsNo)) {
        return;
    } else {
        TGuard guard(BoardLocks[idx]);
        if (!Board[idx]) {
            return;
        }
        Board[idx]->RestoreDefault();
    }
}

bool TControlBoard::SetValue(TString name, TAtomic value, TAtomic &outPrevValue) {
    TIntrusivePtr<TControl> control;
    auto idx = GetIndex(name.c_str());
    if (idx == ui8(EControls::ControlsNo)) {
        return false;
    } else {
        if (TGuard guard(BoardLocks[idx]); Board[idx]) {
            AtomicSet(outPrevValue, Board[idx]->SetFromHtmlRequest(value));
            return control->IsDefault();;
        }
        return true;
    }
}

// Only for tests
void TControlBoard::GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const {
    auto idx = GetIndex(name.c_str());
    if (idx == ui8(EControls::ControlsNo)) {
        outIsControlExists = false;
    } else {
        if (TGuard guard(BoardLocks[idx]); !Board[idx]) {
            outIsControlExists = false;
        } else {
            outValue = Board[idx]->Get();
            outIsControlExists = true;
        }
    }
}

TString TControlBoard::RenderAsHtml() const {
    TStringStream str;
    HTML(str) {
        TABLE_SORTABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "Parameter"; }
                    TABLEH() { str << "Acceptable range"; }
                    TABLEH() { str << "Current"; }
                    TABLEH() { str << "Default"; }
                    TABLEH() { str << "Send new value"; }
                    TABLEH() { str << "Changed"; }
                }
            }
            TABLEBODY() {
                TABLER() {
                    for (size_t i = 0; i < Board.size(); ++i) {
                        TGuard guard(BoardLocks[i]);
                        if (!Board[i]) {
                            continue;
                        }
                        TABLED() { str << *MappingIndexToStr.TryFindByFirst(EControls(i)); }
                        TABLED() { str << Board[i]->RangeAsString(); }
                        TABLED() {
                            if (Board[i]->IsDefault()) {
                                str << "<p>" << Board[i]->Get() << "</p>";
                            } else {
                                str << "<p style='color:red;'><b>" << Board[i]->Get() << " </b></p>";
                            }
                        }
                        TABLED() {
                            if (Board[i]->IsDefault()) {
                                str << "<p>" << Board[i]->GetDefault() << "</p>";
                            } else {
                                str << "<p style='color:red;'><b>" << Board[i]->GetDefault() << " </b></p>";
                            }
                        }
                        TABLED() {
                            str << "<form class='form_horizontal' method='post'>";
                            str << "<input name='" << *MappingIndexToStr.TryFindByFirst(EControls(i)) << "' type='text' value='"
                                << Board[i]->Get() << "'/>";
                            str << "<button type='submit' style='color:red;'><b>Change</b></button>";
                            str << "</form>";
                        }
                        TABLED() { str << !Board[i]->IsDefault(); }
                    }
                }
            }
        }
        str << "<form class='form_horizontal' method='post'>";
        str << "<button type='submit' name='restoreDefaults' style='color:green;'><b>Restore Default</b></button>";
        str << "</form>";
    }
    return str.Str();
}

} // namespace NKikimr