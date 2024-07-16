#include "immediate_control_board_impl.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

namespace {

const static THashMap<std::string_view, ui8>  Mapping = {
    {"DataShardControlsDisableByKeyFilter", 0},
    {"DataShardControlsMaxTxInFly", 1},
    {"DataShardControlsMaxTxLagMilliseconds", 2},
    {"DataShardControlsDataTxProfile.LogThresholdMs", 3},
    {"DataShardControlsDataTxProfile.BufferThresholdMs", 4},
    {"DataShardControlsDataTxProfile.BufferSize", 5},
    {"DataShardControlsCanCancelROWithReadSets", 6},
    {"TxLimitControlsPerShardReadSizeLimit", 7},
    {"DataShardControlsCpuUsageReportThreshlodPercent", 8},
    {"DataShardControlsCpuUsageReportIntervalSeconds", 9},
    {"DataShardControlsHighDataSizeReportThreshlodBytes", 10},
    {"DataShardControlsHighDataSizeReportIntervalSeconds", 11},
    {"DataShardControlsBackupReadAheadLo", 12},
    {"DataShardControlsBackupReadAheadHi", 13},
    {"DataShardControlsTtlReadAheadLo", 14},
    {"DataShardControlsTtlReadAheadHi", 15},
    {"DataShardControlsEnableLockedWrites", 16},
    {"DataShardControlsMaxLockedWritesPerKey", 17},
    {"DataShardControlsEnableLeaderLeases", 18},
    {"DataShardControlsMinLeaderLeaseDurationUs", 19},
    {"DataShardControlsChangeRecordDebugPrint", 20},

    {"BlobStorageEnablePutBatching", 21},
    {"BlobStorageEnableVPatch", 22},

    {"VDiskControlsEnableLocalSyncLogDataCutting", 23},
    {"VDiskControlsEnableSyncLogChunkCompressionHDD", 24},
    {"VDiskControlsEnableSyncLogChunkCompressionSSD", 25},
    {"VDiskControlsMaxSyncLogChunksInFlightHDD", 26},
    {"VDiskControlsMaxSyncLogChunksInFlightSSD", 27},
    {"VDiskControlsBurstThresholdNsHDD", 28},
    {"VDiskControlsBurstThresholdNsSSD", 29},
    {"VDiskControlsBurstThresholdNsNVME", 30},
    {"VDiskControlsDiskTimeAvailableScaleHDD", 31},
    {"VDiskControlsDiskTimeAvailableScaleSSD", 32},
    {"VDiskControlsDiskTimeAvailableScaleNVME", 33},

    {"SchemeShardSplitMergePartCountLimit", 34},
    {"SchemeShardFastSplitSizeThreshold", 35},
    {"SchemeShardFastSplitRowCountThreshold", 36},
    {"SchemeShardFastSplitCpuPercentageThreshold", 37},
    {"SchemeShardSplitByLoadEnabled", 38},
    {"SchemeShardSplitByLoadMaxShardsDefault", 39},
    {"SchemeShardMergeByLoadMinUptimeSec", 40},
    {"SchemeShardMergeByLoadMinLowLoadDurationSec", 41},

    {"SchemeShardControlsForceShardSplitDataSize", 42},
    {"SchemeShardControlsDisableForceShardSplit", 43},

    {"TCMallocControlsProfileSamplingRate", 44},
    {"TCMallocControlsGuardedSamplingRate", 45},
    {"TCMallocControlsMemoryLimit", 46},
    {"TCMallocControlsPageCacheTargetSize", 47},
    {"TCMallocControlsPageCacheReleaseRate", 48},

    {"ColumnShardControlsMinBytesToIndex", 49},
    {"ColumnShardControlsMaxBytesToIndex", 50},
    {"ColumnShardControlsInsertTableCommittedSize", 51},
    {"ColumnShardControlsIndexGoodBlobSize", 52},
    {"ColumnShardControlsGranuleOverloadBytes", 53},
    {"ColumnShardControlsCompactionDelaySec", 54},
    {"ColumnShardControlsGranuleIndexedPortionsSizeLimit", 55},
    {"ColumnShardControlsGranuleIndexedPortionsCountLimit", 56},
    
    {"BlobCacheMaxCacheDataSize", 57},
    {"BlobCacheMaxInFlightDataSize", 58},

    {"ColumnShardControlsBlobWriteGrouppingEnabled", 59},
    {"ColumnShardControlsCacheDataAfterIndexing", 60},
    {"ColumnShardControlsCacheDataAfterCompaction", 61},

    {"CoordinatorControls.EnableLeaderLeases", 62},
    {"CoordinatorControls.MinLeaderLeaseDurationUs", 63},
    {"CoordinatorControls.VolatilePlanLeaseMs", 64},
    {"CoordinatorControls.PlanAheadTimeShiftMs", 65},

    {"SchemeShardAllowConditionalEraseOperations", 66},
    {"SchemeShardDisablePublicationsOfDropping", 67},
    {"SchemeShardFillAllocatePQ", 68},
    {"SchemeShardAllowDataColumnForIndexTable", 69},
    {"SchemeShardAllowServerlessStorageBilling", 70}
};

} // namespace

ui8 TControlBoard::GetIndex(const std::string_view s) const noexcept {
    auto it = Mapping.find(s);
    if (it != Mapping.end()) {
        return ui8(it->second);
    } else {
        return BoardSize;
    }
}

bool TControlBoard::RegisterLocalControl(TControlWrapper control, TString name) {
    auto idx = GetIndex(name.c_str());
    if (idx == BoardSize) {
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
    if (idx == BoardSize) {
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
    for (auto& [name, i] : Mapping) {
        TGuard guard(BoardLocks[i]);
        if (!Board[i]) {
            continue;
        }
        Board[i]->RestoreDefault();
    }
}

void TControlBoard::RestoreDefault(TString name) {
    auto idx = GetIndex(name.c_str());
    if (idx == BoardSize) {
        return;
    } else if (TGuard guard(BoardLocks[idx]); Board[idx]) {
        Board[idx]->RestoreDefault();
    }
}

bool TControlBoard::SetValue(TString name, TAtomic value, TAtomic &outPrevValue) {
    TIntrusivePtr<TControl> control;
    auto idx = GetIndex(name.c_str());
    if (idx == BoardSize) {
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
    if (idx == BoardSize) {
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
                    for (const auto& [name, idx] : Mapping) {
                        TGuard guard(BoardLocks[idx]);
                        if (!Board[idx]) {
                            continue;
                        }
                        TABLED() { str << name; }
                        TABLED() { str << Board[idx]->RangeAsString(); }
                        TABLED() {
                            if (Board[idx]->IsDefault()) {
                                str << "<p>" << Board[idx]->Get() << "</p>";
                            } else {
                                str << "<p style='color:red;'><b>" << Board[idx]->Get() << " </b></p>";
                            }
                        }
                        TABLED() {
                            if (Board[idx]->IsDefault()) {
                                str << "<p>" << Board[idx]->GetDefault() << "</p>";
                            } else {
                                str << "<p style='color:red;'><b>" << Board[idx]->GetDefault() << " </b></p>";
                            }
                        }
                        TABLED() {
                            str << "<form class='form_horizontal' method='post'>";
                            str << "<input name='" << name << "' type='text' value='"
                                << Board[idx]->Get() << "'/>";
                            str << "<button type='submit' style='color:red;'><b>Change</b></button>";
                            str << "</form>";
                        }
                        TABLED() { str << !Board[idx]->IsDefault(); }
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