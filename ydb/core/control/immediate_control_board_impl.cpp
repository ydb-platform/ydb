#include "immediate_control_board_impl.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

namespace {

const static THashMap<std::string_view, ui8>  Mapping = {
    {"DataShardControls.DisableByKeyFilter", 0},
    {"DataShardControls.MaxTxInFly", 1},
    {"DataShardControls.MaxTxLagMilliseconds", 2},
    {"DataShardControls.DataTxProfile.LogThresholdMs", 3},
    {"DataShardControls.DataTxProfile.BufferThresholdMs", 4},
    {"DataShardControls.DataTxProfile.BufferSize", 5},
    {"DataShardControls.CanCancelROWithReadSets", 6},
    {"TxLimitControls.PerShardReadSizeLimit", 7},
    {"DataShardControls.CpuUsageReportThreshlodPercent", 8},
    {"DataShardControls.CpuUsageReportIntervalSeconds", 9},
    {"DataShardControls.HighDataSizeReportThreshlodBytes", 10},
    {"DataShardControls.HighDataSizeReportIntervalSeconds", 11},
    {"DataShardControls.BackupReadAheadLo", 12},
    {"DataShardControls.BackupReadAheadHi", 13},
    {"DataShardControls.TtlReadAheadLo", 14},
    {"DataShardControls.TtlReadAheadHi", 15},
    {"DataShardControls.EnableLockedWrites", 16},
    {"DataShardControls.MaxLockedWritesPerKey", 17},
    {"DataShardControls.EnableLeaderLeases", 18},
    {"DataShardControls.MinLeaderLeaseDurationUs", 19},
    {"DataShardControls.ChangeRecordDebugPrint", 20},

    {"BlobStorage_EnablePutBatching", 21},
    {"BlobStorage_EnableVPatch", 22},

    {"VDiskControls.EnableLocalSyncLogDataCutting", 23},
    {"VDiskControls.EnableSyncLogChunkCompressionHDD", 24},
    {"VDiskControls.EnableSyncLogChunkCompressionSSD", 25},
    {"VDiskControls.MaxSyncLogChunksInFlightHDD", 26},
    {"VDiskControls.MaxSyncLogChunksInFlightSSD", 27},
    {"VDiskControls.BurstThresholdNsHDD", 28},
    {"VDiskControls.BurstThresholdNsSSD", 29},
    {"VDiskControls.BurstThresholdNsNVME", 30},
    {"VDiskControls.DiskTimeAvailableScaleHDD", 31},
    {"VDiskControls.DiskTimeAvailableScaleSSD", 32},
    {"VDiskControls.DiskTimeAvailableScaleNVME", 33},

    {"SchemeShard_SplitMergePartCountLimit", 34},
    {"SchemeShard_FastSplitSizeThreshold", 35},
    {"SchemeShard_FastSplitRowCountThreshold", 36},
    {"SchemeShard_FastSplitCpuPercentageThreshold", 37},
    {"SchemeShard_SplitByLoadEnabled", 38},
    {"SchemeShard_SplitByLoadMaxShardsDefault", 39},
    {"SchemeShard_MergeByLoadMinUptimeSec", 40},
    {"SchemeShard_MergeByLoadMinLowLoadDurationSec", 41},

    {"SchemeShardControls.ForceShardSplitDataSize", 42},
    {"SchemeShardControls.DisableForceShardSplit", 43},

    {"TCMallocControls.ProfileSamplingRate", 44},
    {"TCMallocControls.GuardedSamplingRate", 45},
    {"TCMallocControls.MemoryLimit", 46},
    {"TCMallocControls.PageCacheTargetSize", 47},
    {"TCMallocControls.PageCacheReleaseRate", 48},

    {"ColumnShardControls.MinBytesToIndex", 49},
    {"ColumnShardControls.MaxBytesToIndex", 50},
    {"ColumnShardControls.InsertTableCommittedSize", 51},
    {"ColumnShardControls.IndexGoodBlobSize", 52},
    {"ColumnShardControls.GranuleOverloadBytes", 53},
    {"ColumnShardControls.CompactionDelaySec", 54},
    {"ColumnShardControls.GranuleIndexedPortionsSizeLimit", 55},
    {"ColumnShardControls.GranuleIndexedPortionsCountLimit", 56},
    
    {"BlobCache.MaxCacheDataSize", 57},
    {"BlobCache.MaxInFlightDataSize", 58},

    {"ColumnShardControls.BlobWriteGrouppingEnabled", 59},
    {"ColumnShardControls.CacheDataAfterIndexing", 60},
    {"ColumnShardControls.CacheDataAfterCompaction", 61},

    {"CoordinatorControls.EnableLeaderLeases", 62},
    {"CoordinatorControls.MinLeaderLeaseDurationUs", 63},
    {"CoordinatorControls.VolatilePlanLeaseMs", 64},
    {"CoordinatorControls.PlanAheadTimeShiftMs", 65},

    {"SchemeShard_AllowConditionalEraseOperations", 66},
    {"SchemeShard_DisablePublicationsOfDropping", 67},
    {"SchemeShard_FillAllocatePQ", 68},
    {"SchemeShard_AllowDataColumnForIndexTable", 69},
    {"SchemeShard_AllowServerlessStorageBilling", 70}
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