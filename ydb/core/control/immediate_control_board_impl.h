#pragma once

#include "immediate_control_board_control.h"
#include "immediate_control_board_wrapper.h"
#include <optional>
#include <string_view>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NKikimr {

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

class TControlBoard : public TThrRefBase {
public:
    bool RegisterLocalControl(TControlWrapper control, TString name);

    bool RegisterSharedControl(TControlWrapper& control, TString name);

    void RestoreDefaults();

    void RestoreDefault(TString name);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    TString RenderAsHtml() const;

private:
    ui8 GetIndex(const std::string_view s) const noexcept;

    std::array<TIntrusivePtr<TControl>, size_t(EControls::ControlsNo)> Board;
    std::array<TMutex, size_t(EControls::ControlsNo)> BoardLocks;
};

}
