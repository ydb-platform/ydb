#pragma once

#include "immediate_control_board_control.h"
#include "immediate_control_board_wrapper.h"
#include <optional>
#include <string_view>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NKikimr {

struct TControlBoard : public TThrRefBase {

    #define ICB_REG_LOCAL_CONTROL(icb, value, name) \
        { \
            TGuard guard((icb). name##Mutex); \
            if (!(icb).name) { \
                (icb).Assign((icb).name, (value)); \
            } \
        }
    
    #define ICB_REG_SHARED_CONTROL(icb, value, name) \
        { \
            TGuard guard((icb). name##Mutex); \
            if (!(icb).name) { \
                (icb).Assign((icb).name, (value)); \
            } else { \
                (icb).Assign((value), (icb).name); \
            } \
        }
    
    #define ICB_RESTORE_DEFAULT(icb, name) \
        { \
            TGuard guard((icb). name##Mutex); \
            if ((icb).name) { \
                (icb).name -> RestoreDefault(); \
            } \
        }

    #define ICB_RESTORE_DEFAULTS(icb) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsDisableByKeyFilter) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsMaxTxInFly) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsMaxTxLagMilliseconds) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsDataTxProfileLogThresholdMs) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsDataTxProfileBufferThresholdMs) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsDataTxProfileBufferSize) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsCanCancelROWithReadSets) \
        ICB_RESTORE_DEFAULT((icb), TxLimitControlsPerShardReadSizeLimit) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsCpuUsageReportThreshlodPercent) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsCpuUsageReportIntervalSeconds) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsHighDataSizeReportThreshlodBytes) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsHighDataSizeReportIntervalSeconds) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsBackupReadAheadLo) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsBackupReadAheadHi) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsTtlReadAheadLo) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsTtlReadAheadHi) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsEnableLockedWrites) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsMaxLockedWritesPerKey) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsEnableLeaderLeases) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsMinLeaderLeaseDurationUs) \
        ICB_RESTORE_DEFAULT((icb), DataShardControlsChangeRecordDebugPrint) \
        \
        ICB_RESTORE_DEFAULT((icb), BlobStorageEnablePutBatching) \
        ICB_RESTORE_DEFAULT((icb), BlobStorageEnableVPatch) \
        \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsEnableLocalSyncLogDataCutting) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsEnableSyncLogChunkCompressionHDD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsEnableSyncLogChunkCompressionSSD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsMaxSyncLogChunksInFlightHDD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsMaxSyncLogChunksInFlightSSD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsBurstThresholdNsHDD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsBurstThresholdNsSSD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsBurstThresholdNsNVME) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsDiskTimeAvailableScaleHDD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsDiskTimeAvailableScaleSSD) \
        ICB_RESTORE_DEFAULT((icb), VDiskControlsDiskTimeAvailableScaleNVME) \
        \
        ICB_RESTORE_DEFAULT((icb), SchemeShardSplitMergePartCountLimit) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardFastSplitSizeThreshold) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardFastSplitRowCountThreshold) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardFastSplitCpuPercentageThreshold) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardSplitByLoadEnabled) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardSplitByLoadMaxShardsDefault) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardMergeByLoadMinUptimeSec) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardMergeByLoadMinLowLoadDurationSec) \
        \
        ICB_RESTORE_DEFAULT((icb), SchemeShardControlsForceShardSplitDataSize) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardControlsDisableForceShardSplit) \
        \
        ICB_RESTORE_DEFAULT((icb), TCMallocControlsProfileSamplingRate) \
        ICB_RESTORE_DEFAULT((icb), TCMallocControlsGuardedSamplingRate) \
        ICB_RESTORE_DEFAULT((icb), TCMallocControlsMemoryLimit) \
        ICB_RESTORE_DEFAULT((icb), TCMallocControlsPageCacheTargetSize) \
        ICB_RESTORE_DEFAULT((icb), TCMallocControlsPageCacheReleaseRate) \
        \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsMinBytesToIndex) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsMaxBytesToIndex) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsInsertTableCommittedSize) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsIndexGoodBlobSize) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsGranuleOverloadBytes) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsCompactionDelaySec) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsGranuleIndexedPortionsSizeLimit) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsGranuleIndexedPortionsCountLimit) \
        \
        ICB_RESTORE_DEFAULT((icb), BlobCacheMaxCacheDataSize) \
        ICB_RESTORE_DEFAULT((icb), BlobCacheMaxInFlightDataSize) \
        \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsBlobWriteGrouppingEnabled) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsCacheDataAfterIndexing) \
        ICB_RESTORE_DEFAULT((icb), ColumnShardControlsCacheDataAfterCompaction) \
        \
        ICB_RESTORE_DEFAULT((icb), CoordinatorControlsEnableLeaderLeases) \
        ICB_RESTORE_DEFAULT((icb), CoordinatorControlsMinLeaderLeaseDurationUs) \
        ICB_RESTORE_DEFAULT((icb), CoordinatorControlsVolatilePlanLeaseMs) \
        ICB_RESTORE_DEFAULT((icb), CoordinatorControlsPlanAheadTimeShiftMs) \
        \
        ICB_RESTORE_DEFAULT((icb), SchemeShardAllowConditionalEraseOperations) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardDisablePublicationsOfDropping) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardFillAllocatePQ) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardAllowDataColumnForIndexTable) \
        ICB_RESTORE_DEFAULT((icb), SchemeShardAllowServerlessStorageBilling)
    
    #define ICB_GET_VALUE(icb, name, value, exist) \
        { \
            TGuard guard((icb). name##Mutex); \
            if ((icb).name) { \
                (value) = (icb).name -> Get(); \
                exist = true; \
            } else { \
                exist = false; \
            } \
        }
    
    #define ICB_SET_VALUE(icb, name, value, prevValue) \
        { \
            TGuard guard((icb). name##Mutex); \
            if ((icb).name) { \
                (prevValue) = (icb).name -> SetFromHtmlRequest((value)); \
            } \
        }

    #define ICB_RENDER_HTML_TABLE_ROW(icb, str, name) \
        if ((icb).name) { \
            TABLER() { \
                TABLED() { (str) << #name ; } \
                TABLED() { (str) << (icb).name ->RangeAsString(); } \
                TABLED() { \
                    if ((icb).name ->IsDefault()) { \
                        (str) << "<p>" << (icb).name ->Get() << "</p>"; \
                    } else { \
                        (str) << "<p style='color:red;'><b>" << (icb).name ->Get() << " </b></p>"; \
                    } \
                } \
                TABLED() { \
                    if ((icb).name ->IsDefault()) { \
                        (str) << "<p>" << (icb).name ->GetDefault() << "</p>"; \
                    } else { \
                        (str) << "<p style='color:red;'><b>" << (icb).name ->GetDefault() << " </b></p>"; \
                    } \
                } \
                TABLED() { \
                    (str) << "<form class='form_horizontal' method='post'>"; \
                    (str) << "<input name='" << #name << "' type='text' value='" \
                        << (icb).name ->Get() << "'/>"; \
                    (str) << "<button type='submit' style='color:red;'><b>Change</b></button>"; \
                    (str) << "</form>"; \
                } \
                TABLED() { (str) << !(icb).name ->IsDefault(); } \
            } \
        }
    
    #define ICB_RENDER_HTML_TABLE_ROWS(icb, str) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsDisableByKeyFilter) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsMaxTxInFly) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsMaxTxLagMilliseconds) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsDataTxProfileLogThresholdMs) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsDataTxProfileBufferThresholdMs) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsDataTxProfileBufferSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsCanCancelROWithReadSets) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TxLimitControlsPerShardReadSizeLimit) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsCpuUsageReportThreshlodPercent) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsCpuUsageReportIntervalSeconds) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsHighDataSizeReportThreshlodBytes) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsHighDataSizeReportIntervalSeconds) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsBackupReadAheadLo) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsBackupReadAheadHi) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsTtlReadAheadLo) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsTtlReadAheadHi) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsEnableLockedWrites) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsMaxLockedWritesPerKey) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsEnableLeaderLeases) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsMinLeaderLeaseDurationUs) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), DataShardControlsChangeRecordDebugPrint) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), BlobStorageEnablePutBatching) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), BlobStorageEnableVPatch) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsEnableLocalSyncLogDataCutting) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsEnableSyncLogChunkCompressionHDD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsEnableSyncLogChunkCompressionSSD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsMaxSyncLogChunksInFlightHDD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsMaxSyncLogChunksInFlightSSD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsBurstThresholdNsHDD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsBurstThresholdNsSSD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsBurstThresholdNsNVME) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsDiskTimeAvailableScaleHDD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsDiskTimeAvailableScaleSSD) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), VDiskControlsDiskTimeAvailableScaleNVME) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardSplitMergePartCountLimit) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardFastSplitSizeThreshold) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardFastSplitRowCountThreshold) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardFastSplitCpuPercentageThreshold) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardSplitByLoadEnabled) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardSplitByLoadMaxShardsDefault) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardMergeByLoadMinUptimeSec) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardMergeByLoadMinLowLoadDurationSec) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardControlsForceShardSplitDataSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardControlsDisableForceShardSplit) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TCMallocControlsProfileSamplingRate) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TCMallocControlsGuardedSamplingRate) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TCMallocControlsMemoryLimit) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TCMallocControlsPageCacheTargetSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), TCMallocControlsPageCacheReleaseRate) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsMinBytesToIndex) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsMaxBytesToIndex) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsInsertTableCommittedSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsIndexGoodBlobSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsGranuleOverloadBytes) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsCompactionDelaySec) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsGranuleIndexedPortionsSizeLimit) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsGranuleIndexedPortionsCountLimit) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), BlobCacheMaxCacheDataSize) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), BlobCacheMaxInFlightDataSize) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsBlobWriteGrouppingEnabled) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsCacheDataAfterIndexing) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), ColumnShardControlsCacheDataAfterCompaction) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), CoordinatorControlsEnableLeaderLeases) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), CoordinatorControlsMinLeaderLeaseDurationUs) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), CoordinatorControlsVolatilePlanLeaseMs) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), CoordinatorControlsPlanAheadTimeShiftMs) \
        \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardAllowConditionalEraseOperations) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardDisablePublicationsOfDropping) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardFillAllocatePQ) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardAllowDataColumnForIndexTable) \
        ICB_RENDER_HTML_TABLE_ROW((icb), (str), SchemeShardAllowServerlessStorageBilling)

    void RenderAsHtmlTableRows(TStringStream& str) const;

    void Assign(TIntrusivePtr<TControl>& to, TControlWrapper& from);
    
    void Assign(TControlWrapper& to, TIntrusivePtr<TControl>& from);

    TIntrusivePtr<TControl> DataShardControlsDisableByKeyFilter;
    TMutex DataShardControlsDisableByKeyFilterMutex;
    TIntrusivePtr<TControl> DataShardControlsMaxTxInFly;
    TMutex DataShardControlsMaxTxInFlyMutex;
    TIntrusivePtr<TControl> DataShardControlsMaxTxLagMilliseconds;
    TMutex DataShardControlsMaxTxLagMillisecondsMutex;
    TIntrusivePtr<TControl> DataShardControlsDataTxProfileLogThresholdMs;
    TMutex DataShardControlsDataTxProfileLogThresholdMsMutex;
    TIntrusivePtr<TControl> DataShardControlsDataTxProfileBufferThresholdMs;
    TMutex DataShardControlsDataTxProfileBufferThresholdMsMutex;
    TIntrusivePtr<TControl> DataShardControlsDataTxProfileBufferSize;
    TMutex DataShardControlsDataTxProfileBufferSizeMutex;
    TIntrusivePtr<TControl> DataShardControlsCanCancelROWithReadSets;
    TMutex DataShardControlsCanCancelROWithReadSetsMutex;
    TIntrusivePtr<TControl> TxLimitControlsPerShardReadSizeLimit;
    TMutex TxLimitControlsPerShardReadSizeLimitMutex;
    TIntrusivePtr<TControl> DataShardControlsCpuUsageReportThreshlodPercent;
    TMutex DataShardControlsCpuUsageReportThreshlodPercentMutex;
    TIntrusivePtr<TControl> DataShardControlsCpuUsageReportIntervalSeconds;
    TMutex DataShardControlsCpuUsageReportIntervalSecondsMutex;
    TIntrusivePtr<TControl> DataShardControlsHighDataSizeReportThreshlodBytes;
    TMutex DataShardControlsHighDataSizeReportThreshlodBytesMutex;
    TIntrusivePtr<TControl> DataShardControlsHighDataSizeReportIntervalSeconds;
    TMutex DataShardControlsHighDataSizeReportIntervalSecondsMutex;
    TIntrusivePtr<TControl> DataShardControlsBackupReadAheadLo;
    TMutex DataShardControlsBackupReadAheadLoMutex;
    TIntrusivePtr<TControl> DataShardControlsBackupReadAheadHi;
    TMutex DataShardControlsBackupReadAheadHiMutex;
    TIntrusivePtr<TControl> DataShardControlsTtlReadAheadLo;
    TMutex DataShardControlsTtlReadAheadLoMutex;
    TIntrusivePtr<TControl> DataShardControlsTtlReadAheadHi;
    TMutex DataShardControlsTtlReadAheadHiMutex;
    TIntrusivePtr<TControl> DataShardControlsEnableLockedWrites;
    TMutex DataShardControlsEnableLockedWritesMutex;
    TIntrusivePtr<TControl> DataShardControlsMaxLockedWritesPerKey;
    TMutex DataShardControlsMaxLockedWritesPerKeyMutex;
    TIntrusivePtr<TControl> DataShardControlsEnableLeaderLeases;
    TMutex DataShardControlsEnableLeaderLeasesMutex;
    TIntrusivePtr<TControl> DataShardControlsMinLeaderLeaseDurationUs;
    TMutex DataShardControlsMinLeaderLeaseDurationUsMutex;
    TIntrusivePtr<TControl> DataShardControlsChangeRecordDebugPrint;
    TMutex DataShardControlsChangeRecordDebugPrintMutex;

    TIntrusivePtr<TControl> BlobStorageEnablePutBatching;
    TMutex BlobStorageEnablePutBatchingMutex;
    TIntrusivePtr<TControl> BlobStorageEnableVPatch;
    TMutex BlobStorageEnableVPatchMutex;

    TIntrusivePtr<TControl> VDiskControlsEnableLocalSyncLogDataCutting;
    TMutex VDiskControlsEnableLocalSyncLogDataCuttingMutex;
    TIntrusivePtr<TControl> VDiskControlsEnableSyncLogChunkCompressionHDD;
    TMutex VDiskControlsEnableSyncLogChunkCompressionHDDMutex;
    TIntrusivePtr<TControl> VDiskControlsEnableSyncLogChunkCompressionSSD;
    TMutex VDiskControlsEnableSyncLogChunkCompressionSSDMutex;
    TIntrusivePtr<TControl> VDiskControlsMaxSyncLogChunksInFlightHDD;
    TMutex VDiskControlsMaxSyncLogChunksInFlightHDDMutex;
    TIntrusivePtr<TControl> VDiskControlsMaxSyncLogChunksInFlightSSD;
    TMutex VDiskControlsMaxSyncLogChunksInFlightSSDMutex;
    TIntrusivePtr<TControl> VDiskControlsBurstThresholdNsHDD;
    TMutex VDiskControlsBurstThresholdNsHDDMutex;
    TIntrusivePtr<TControl> VDiskControlsBurstThresholdNsSSD;
    TMutex VDiskControlsBurstThresholdNsSSDMutex;
    TIntrusivePtr<TControl> VDiskControlsBurstThresholdNsNVME;
    TMutex VDiskControlsBurstThresholdNsNVMEMutex;
    TIntrusivePtr<TControl> VDiskControlsDiskTimeAvailableScaleHDD;
    TMutex VDiskControlsDiskTimeAvailableScaleHDDMutex;
    TIntrusivePtr<TControl> VDiskControlsDiskTimeAvailableScaleSSD;
    TMutex VDiskControlsDiskTimeAvailableScaleSSDMutex;
    TIntrusivePtr<TControl> VDiskControlsDiskTimeAvailableScaleNVME;
    TMutex VDiskControlsDiskTimeAvailableScaleNVMEMutex;

    TIntrusivePtr<TControl> SchemeShardSplitMergePartCountLimit;
    TMutex SchemeShardSplitMergePartCountLimitMutex;
    TIntrusivePtr<TControl> SchemeShardFastSplitSizeThreshold;
    TMutex SchemeShardFastSplitSizeThresholdMutex;
    TIntrusivePtr<TControl> SchemeShardFastSplitRowCountThreshold;
    TMutex SchemeShardFastSplitRowCountThresholdMutex;
    TIntrusivePtr<TControl> SchemeShardFastSplitCpuPercentageThreshold;
    TMutex SchemeShardFastSplitCpuPercentageThresholdMutex;
    TIntrusivePtr<TControl> SchemeShardSplitByLoadEnabled;
    TMutex SchemeShardSplitByLoadEnabledMutex;
    TIntrusivePtr<TControl> SchemeShardSplitByLoadMaxShardsDefault;
    TMutex SchemeShardSplitByLoadMaxShardsDefaultMutex;
    TIntrusivePtr<TControl> SchemeShardMergeByLoadMinUptimeSec;
    TMutex SchemeShardMergeByLoadMinUptimeSecMutex;
    TIntrusivePtr<TControl> SchemeShardMergeByLoadMinLowLoadDurationSec;
    TMutex SchemeShardMergeByLoadMinLowLoadDurationSecMutex;

    TIntrusivePtr<TControl> SchemeShardControlsForceShardSplitDataSize;
    TMutex SchemeShardControlsForceShardSplitDataSizeMutex;
    TIntrusivePtr<TControl> SchemeShardControlsDisableForceShardSplit;
    TMutex SchemeShardControlsDisableForceShardSplitMutex;

    TIntrusivePtr<TControl> TCMallocControlsProfileSamplingRate;
    TMutex TCMallocControlsProfileSamplingRateMutex;
    TIntrusivePtr<TControl> TCMallocControlsGuardedSamplingRate;
    TMutex TCMallocControlsGuardedSamplingRateMutex;
    TIntrusivePtr<TControl> TCMallocControlsMemoryLimit;
    TMutex TCMallocControlsMemoryLimitMutex;
    TIntrusivePtr<TControl> TCMallocControlsPageCacheTargetSize;
    TMutex TCMallocControlsPageCacheTargetSizeMutex;
    TIntrusivePtr<TControl> TCMallocControlsPageCacheReleaseRate;
    TMutex TCMallocControlsPageCacheReleaseRateMutex;

    TIntrusivePtr<TControl> ColumnShardControlsMinBytesToIndex;
    TMutex ColumnShardControlsMinBytesToIndexMutex;
    TIntrusivePtr<TControl> ColumnShardControlsMaxBytesToIndex;
    TMutex ColumnShardControlsMaxBytesToIndexMutex;
    TIntrusivePtr<TControl> ColumnShardControlsInsertTableCommittedSize;
    TMutex ColumnShardControlsInsertTableCommittedSizeMutex;
    TIntrusivePtr<TControl> ColumnShardControlsIndexGoodBlobSize;
    TMutex ColumnShardControlsIndexGoodBlobSizeMutex;
    TIntrusivePtr<TControl> ColumnShardControlsGranuleOverloadBytes;
    TMutex ColumnShardControlsGranuleOverloadBytesMutex;
    TIntrusivePtr<TControl> ColumnShardControlsCompactionDelaySec;
    TMutex ColumnShardControlsCompactionDelaySecMutex;
    TIntrusivePtr<TControl> ColumnShardControlsGranuleIndexedPortionsSizeLimit;
    TMutex ColumnShardControlsGranuleIndexedPortionsSizeLimitMutex;
    TIntrusivePtr<TControl> ColumnShardControlsGranuleIndexedPortionsCountLimit;
    TMutex ColumnShardControlsGranuleIndexedPortionsCountLimitMutex;
    
    TIntrusivePtr<TControl> BlobCacheMaxCacheDataSize;
    TMutex BlobCacheMaxCacheDataSizeMutex;
    TIntrusivePtr<TControl> BlobCacheMaxInFlightDataSize;
    TMutex BlobCacheMaxInFlightDataSizeMutex;

    TIntrusivePtr<TControl> ColumnShardControlsBlobWriteGrouppingEnabled;
    TMutex ColumnShardControlsBlobWriteGrouppingEnabledMutex;
    TIntrusivePtr<TControl> ColumnShardControlsCacheDataAfterIndexing;
    TMutex ColumnShardControlsCacheDataAfterIndexingMutex;
    TIntrusivePtr<TControl> ColumnShardControlsCacheDataAfterCompaction;
    TMutex ColumnShardControlsCacheDataAfterCompactionMutex;

    TIntrusivePtr<TControl> CoordinatorControlsEnableLeaderLeases;
    TMutex CoordinatorControlsEnableLeaderLeasesMutex;
    TIntrusivePtr<TControl> CoordinatorControlsMinLeaderLeaseDurationUs;
    TMutex CoordinatorControlsMinLeaderLeaseDurationUsMutex;
    TIntrusivePtr<TControl> CoordinatorControlsVolatilePlanLeaseMs;
    TMutex CoordinatorControlsVolatilePlanLeaseMsMutex;
    TIntrusivePtr<TControl> CoordinatorControlsPlanAheadTimeShiftMs;
    TMutex CoordinatorControlsPlanAheadTimeShiftMsMutex;

    TIntrusivePtr<TControl> SchemeShardAllowConditionalEraseOperations;
    TMutex SchemeShardAllowConditionalEraseOperationsMutex;
    TIntrusivePtr<TControl> SchemeShardDisablePublicationsOfDropping;
    TMutex SchemeShardDisablePublicationsOfDroppingMutex;
    TIntrusivePtr<TControl> SchemeShardFillAllocatePQ;
    TMutex SchemeShardFillAllocatePQMutex;
    TIntrusivePtr<TControl> SchemeShardAllowDataColumnForIndexTable;
    TMutex SchemeShardAllowDataColumnForIndexTableMutex;
    TIntrusivePtr<TControl> SchemeShardAllowServerlessStorageBilling;
    TMutex SchemeShardAllowServerlessStorageBillingMutex;

    constexpr static ui8 BoardSize = 128;
    std::array<TIntrusivePtr<TControl>, BoardSize> Board;
    std::array<TMutex, BoardSize> BoardLocks;
};

}
