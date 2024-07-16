#pragma once

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NKikimr {

struct TControlBoard : public TThrRefBase {

    class TControl;

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
};

}
