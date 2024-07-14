#pragma once

#if \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fauth_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fblobstorage_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fbootstrap_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fcms_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fconfig_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fdatashard_5fconfig_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fkey_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fkqp_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fnetclassifier_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fstream_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fshared_5fcache_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fpqconfig_2eproto)
#   define __PROTOS_WERE_INCLUDED 1
#else
#   define __PROTOS_WERE_INCLUDED 0
#endif

#include "defs.h"
#include "appdata_fwd.h"
#include "channel_profiles.h"
#include "domain.h"
#include "feature_flags.h"
#include "nameservice.h"
#include "tablet_types.h"
#include "resource_profile.h"
#include "event_filter.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/pdisk_io/aio.h>

#include <ydb/core/base/event_filter.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

    struct TExperimentingService : public TThrRefBase {
        #define EXP_SERVICE_REG_SHARED(expService, name, value) \
            if (!(expService).name.IsDefined()) { \
                (expService).name = value; \
            } else { \
                value = (expService).name; \
            }
        
        #define EXP_SERVICE_REG_LOCAL(expService, name, value) \
            (expService).name = value;
        
        #define EXP_SERVICE_SET_VALUE(expService, name, value, prevValue) \
            prevValue = std::exchange((expService).name, value)

        TControlWrapper DataShardControlsDisableByKeyFilter;
        TControlWrapper DataShardControlsMaxTxInFly;
        TControlWrapper DataShardControlsMaxTxLagMilliseconds;
        TControlWrapper DataShardControlsDataTxProfileLogThresholdMs;
        TControlWrapper DataShardControlsDataTxProfileBufferThresholdMs;
        TControlWrapper DataShardControlsDataTxProfileBufferSize;
        TControlWrapper DataShardControlsCanCancelROWithReadSets;
        TControlWrapper TxLimitControlsPerShardReadSizeLimit;
        TControlWrapper DataShardControlsCpuUsageReportThreshlodPercent;
        TControlWrapper DataShardControlsCpuUsageReportIntervalSeconds;
        TControlWrapper DataShardControlsHighDataSizeReportThreshlodBytes;
        TControlWrapper DataShardControlsHighDataSizeReportIntervalSeconds;
        TControlWrapper DataShardControlsBackupReadAheadLo;
        TControlWrapper DataShardControlsBackupReadAheadHi;
        TControlWrapper DataShardControlsTtlReadAheadLo;
        TControlWrapper DataShardControlsTtlReadAheadHi;
        TControlWrapper DataShardControlsEnableLockedWrites;
        TControlWrapper DataShardControlsMaxLockedWritesPerKey;
        TControlWrapper DataShardControlsEnableLeaderLeases;
        TControlWrapper DataShardControlsMinLeaderLeaseDurationUs;
        TControlWrapper DataShardControlsChangeRecordDebugPrint;
        
        TControlWrapper BlobStorageEnablePutBatching;
        TControlWrapper BlobStorageEnableVPatch;
        TControlWrapper VDiskControlsEnableLocalSyncLogDataCutting;
        TControlWrapper VDiskControlsEnableSyncLogChunkCompressionHDD;
        TControlWrapper VDiskControlsEnableSyncLogChunkCompressionSSD;
        TControlWrapper VDiskControlsMaxSyncLogChunksInFlightHDD;
        TControlWrapper VDiskControlsMaxSyncLogChunksInFlightSSD;
        
        TControlWrapper VDiskControlsBurstThresholdNsHDD;
        TControlWrapper VDiskControlsBurstThresholdNsSSD;
        TControlWrapper VDiskControlsBurstThresholdNsNVME;
        TControlWrapper VDiskControlsDiskTimeAvailableScaleHDD;
        TControlWrapper VDiskControlsDiskTimeAvailableScaleSSD;
        TControlWrapper VDiskControlsDiskTimeAvailableScaleNVME;
        
        TControlWrapper SchemeShardSplitMergePartCountLimit;
        TControlWrapper SchemeShardFastSplitSizeThreshold;
        TControlWrapper SchemeShardFastSplitRowCountThreshold;
        TControlWrapper SchemeShardFastSplitCpuPercentageThreshold;
        
        TControlWrapper SchemeShardSplitByLoadEnabled;
        TControlWrapper SchemeShardSplitByLoadMaxShardsDefault;
        TControlWrapper SchemeShardMergeByLoadMinUptimeSec;
        TControlWrapper SchemeShardMergeByLoadMinLowLoadDurationSec;
        
        TControlWrapper SchemeShardControlsForceShardSplitDataSize;
        TControlWrapper SchemeShardControlsDisableForceShardSplit;
        
        TControlWrapper TCMallocControlsProfileSamplingRate;
        TControlWrapper TCMallocControlsGuardedSamplingRate;
        TControlWrapper TCMallocControlsMemoryLimit;
        TControlWrapper TCMallocControlsPageCacheTargetSize;
        TControlWrapper TCMallocControlsPageCacheReleaseRate;
        
        TControlWrapper ColumnShardControlsMinBytesToIndex;
        TControlWrapper ColumnShardControlsMaxBytesToIndex;
        TControlWrapper ColumnShardControlsInsertTableCommittedSize;
        
        TControlWrapper ColumnShardControlsIndexGoodBlobSize;
        TControlWrapper ColumnShardControlsGranuleOverloadBytes;
        TControlWrapper ColumnShardControlsCompactionDelaySec;
        TControlWrapper ColumnShardControlsGranuleIndexedPortionsSizeLimit;
        TControlWrapper ColumnShardControlsGranuleIndexedPortionsCountLimit;
        
        TControlWrapper BlobCacheMaxCacheDataSize;
        TControlWrapper BlobCacheMaxInFlightDataSize;
        
        TControlWrapper ColumnShardControlsBlobWriteGrouppingEnabled;
        TControlWrapper ColumnShardControlsCacheDataAfterIndexing;
        TControlWrapper ColumnShardControlsCacheDataAfterCompaction;
        
        TControlWrapper CoordinatorControlsEnableLeaderLeases;
        TControlWrapper CoordinatorControlsMinLeaderLeaseDurationUs;
        TControlWrapper CoordinatorControlsVolatilePlanLeaseMs;
        TControlWrapper CoordinatorControlsPlanAheadTimeShiftMs;
        
        TControlWrapper SchemeShardAllowConditionalEraseOperations;
        TControlWrapper SchemeShardDisablePublicationsOfDropping;
        TControlWrapper SchemeShardFillAllocatePQ;
        TControlWrapper SchemeShardAllowDataColumnForIndexTable;
        TControlWrapper SchemeShardAllowServerlessStorageBilling;
    };

} // NKikimr

#if !__PROTOS_WERE_INCLUDED && (\
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fauth_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fblobstorage_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fbootstrap_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fcms_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fconfig_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fdatashard_5fconfig_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fkey_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fkqp_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fnetclassifier_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fstream_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fshared_5fcache_2eproto) || \
        defined(GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fpqconfig_2eproto))
#   error NEVER EVER INCLUDE pb.h FILES FROM appdata.h
#endif

#undef __PROTOS_WERE_INCLUDED
