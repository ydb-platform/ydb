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

        TControlWrapper DataShardControlsDisableByKeyFilter{nullptr};
        TControlWrapper DataShardControlsMaxTxInFly{nullptr};
        TControlWrapper DataShardControlsMaxTxLagMilliseconds{nullptr};
        TControlWrapper DataShardControlsDataTxProfileLogThresholdMs{nullptr};
        TControlWrapper DataShardControlsDataTxProfileBufferThresholdMs{nullptr};
        TControlWrapper DataShardControlsDataTxProfileBufferSize{nullptr};
        TControlWrapper DataShardControlsCanCancelROWithReadSets{nullptr};
        TControlWrapper TxLimitControlsPerShardReadSizeLimit{nullptr};
        TControlWrapper DataShardControlsCpuUsageReportThreshlodPercent{nullptr};
        TControlWrapper DataShardControlsCpuUsageReportIntervalSeconds{nullptr};
        TControlWrapper DataShardControlsHighDataSizeReportThreshlodBytes{nullptr};
        TControlWrapper DataShardControlsHighDataSizeReportIntervalSeconds{nullptr};
        TControlWrapper DataShardControlsBackupReadAheadLo{nullptr};
        TControlWrapper DataShardControlsBackupReadAheadHi{nullptr};
        TControlWrapper DataShardControlsTtlReadAheadLo{nullptr};
        TControlWrapper DataShardControlsTtlReadAheadHi{nullptr};
        TControlWrapper DataShardControlsEnableLockedWrites{nullptr};
        TControlWrapper DataShardControlsMaxLockedWritesPerKey{nullptr};
        TControlWrapper DataShardControlsEnableLeaderLeases{nullptr};
        TControlWrapper DataShardControlsMinLeaderLeaseDurationUs{nullptr};
        TControlWrapper DataShardControlsChangeRecordDebugPrint{nullptr};
        
        TControlWrapper BlobStorageEnablePutBatching{nullptr};
        TControlWrapper BlobStorageEnableVPatch{nullptr};
        TControlWrapper VDiskControlsEnableLocalSyncLogDataCutting{nullptr};
        TControlWrapper VDiskControlsEnableSyncLogChunkCompressionHDD{nullptr};
        TControlWrapper VDiskControlsEnableSyncLogChunkCompressionSSD{nullptr};
        TControlWrapper VDiskControlsMaxSyncLogChunksInFlightHDD{nullptr};
        TControlWrapper VDiskControlsMaxSyncLogChunksInFlightSSD{nullptr};
        
        TControlWrapper VDiskControlsBurstThresholdNsHDD{nullptr};
        TControlWrapper VDiskControlsBurstThresholdNsSSD{nullptr};
        TControlWrapper VDiskControlsBurstThresholdNsNVME{nullptr};
        TControlWrapper VDiskControlsDiskTimeAvailableScaleHDD{nullptr};
        TControlWrapper VDiskControlsDiskTimeAvailableScaleSSD{nullptr};
        TControlWrapper VDiskControlsDiskTimeAvailableScaleNVME{nullptr};
        
        TControlWrapper SchemeShardSplitMergePartCountLimit{nullptr};
        TControlWrapper SchemeShardFastSplitSizeThreshold{nullptr};
        TControlWrapper SchemeShardFastSplitRowCountThreshold{nullptr};
        TControlWrapper SchemeShardFastSplitCpuPercentageThreshold{nullptr};
        
        TControlWrapper SchemeShardSplitByLoadEnabled{nullptr};
        TControlWrapper SchemeShardSplitByLoadMaxShardsDefault{nullptr};
        TControlWrapper SchemeShardMergeByLoadMinUptimeSec{nullptr};
        TControlWrapper SchemeShardMergeByLoadMinLowLoadDurationSec{nullptr};
        
        TControlWrapper SchemeShardControlsForceShardSplitDataSize{nullptr};
        TControlWrapper SchemeShardControlsDisableForceShardSplit{nullptr};
        
        TControlWrapper TCMallocControlsProfileSamplingRate{nullptr};
        TControlWrapper TCMallocControlsGuardedSamplingRate{nullptr};
        TControlWrapper TCMallocControlsMemoryLimit{nullptr};
        TControlWrapper TCMallocControlsPageCacheTargetSize{nullptr};
        TControlWrapper TCMallocControlsPageCacheReleaseRate{nullptr};
        
        TControlWrapper ColumnShardControlsMinBytesToIndex{nullptr};
        TControlWrapper ColumnShardControlsMaxBytesToIndex{nullptr};
        TControlWrapper ColumnShardControlsInsertTableCommittedSize{nullptr};
        
        TControlWrapper ColumnShardControlsIndexGoodBlobSize{nullptr};
        TControlWrapper ColumnShardControlsGranuleOverloadBytes{nullptr};
        TControlWrapper ColumnShardControlsCompactionDelaySec{nullptr};
        TControlWrapper ColumnShardControlsGranuleIndexedPortionsSizeLimit{nullptr};
        TControlWrapper ColumnShardControlsGranuleIndexedPortionsCountLimit{nullptr};
        
        TControlWrapper BlobCacheMaxCacheDataSize{nullptr};
        TControlWrapper BlobCacheMaxInFlightDataSize{nullptr};
        
        TControlWrapper ColumnShardControlsBlobWriteGrouppingEnabled{nullptr};
        TControlWrapper ColumnShardControlsCacheDataAfterIndexing{nullptr};
        TControlWrapper ColumnShardControlsCacheDataAfterCompaction{nullptr};
        
        TControlWrapper CoordinatorControlsEnableLeaderLeases{nullptr};
        TControlWrapper CoordinatorControlsMinLeaderLeaseDurationUs{nullptr};
        TControlWrapper CoordinatorControlsVolatilePlanLeaseMs{nullptr};
        TControlWrapper CoordinatorControlsPlanAheadTimeShiftMs{nullptr};
        
        TControlWrapper SchemeShardAllowConditionalEraseOperations{nullptr};
        TControlWrapper SchemeShardDisablePublicationsOfDropping{nullptr};
        TControlWrapper SchemeShardFillAllocatePQ{nullptr};
        TControlWrapper SchemeShardAllowDataColumnForIndexTable{nullptr};
        TControlWrapper SchemeShardAllowServerlessStorageBilling{nullptr};
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
