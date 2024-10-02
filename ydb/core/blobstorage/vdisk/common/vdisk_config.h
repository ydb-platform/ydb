#pragma once
#include "defs.h"

#include "vdisk_performance_params.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/repl/repl_quoter.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#include <ydb/core/control/immediate_control_board_impl.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Every VDisk has a config, this is it
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    struct TVDiskConfig : public TThrRefBase {

        using EKind = NKikimrBlobStorage::TVDiskKind::EVDiskKind;

        // Base info provided while constructing TVDiskConfig
        struct TBaseInfo {
            TVDiskIdShort VDiskIdShort;
            TActorId PDiskActorID;
            ui64 InitOwnerRound = 0;
            ui64 PDiskGuid = 0;
            ui32 PDiskId = 0;
            NPDisk::EDeviceType DeviceType = NPDisk::DEVICE_TYPE_UNKNOWN;
            ui32 VDiskSlotId = 0;
            EKind Kind = NKikimrBlobStorage::TVDiskKind::Default;
            // name of the storage pool this VDisk belongs to
            TString StoragePoolName;
            // is the donor mode enabled for this disk? (no communication with group, actually, no group -- only reads)
            const bool DonorMode = false;
            // a set of donor disks for this one
            std::vector<std::pair<TVDiskID, TActorId>> DonorDiskIds;
            // replication quoters
            TReplQuoter::TPtr ReplPDiskReadQuoter;
            TReplQuoter::TPtr ReplPDiskWriteQuoter;
            TReplQuoter::TPtr ReplNodeRequestQuoter;
            TReplQuoter::TPtr ReplNodeResponseQuoter;
            TDuration YardInitDelay = TDuration::Zero();
            const ui64 ScrubCookie = 0;
            const ui64 WhiteboardInstanceGuid = 0;
            // handle only read requests: needed when VDisk can't write, e.g. no disk space, but still has the data
            const bool ReadOnly = false;

            TBaseInfo(
                    const TVDiskIdShort &vDiskIdShort,
                    const TActorId &pDiskActorId,
                    ui64 pDiskGuid,
                    ui32 pdiskId,
                    NPDisk::EDeviceType deviceType,
                    ui32 vdiskSlotId,
                    EKind kind,
                    ui64 initOwnerRound,
                    TString storagePoolName,
                    const bool donorMode = false,
                    std::vector<std::pair<TVDiskID, TActorId>> donorDiskIds = {},
                    ui64 scrubCookie = 0,
                    ui64 whiteboardInstanceGuid = 0,
                    const bool readOnly = false
            )
                : VDiskIdShort(vDiskIdShort)
                , PDiskActorID(pDiskActorId)
                , InitOwnerRound(initOwnerRound)
                , PDiskGuid(pDiskGuid)
                , PDiskId(pdiskId)
                , DeviceType(deviceType)
                , VDiskSlotId(vdiskSlotId)
                , Kind(kind)
                , StoragePoolName(storagePoolName)
                , DonorMode(donorMode)
                , DonorDiskIds(std::move(donorDiskIds))
                , ScrubCookie(scrubCookie)
                , WhiteboardInstanceGuid(whiteboardInstanceGuid)
                , ReadOnly(readOnly)
            {}

            TBaseInfo(const TBaseInfo &) = default;
            TBaseInfo &operator=(const TBaseInfo &) = default;

            static TBaseInfo SampleForTests() {
                return TBaseInfo();
            }

        private:
            TBaseInfo() = default;
            friend class TAllVDiskKinds;
        };

        const TBaseInfo BaseInfo;

        //////////////// HULL SETTINGS //////////////////////
        // we compact fresh (mem table) by size or by time, below is the setting for time
        ui64 FreshBufSizeLogoBlobs;
        ui64 FreshBufSizeBarriers;
        ui64 FreshBufSizeBlocks;
        ui64 FreshCompThresholdLogoBlobs;
        ui64 FreshCompThresholdBarriers;
        ui64 FreshCompThresholdBlocks;
        TDuration FreshHistoryWindow;
        ui64 FreshHistoryBuckets;
        bool FreshReadReadyMode;
        bool FreshUseDreg;
        bool Level0UseDreg;
        TDuration HullCutLogDuration;
        TDuration HullCompSchedulingInterval;
        TDuration HullCompStorageRatioCalcPeriod;
        TDuration HullCompStorageRatioMaxCalcDuration;
        bool LevelCompaction;
        bool FreshCompaction;
        bool GCOnlySynced;
        bool AllowKeepFlags;
        bool CheckHugeBlobs;
        ui32 MaxLogoBlobDataSize;
        ui32 HullSstSizeInChunksFresh;
        ui32 HullSstSizeInChunksLevel;
        ui32 HugeBlobsFreeChunkReservation;
        ui32 MinHugeBlobInBytes;
        ui32 OldMinHugeBlobInBytes;
        ui32 MilestoneHugeBlobInBytes;
        ui32 HugeBlobOverhead;
        ui32 HullCompLevel0MaxSstsAtOnce;
        ui32 HullCompSortedPartsNum;
        double HullCompLevelRateThreshold;
        double HullCompFreeSpaceThreshold;
        ui32 FreshCompMaxInFlightWrites;
        ui32 HullCompMaxInFlightWrites;
        ui32 HullCompMaxInFlightReads;
        double HullCompReadBatchEfficiencyThreshold;
        ui64 AnubisOsirisMaxInFly;
        bool AddHeader;

        //////////////// LOG CUTTER SETTINGS ////////////////
        TDuration RecoveryLogCutterFirstDuration;
        TDuration RecoveryLogCutterRegularDuration;
        // periodically we write entry point for the database to advance recovery log
        // (we do it even if there is no data written and no compaction running)
        TDuration AdvanceEntryPointTimeout;

        ///////////// SYNCER SETTINGS ///////////////////////
        // run synchronization each SyncTimeInterval
        TDuration SyncTimeInterval;
        TDuration SyncJobTimeout;
        TDuration SyncerRLDRetryTimeout;
        TDuration AnubisTimeout;
        bool RunSyncer;
        bool RunAnubis;
        bool RunDefrag;
        bool RunScrubber;

        ///////////// SYNCLOG SETTINGS //////////////////////
        ui64 SyncLogMaxDiskAmount;
        ui64 SyncLogMaxEntryPointSize;
        ui32 SyncLogAdvisedIndexedBlockSize;
        ui64 SyncLogMaxMemAmount;

        TControlWrapper EnableLocalSyncLogDataCutting;
        TControlWrapper EnableSyncLogChunkCompression;
        TControlWrapper MaxSyncLogChunksInFlight;
        ui32 MaxSyncLogChunkSize;

        ///////////// REPL SETTINGS /////////////////////////
        TDuration ReplTimeInterval;
        TDuration ReplRequestTimeout;
        TDuration ReplPlanQuantum;
        ui32 ReplMaxQuantumBytes;
        ui32 ReplMaxLostVecSize;
        ui32 ReplRequestElements;
        ui32 ReplPrefetchElements;
        ui32 ReplPrefetchDataSize;
        ui32 ReplMaxResponseSize;
        ui32 ReplInterconnectChannel;
        ui32 HandoffMaxWaitQueueSize;
        ui32 HandoffMaxWaitQueueByteSize;
        ui32 HandoffMaxInFlightSize;
        ui32 HandoffMaxInFlightByteSize;
        TDuration HandoffTimeout;
        bool RunRepl;
        bool ReplPausedAtStart = false;
        TDuration ReplMaxTimeToMakeProgress;

        ///////////// SKELETON SETTINGS /////////////////////
        ui64 SkeletonFrontGets_MaxInFlightCount;
        ui64 SkeletonFrontGets_MaxInFlightCost;
        ui64 SkeletonFrontDiscover_MaxInFlightCount;
        ui64 SkeletonFrontDiscover_MaxInFlightCost;
        ui64 SkeletonFrontLogPuts_MaxInFlightCount;
        ui64 SkeletonFrontLogPuts_MaxInFlightCost;
        ui64 SkeletonFrontHugePuts_MaxInFlightCount;
        ui64 SkeletonFrontHugePuts_MaxInFlightCost;
        ui64 SkeletonFrontExtPutTabletLog_TotalCost;
        ui64 SkeletonFrontExtPutAsyncBlob_TotalCost;
        ui64 SkeletonFrontExtPutUserData_TotalCost;
        ui64 SkeletonFrontExtGetAsync_TotalCost;
        ui64 SkeletonFrontExtGetFast_TotalCost;
        ui64 SkeletonFrontExtGetDiscover_TotalCost;
        ui64 SkeletonFrontExtGetLow_TotalCost;
        TDuration SkeletonFrontWakeupPeriod;
        TDuration SkeletonFrontRequestTimeout;
        bool SkeletonFrontQueueBackpressureCheckMsgId;

        /////////////// WINDOW SETTINGS /////////////////////
        ui8 WindowCostChangeToRecalculatePercent;
        ui8 WindowMinLowWatermarkPercent;
        ui8 WindowMaxLowWatermarkPercent;
        ui8 WindowPercentThreshold;
        ui8 WindowCostChangeUntilFrozenPercent;
        ui8 WindowCostChangeUntilDeathPercent;
        TDuration WindowTimeout;

        ///////////// OTHER SETTINGS ////////////////////////
        ui32 MaxResponseSize;
        TDuration DskTrackerInterval;
        bool BarrierValidation;
        TDuration WhiteboardUpdateInterval;
        bool EnableVDiskCooldownTimeout;
        TControlWrapper EnableVPatch = true;
        TControlWrapper DefaultHugeGarbagePerMille;

        ///////////// COST METRICS SETTINGS ////////////////
        bool UseCostTracker = true;
        TCostMetricsParametersByMedia CostMetricsParametersByMedia;

        ///////////// FEATURE FLAGS ////////////////////////
        NKikimrConfig::TFeatureFlags FeatureFlags;

        TVDiskConfig(const TBaseInfo &baseInfo);
        void Merge(const NKikimrBlobStorage::TVDiskConfig &update);
    private:
        // setup default borders for huge blobs depending on device type
        void SetupHugeBytes();
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TAllVDiskKinds responsible for:
    // 1. All vdisk kinds config parsing
    // 2. Creates a corresponding VDisk config given its kind and specific parameters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TAllVDiskKinds : public TThrRefBase {
    public:
        TAllVDiskKinds(const TString &prototext = TString());
        TAllVDiskKinds(const NKikimrBlobStorage::TAllVDiskKinds &proto);
        TIntrusivePtr<TVDiskConfig> MakeVDiskConfig(const TVDiskConfig::TBaseInfo &baseInfo);
        void Merge(const NKikimrBlobStorage::TAllVDiskKinds &allVDiskKinds);

    private:
        using EKind = NKikimrBlobStorage::TVDiskKind::EVDiskKind;
        using TKind = NKikimrBlobStorage::TVDiskKind;
        using TKindsMap = THashMap<EKind, const TKind *>;

        NKikimrBlobStorage::TAllVDiskKinds AllKindsConfig;
        TVDiskConfig VDiskMegaBaseConfig;
        TKindsMap KindsMap;

        void ParseConfig();
    };

} // NKikimr
