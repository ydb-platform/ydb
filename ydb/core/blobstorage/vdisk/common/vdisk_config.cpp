#include "vdisk_config.h"
#include <ydb/core/base/interconnect_channels.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {

    TVDiskConfig::TVDiskConfig(const TBaseInfo &baseInfo)
        : BaseInfo(baseInfo)
    {
        FreshBufSizeLogoBlobs = 64u << 20u;
        FreshBufSizeBarriers = 1u << 10u;
        FreshBufSizeBlocks = 1u << 10u;
        FreshCompThresholdLogoBlobs = 0;    // use default, i.e. chunk size
        FreshCompThresholdBarriers = 0;     // use default, i.e. chunk size
        FreshCompThresholdBlocks = 0;       // use default, i.e. chunk size
        FreshHistoryWindow = TDuration::Minutes(10);
        FreshHistoryBuckets = 10u;
        FreshReadReadyMode = true;
        FreshUseDreg = false;
        Level0UseDreg = true;
        HullCutLogDuration = TDuration::Seconds(1);
        HullCompSchedulingInterval = TDuration::Seconds(1);
        HullCompStorageRatioCalcPeriod = TDuration::Minutes(5);
        HullCompStorageRatioMaxCalcDuration = TDuration::Seconds(1);
        LevelCompaction = true;
        FreshCompaction = true;
        GCOnlySynced = true;
        AllowKeepFlags = true;              // by default vdisk client can send keep/don'tkeep flags, for log can't
        CheckHugeBlobs = false;
        // 128KB for SSD (because of PDisk crc?), 1MB for HDD (?)
        MaxLogoBlobDataSize = MaxVDiskBlobSize;
        HullSstSizeInChunksFresh = 1;
        HullSstSizeInChunksLevel = 1;
        HugeBlobsFreeChunkReservation = 1;
        SetupHugeBytes();
        HugeBlobOverhead = 8u;
        HullCompLevel0MaxSstsAtOnce = 8u;
        HullCompSortedPartsNum = 8u;
        HullCompLevelRateThreshold = 1.0;
        HullCompFreeSpaceThreshold = 2.0;
        FreshCompMaxInFlightWrites = 10;
        HullCompMaxInFlightWrites = 10;
        HullCompMaxInFlightReads = 20;
        HullCompReadBatchEfficiencyThreshold = 0.5;  // don't issue reads if there are more gaps than the useful data
        AnubisOsirisMaxInFly = 1000;
        AddHeader = true;

        RecoveryLogCutterFirstDuration = TDuration::Seconds(10);
        RecoveryLogCutterRegularDuration = TDuration::Seconds(30);
        AdvanceEntryPointTimeout = TDuration::Seconds(10);          // 10 seconds (FIXME: use feedback from PDisk)
        SyncTimeInterval = TDuration::Seconds(3);                   // 3 seconds
        SyncJobTimeout = TDuration::Max();                          // disabled
        SyncerRLDRetryTimeout = TDuration::Seconds(1);
        AnubisTimeout = TDuration::Minutes(60);
        RunSyncer = true;
        RunAnubis = false;                                          // FIXME: turn on by default
        RunDefrag = !baseInfo.ReadOnly;
        RunScrubber = !baseInfo.ReadOnly;

        SyncLogMaxDiskAmount = 0; //ui64(2) << ui64(30);                 // 2 GB
        SyncLogMaxEntryPointSize = ui64(128) << ui64(10);           // 128 KB
        SyncLogAdvisedIndexedBlockSize = ui32(1) << ui32(20);       // 1 MB
        SyncLogMaxMemAmount = ui64(64) << ui64(20);                 // 64 MB

        MaxSyncLogChunkSize = ui32(16) << ui32(10);                 // 32 Kb

        ReplTimeInterval = TDuration::Seconds(60);                  // 60 seconds
        ReplRequestTimeout = TDuration::Seconds(10);                // 10 seconds
        ReplPlanQuantum = TDuration::MilliSeconds(100);             // 100 ms
        ReplMaxQuantumBytes = 384 << 20;                            // 384 MB
        ReplMaxLostVecSize = 100000;
        ReplRequestElements = 250;
        ReplPrefetchElements = 2500;
        ReplPrefetchDataSize = 32 << 20;
        ReplMaxResponseSize = 10 << 20;
        ReplInterconnectChannel = TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA;
        HandoffMaxWaitQueueSize = 10000;
        HandoffMaxWaitQueueByteSize = 32u << 20u;
        HandoffMaxInFlightSize = 1000;
        HandoffMaxInFlightByteSize = 16u << 20u;
        HandoffTimeout = TDuration::Seconds(10);
        RunRepl = !baseInfo.ReadOnly;

        ReplMaxTimeToMakeProgress = VDiskPerformance.at(baseInfo.DeviceType).ReplMaxTimeToMakeProgress;

        SkeletonFrontGets_MaxInFlightCount = 24;
        SkeletonFrontGets_MaxInFlightCost = 200000000;              // 200ms
        SkeletonFrontDiscover_MaxInFlightCount = 100;
        SkeletonFrontDiscover_MaxInFlightCost = 300000000;          // 300ms
        SkeletonFrontLogPuts_MaxInFlightCount = 4000;
        SkeletonFrontLogPuts_MaxInFlightCost = 200000000;           // 200ms
        SkeletonFrontHugePuts_MaxInFlightCount = 50;
        SkeletonFrontHugePuts_MaxInFlightCost = 700000000;          // 700ms

        SkeletonFrontExtPutTabletLog_TotalCost = 300000000;         // 300ms
        SkeletonFrontExtPutAsyncBlob_TotalCost = 700000000;         // 700ms
        SkeletonFrontExtPutUserData_TotalCost = 300000000;          // 300ms
        SkeletonFrontExtGetAsync_TotalCost = 300000000;             // 300ms
        SkeletonFrontExtGetFast_TotalCost = 300000000;              // 300ms
        SkeletonFrontExtGetDiscover_TotalCost = 300000000;          // 300ms
        SkeletonFrontExtGetLow_TotalCost = 300000000;              // 300ms

        SkeletonFrontWakeupPeriod = TDuration::Seconds(1);
        SkeletonFrontRequestTimeout = TDuration::Seconds(10);
        SkeletonFrontQueueBackpressureCheckMsgId = true;

        WindowCostChangeToRecalculatePercent = 2;                   // 2%
        WindowMinLowWatermarkPercent = 2;                           // 2%
        WindowMaxLowWatermarkPercent = 50;                          // 50%
        WindowPercentThreshold = 5;                                 // 5%
        WindowCostChangeUntilFrozenPercent = 20;                    // 20%
        WindowCostChangeUntilDeathPercent = 33;                     // 33%
        WindowTimeout = TDuration::Minutes(60);                     // 1 hour

        MaxResponseSize = ui32(8) << ui32(20);                      // 8 MB
        DskTrackerInterval = TDuration::Seconds(1);
        WhiteboardUpdateInterval = TDuration::Seconds(1);
        EnableVDiskCooldownTimeout = false;

#ifdef NDEBUG
        BarrierValidation = false;
#else
        BarrierValidation = true; // switch by default on debug builds
#endif

    }

    void TVDiskConfig::SetupHugeBytes() {
        switch (BaseInfo.DeviceType) {
            case NPDisk::DEVICE_TYPE_SSD:
            case NPDisk::DEVICE_TYPE_NVME:
                MinHugeBlobInBytes = 64u << 10u;
                break;
            default:
                MinHugeBlobInBytes = 512u << 10u;
                break;
        }
        OldMinHugeBlobInBytes = MinHugeBlobInBytes; // preserved to migrate entry point state correctly 
        MilestoneHugeBlobInBytes = 512u << 10u;  // for compatibility reasons it must be 512KB

    }

    void TVDiskConfig::Merge(const NKikimrBlobStorage::TVDiskConfig &update) {
#define UPDATE_MACRO(name)          \
    if (update.Has##name()) {       \
        name = update.Get##name();  \
    }

        UPDATE_MACRO(FreshBufSizeLogoBlobs);
        UPDATE_MACRO(FreshBufSizeBarriers);
        UPDATE_MACRO(FreshBufSizeBlocks);

        UPDATE_MACRO(FreshCompThresholdLogoBlobs);
        UPDATE_MACRO(FreshCompThresholdBarriers);
        UPDATE_MACRO(FreshCompThresholdBlocks);

        UPDATE_MACRO(FreshReadReadyMode);
        UPDATE_MACRO(FreshUseDreg);

        UPDATE_MACRO(AllowKeepFlags);

        UPDATE_MACRO(HullCompLevel0MaxSstsAtOnce);
        UPDATE_MACRO(HullCompSortedPartsNum);

        UPDATE_MACRO(ReplInterconnectChannel);

        UPDATE_MACRO(BarrierValidation);

#undef UPDATE_MACRO
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TAllVDiskKinds responsible for:
    // 1. All vdisk kinds config parsing
    // 2. Creates a corresponding VDisk config given its kind and specific parameters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    TAllVDiskKinds::TAllVDiskKinds(const TString &prototext)
        : AllKindsConfig()
        , VDiskMegaBaseConfig(TVDiskConfig::TBaseInfo())
        , KindsMap()
    {
        bool result = google::protobuf::TextFormat::ParseFromString(prototext, &AllKindsConfig);
        Y_ABORT_UNLESS(result, "Failed to parse AllVDiskKinds config "
                "(error in protobuf format):\n%s\n", prototext.data());
        ParseConfig();
    }

    TAllVDiskKinds::TAllVDiskKinds(const NKikimrBlobStorage::TAllVDiskKinds &proto)
        : AllKindsConfig()
        , VDiskMegaBaseConfig(TVDiskConfig::TBaseInfo())
        , KindsMap()
    {
        AllKindsConfig.CopyFrom(proto);
        ParseConfig();
    }

    TIntrusivePtr<TVDiskConfig> TAllVDiskKinds::MakeVDiskConfig(const TVDiskConfig::TBaseInfo &baseInfo) {
        EKind k = baseInfo.Kind;
        TVector<const TKind *> merge;
        int levels = 0;
        while (k != NKikimrBlobStorage::TVDiskKind::Default) {
            const auto it = KindsMap.find(k);
            Y_ABORT_UNLESS(it != KindsMap.end(),
                    "Can't find kind='%s' in the config (probably config is incorrect)",
                    NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(k).data());
            merge.push_back(it->second);
            k = it->second->GetBaseKind();

            ++levels;
            Y_ABORT_UNLESS(levels < 32, "Nesting is too large (cycle in the graph?)");
        }

        TIntrusivePtr<TVDiskConfig> cfg(new TVDiskConfig(baseInfo));
        for (auto it = merge.rbegin(), end = merge.rend(); it != end; ++it) {
            cfg->Merge((**it).GetConfig());
        }

        return cfg;
    }

    void TAllVDiskKinds::Merge(const NKikimrBlobStorage::TAllVDiskKinds &allVDiskKinds) {
        AllKindsConfig.MergeFrom(allVDiskKinds);

        ParseConfig();
    }

    void TAllVDiskKinds::ParseConfig() {
        bool result;
        KindsMap.clear();
        for (const auto &x : AllKindsConfig.GetVDiskKinds()) {
            EKind kind = x.GetKind();
            Y_ABORT_UNLESS(kind != NKikimrBlobStorage::TVDiskKind::Default,
                    "It is forbidden to redefine Default kind");
            const NKikimrBlobStorage::TVDiskKind *val = &x;
            result = KindsMap.emplace(kind, val).second;
            Y_ABORT_UNLESS(result, "Duplicate elements in the AllVDiskKinds config: kind='%s",
                    NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(kind).data());
        }
    }


} // NKikimr
