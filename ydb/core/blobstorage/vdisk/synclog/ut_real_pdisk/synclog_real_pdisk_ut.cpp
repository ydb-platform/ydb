#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogdata.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgwriter.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper_committer.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/pdisk_io/aio.h>

#include <library/cpp/testing/unittest/registar.h>

#include <deque>
#include <functional>

namespace NKikimr {

namespace {

constexpr ui32 NodeIndex = 0;
constexpr ui32 PDiskId = 1;
constexpr ui64 PDiskGuid = 1;
constexpr ui32 DefaultSectorSize = 4_KB;
constexpr ui32 DefaultChunkSize = 16_KB;
constexpr ui32 DefaultSyncLogAdvisedIndexedBlockSize = 1_KB;
constexpr ui64 DefaultDiskSize = 256_MB;
constexpr ui64 DomainId = 31;
constexpr ui64 MainKeyValue = 0x7e5700007e570000ull;

struct TRealPDiskTestConfig {
    ui32 SectorSize = DefaultSectorSize;
    ui32 ChunkSize = DefaultChunkSize;
    ui32 SyncLogAdvisedIndexedBlockSize = DefaultSyncLogAdvisedIndexedBlockSize;
    ui64 DiskSize = DefaultDiskSize;
    ui64 SyncLogMaxMemAmount = 0;
    ui64 SyncLogMaxDiskAmount = 0;
    ui32 MaxLogoBlobDataSize = 0;
    ui32 MinHugeBlobInBytes = 0;
    ui32 MilestoneHugeBlobInBytes = 0;
    ui64 MaxCommonLogChunks = 0;
    ui32 GroupId = 0;
    bool EnablePhantomFlagStorage = false;
    ui64 PhantomFlagStorageLimit = 64_MB;
    ui64 VolatilePhantomFlagStorageBlobSizeLimit = 0;
    bool EnableSmallDiskOptimization = true;
    bool RunSyncer = false;
    TDuration AdvanceEntryPointTimeout = TDuration::Seconds(5);
    TDuration RecoveryLogCutterFirstDuration = TDuration::Seconds(3);
    TDuration RecoveryLogCutterRegularDuration = TDuration::Seconds(5);
    TString PDiskPathSuffix = "synclog_real_vdisk_pdisk.dat";

    ui64 EffectiveSyncLogMaxMemAmount() const {
        return SyncLogMaxMemAmount ? SyncLogMaxMemAmount : 4 * ChunkSize;
    }

    ui64 EffectiveSyncLogMaxDiskAmount() const {
        return SyncLogMaxDiskAmount ? SyncLogMaxDiskAmount : 2 * ChunkSize;
    }

    ui32 EffectiveMaxLogoBlobDataSize() const {
        return MaxLogoBlobDataSize ? MaxLogoBlobDataSize : ChunkSize - 1024;
    }

    ui32 EffectiveMinHugeBlobInBytes() const {
        return MinHugeBlobInBytes ? MinHugeBlobInBytes : ChunkSize / 2;
    }

    ui32 EffectiveMilestoneHugeBlobInBytes() const {
        return MilestoneHugeBlobInBytes ? MilestoneHugeBlobInBytes : ChunkSize * 3 / 4;
    }
};

struct TRuntimeContext : NActors::IDestructable {
    std::shared_ptr<NPDisk::IIoContextFactory> IoContextFactory = std::make_shared<NPDisk::TIoContextFactoryOSS>();
};

TTestActorRuntime::TEgg MakeRuntimeEgg() {
    auto app = MakeHolder<TAppData>(0, 1, 2, 3, TMap<TString, ui32>{}, nullptr, nullptr, nullptr, nullptr);
    auto domains = MakeIntrusive<TDomainsInfo>();
    domains->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1", DomainId).Release());
    domains->AddHive(MakeDefaultHiveID());
    app->DomainsInfo = domains;

    auto context = MakeHolder<TRuntimeContext>();
    app->IoContextFactory = context->IoContextFactory.get();

    return {
        .App0 = app.Release(),
        .Opaque = context.Release(),
        .KeyConfigGenerator = nullptr,
        .Icb = {},
    };
}

TString MakeData(ui32 size) {
    TString data = TString::Uninitialized(size);
    char *p = data.Detach();
    for (ui32 i = 0; i < size; ++i) {
        p[i] = 'a' + i % 23;
    }
    return data;
}

TString MakeLocalSyncDataBlockPayload(ui32 minPayloadBytes) {
    NSyncLog::TNaiveFragmentWriter writer;
    ui64 lsn = 1;
    while (writer.GetSize() < minPayloadBytes) {
        char buf[NSyncLog::MaxRecFullSize];
        const ui32 generation = 1 + static_cast<ui32>(lsn % 10'000);
        const ui32 size = NSyncLog::TSerializeRoutines::SetBlock(buf, lsn,
            1'000'000 + lsn, generation, 0);
        writer.Push(reinterpret_cast<const NSyncLog::TRecordHdr*>(buf), size);
        ++lsn;
    }

    TString result;
    writer.Finish(&result);
    return result;
}

void DispatchFor(TTestActorRuntime& runtime, TDuration timeout) {
    TDispatchOptions options;
    const TDuration oldDispatchTimeout = runtime.SetDispatchTimeout(TDuration::MilliSeconds(10));
    try {
        runtime.DispatchEvents(options, timeout);
    } catch (const TEmptyEventQueueException&) {
    }
    runtime.SetDispatchTimeout(oldDispatchTimeout);
}

template <typename TPredicate>
bool PumpUntil(TTestActorRuntime& runtime, TPredicate&& predicate, ui32 iterations, TDuration step) {
    bool ok = predicate();
    for (ui32 i = 0; i < iterations && !ok; ++i) {
        DispatchFor(runtime, step);
        ok = predicate();
    }
    return ok;
}

bool WaitVDiskReady(TTestActorRuntime& runtime, const TActorId& vdiskActorId, const TVDiskID& vdiskId,
        const TActorId& edge) {
    return PumpUntil(runtime, [&] {
        runtime.Send(new IEventHandle(vdiskActorId, edge,
            new TEvBlobStorage::TEvVStatus(vdiskId), IEventHandle::FlagTrackDelivery), NodeIndex);
        auto res = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVStatusResult>(edge, TDuration::Seconds(1));
        return res && res->Get()->Record.GetStatus() == NKikimrProto::OK;
    }, 60, TDuration::MilliSeconds(100));
}

TIntrusivePtr<TBlobStorageGroupInfo> MakeGroupInfo(ui32 nodeId, ui32 groupId, TBlobStorageGroupType::EErasureSpecies erasure) {
    const TBlobStorageGroupType groupType(erasure);
    const ui32 numVDisks = groupType.BlobSubgroupSize();
    TVector<TActorId> actorIds;
    for (ui32 i = 0; i < numVDisks; ++i) {
        actorIds.push_back(MakeBlobStorageVDiskID(nodeId, PDiskId, i));
    }
    return new TBlobStorageGroupInfo(groupType, 1, numVDisks, 1,
        &actorIds, TBlobStorageGroupInfo::EEM_NONE, TBlobStorageGroupInfo::ELCP_IN_USE,
        TCypherKey(), TGroupId::FromValue(groupId));
}

TIntrusivePtr<TVDiskConfig> MakeTestVDiskConfig(const TIntrusivePtr<TAllVDiskKinds>& allVDiskKinds,
        const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TActorId& pdiskServiceId, ui32 slotId,
        NPDisk::TOwnerRound ownerRound, const TRealPDiskTestConfig& testConfig = {}) {
    const TVDiskID vdiskId = info->GetVDiskId(slotId);
    TVDiskConfig::TBaseInfo baseInfo(vdiskId, pdiskServiceId, PDiskGuid, PDiskId, NPDisk::DEVICE_TYPE_ROT,
        slotId, NKikimrBlobStorage::TVDiskKind::Default, ownerRound, "static");
    auto vdiskConfig = allVDiskKinds->MakeVDiskConfig(baseInfo);
    vdiskConfig->MaxLogoBlobDataSize = testConfig.EffectiveMaxLogoBlobDataSize();
    vdiskConfig->MinHugeBlobInBytes = testConfig.EffectiveMinHugeBlobInBytes();
    vdiskConfig->MilestoneHugeBlobInBytes = testConfig.EffectiveMilestoneHugeBlobInBytes();
    vdiskConfig->SyncLogMaxMemAmount = testConfig.EffectiveSyncLogMaxMemAmount();
    vdiskConfig->SyncLogMaxDiskAmount = testConfig.EffectiveSyncLogMaxDiskAmount();
    vdiskConfig->SyncLogAdvisedIndexedBlockSize = testConfig.SyncLogAdvisedIndexedBlockSize;
    vdiskConfig->AdvanceEntryPointTimeout = testConfig.AdvanceEntryPointTimeout;
    vdiskConfig->RecoveryLogCutterFirstDuration = testConfig.RecoveryLogCutterFirstDuration;
    vdiskConfig->RecoveryLogCutterRegularDuration = testConfig.RecoveryLogCutterRegularDuration;
    vdiskConfig->EnablePhantomFlagStorage = testConfig.EnablePhantomFlagStorage;
    vdiskConfig->PhantomFlagStorageLimit = testConfig.PhantomFlagStorageLimit;
    vdiskConfig->VolatilePhantomFlagStorageBlobSizeLimit =
        testConfig.VolatilePhantomFlagStorageBlobSizeLimit;
    vdiskConfig->RunRepl = false;
    vdiskConfig->RunSyncer = testConfig.RunSyncer;
    vdiskConfig->UseCostTracker = false;
    return vdiskConfig;
}

struct TRealStorage {
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TAllVDiskKinds> AllVDiskKinds;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorId PDiskServiceId;
    TVector<TActorId> VDiskActors;
    TActorId Edge;
    TActorId PutQueue;
};

struct TSyncLogFootprint {
    ui32 MemPages = 0;
    ui32 DiskChunks = 0;
    ui32 IndexedPages = 0;
    ui32 DiskUsedPages = 0;
    ui32 DiskIndexRecs = 0;
    ui32 EntryPointBytes = 0;
    ui32 EntryPointIndexRecs = 0;
    ui32 IndexBulk = 0;
    ui32 AppendBlockSize = 0;
    ui32 ChunkSize = 0;
    ui64 DiskLastLsn = 0;
    ui64 LastLsn = 0;

    TString ToString() const {
        return TStringBuilder()
            << "{MemPages# " << MemPages
            << " DiskChunks# " << DiskChunks
            << " IndexedPages# " << IndexedPages
            << " DiskUsedPages# " << DiskUsedPages
            << " DiskIndexRecs# " << DiskIndexRecs
            << " EntryPointBytes# " << EntryPointBytes
            << " EntryPointIndexRecs# " << EntryPointIndexRecs
            << " IndexBulk# " << IndexBulk
            << " AppendBlockSize# " << AppendBlockSize
            << " ChunkSize# " << ChunkSize
            << " DiskLastLsn# " << DiskLastLsn
            << " LastLsn# " << LastLsn
            << "}";
    }
};

TSyncLogFootprint GetSyncLogFootprint(const NSyncLog::TSyncLogSnapshotPtr& snapshot) {
    UNIT_ASSERT_C(snapshot, "SyncLog snapshot is null");

    TSyncLogFootprint footprint;
    footprint.AppendBlockSize = snapshot->AppendBlockSize;
    footprint.EntryPointBytes = snapshot->LastEntryPointDbgInfo.ByteSize;
    footprint.EntryPointIndexRecs = snapshot->LastEntryPointDbgInfo.IndexRecsNum;
    footprint.MemPages = snapshot->MemSnapPtr ? snapshot->MemSnapPtr->Size() : 0;
    if (snapshot->MemSnapPtr && !snapshot->MemSnapPtr->Empty()) {
        footprint.LastLsn = Max(footprint.LastLsn, snapshot->MemSnapPtr->GetLastLsn());
    }

    if (snapshot->DiskSnapPtr) {
        footprint.ChunkSize = snapshot->DiskSnapPtr->ChunkSize;
        footprint.IndexBulk = snapshot->DiskSnapPtr->IndexBulk;

        TString serialized;
        TStringOutput output(serialized);
        snapshot->DiskSnapPtr->Serialize(output,
            NSyncLog::TDeltaToDiskRecLog(snapshot->DiskSnapPtr->IndexBulk));

        const char *pos = serialized.data();
        const char *end = pos + serialized.size();
        auto read = [&](auto& value) {
            UNIT_ASSERT_C(static_cast<size_t>(end - pos) >= sizeof(value),
                "truncated serialized SyncLog disk snapshot"
                << " footprint# " << footprint.ToString()
                << " serializedSize# " << serialized.size());
            memcpy(&value, pos, sizeof(value));
            pos += sizeof(value);
        };

        ui32 chunksNum = 0;
        read(chunksNum);
        THashSet<TChunkIdx> chunks;
        for (ui32 chunk = 0; chunk < chunksNum; ++chunk) {
            ui32 chunkIdx = 0;
            ui64 lastRealLsn = 0;
            ui32 indexRecsNum = 0;
            read(chunkIdx);
            read(lastRealLsn);
            read(indexRecsNum);
            chunks.insert(chunkIdx);
            footprint.DiskLastLsn = Max(footprint.DiskLastLsn, lastRealLsn);
            footprint.LastLsn = Max(footprint.LastLsn, lastRealLsn);

            footprint.DiskIndexRecs += indexRecsNum;
            ui32 chunkUsedPages = 0;
            for (ui32 indexRec = 0; indexRec < indexRecsNum; ++indexRec) {
                ui64 firstLsn = 0;
                ui16 offsetInPages = 0;
                ui16 pagesNum = 0;
                read(firstLsn);
                read(offsetInPages);
                read(pagesNum);
                Y_UNUSED(firstLsn);
                footprint.IndexedPages += pagesNum;
                chunkUsedPages = Max<ui32>(chunkUsedPages, offsetInPages + pagesNum);
            }
            footprint.DiskUsedPages += chunkUsedPages;
        }
        footprint.DiskChunks = chunks.size();
        UNIT_ASSERT_C(pos == end,
            "unexpected tail in serialized SyncLog disk snapshot"
            << " tailBytes# " << end - pos
            << " serializedSize# " << serialized.size());
    }

    return footprint;
}

class TTestRuntimeCallbackGuard {
public:
    TTestRuntimeCallbackGuard(TTestActorRuntimeBase& runtime,
            TTestActorRuntimeBase::TEventObserver previousObserver,
            TTestActorRuntimeBase::TEventFilter previousEventFilter,
            TTestActorRuntimeBase::TRegistrationObserver previousRegistrationObserver =
                &TTestActorRuntimeBase::DefaultRegistrationObserver)
        : Runtime(runtime)
        , PreviousObserver(std::move(previousObserver))
        , PreviousEventFilter(std::move(previousEventFilter))
        , PreviousRegistrationObserver(std::move(previousRegistrationObserver))
    {}

    TTestRuntimeCallbackGuard(const TTestRuntimeCallbackGuard&) = delete;
    TTestRuntimeCallbackGuard& operator=(const TTestRuntimeCallbackGuard&) = delete;

    ~TTestRuntimeCallbackGuard() {
        Runtime.SetEventFilter(std::move(PreviousEventFilter));
        Runtime.SetObserverFunc(std::move(PreviousObserver));
        Runtime.SetRegistrationObserverFunc(std::move(PreviousRegistrationObserver));
    }

private:
    TTestActorRuntimeBase& Runtime;
    TTestActorRuntimeBase::TEventObserver PreviousObserver;
    TTestActorRuntimeBase::TEventFilter PreviousEventFilter;
    TTestActorRuntimeBase::TRegistrationObserver PreviousRegistrationObserver;
};

TRealStorage SetupRealPDiskAndRealVDisk(TTestActorRuntime& runtime,
        TBlobStorageGroupType::EErasureSpecies erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        const TRealPDiskTestConfig& testConfig = {}) {
    runtime.Initialize(MakeRuntimeEgg());

    const ui32 nodeId = runtime.GetNodeId(NodeIndex);
    const ui32 groupId = testConfig.GroupId;
    const TBlobStorageGroupType groupType(erasure);
    const ui32 numVDisks = groupType.BlobSubgroupSize();
    const TString pdiskPath = TStringBuilder() << runtime.GetTempDir() << testConfig.PDiskPathSuffix;
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    const TActorId pdiskServiceId = MakeBlobStoragePDiskID(nodeId, PDiskId);

    const auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(testConfig.DiskSize);
    TFormatOptions formatOptions;
    formatOptions.SectorMap = sectorMap;
    formatOptions.EnableSmallDiskOptimization = testConfig.EnableSmallDiskOptimization;
    formatOptions.EnableSectorEncryption = false;
    FormatPDisk(pdiskPath, testConfig.DiskSize, testConfig.SectorSize, testConfig.ChunkSize, PDiskGuid, MainKeyValue, MainKeyValue, MainKeyValue,
        MainKeyValue,
        "synclog real vdisk race test", formatOptions);
    auto pdiskConfig = MakeIntrusive<TPDiskConfig>(pdiskPath, PDiskGuid, PDiskId,
        TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
    pdiskConfig->ChunkSize = testConfig.ChunkSize;
    pdiskConfig->SectorSize = testConfig.SectorSize;
    pdiskConfig->SectorMap = sectorMap;
    pdiskConfig->FeatureFlags.SetEnablePDiskDataEncryption(false);
    pdiskConfig->FeatureFlags.SetEnableSmallDiskOptimization(testConfig.EnableSmallDiskOptimization);
    pdiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
    pdiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
    if (testConfig.MaxCommonLogChunks) {
        pdiskConfig->MaxCommonLogChunks = testConfig.MaxCommonLogChunks;
    }

    const NPDisk::TMainKey mainKey{ .Keys = { MainKeyValue }, .IsInitialized = true };
    const TActorId pdiskActorId = runtime.Register(CreatePDisk(pdiskConfig, mainKey, counters), NodeIndex, 0,
        TMailboxType::Revolving);
    runtime.RegisterService(pdiskServiceId, pdiskActorId, NodeIndex);

    auto info = MakeGroupInfo(nodeId, groupId, erasure);
    auto allVDiskKinds = MakeIntrusive<TAllVDiskKinds>();
    TVector<TActorId> vdiskActors;
    for (ui32 i = 0; i < numVDisks; ++i) {
        const TActorId vdiskActorId = info->GetActorId(i);
        auto vdiskConfig = MakeTestVDiskConfig(allVDiskKinds, info, pdiskServiceId, i, 11 + i, testConfig);

        const TActorId vdiskActor = runtime.Register(CreateVDisk(vdiskConfig, info,
            counters->GetSubgroup("subsystem", "vdisk")->GetSubgroup("slot", ToString(i))),
            NodeIndex, 0, TMailboxType::Revolving);
        runtime.RegisterService(vdiskActorId, vdiskActor, NodeIndex);
        vdiskActors.push_back(vdiskActor);
    }

    const TActorId queueEdge = runtime.AllocateEdgeActor(NodeIndex);
    const auto queueId = NKikimrBlobStorage::EVDiskQueueId::PutTabletLog;
    const TString queueName = NKikimrBlobStorage::EVDiskQueueId_Name(queueId);
    auto bspctx = MakeIntrusive<TBSProxyContext>(counters->GetSubgroup("subsystem", "bsp"));
    TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord = new NBackpressure::TFlowRecord;
    std::unique_ptr<IActor> queueActor(CreateVDiskBackpressureClient(info, info->GetVDiskId(0),
        queueId, counters->GetSubgroup("subsystem", "queue"), bspctx,
        NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::DSProxy, nodeId), queueName, 0, false,
        TDuration::Seconds(60), flowRecord, NMonitoring::TCountableBase::EVisibility::Private));
    const TActorId putQueue = runtime.Register(queueActor.release(), NodeIndex, 0, TMailboxType::Revolving, 0,
        queueEdge);
    UNIT_ASSERT_C([&] {
        for (ui32 i = 0; i < 100; ++i) {
            TEvProxyQueueState::TPtr ev;
            try {
                ev = runtime.GrabEdgeEvent<TEvProxyQueueState>(queueEdge, TDuration::Seconds(1));
            } catch (const TEmptyEventQueueException&) {
                continue;
            }
            if (ev && ev->Get()->IsConnected) {
                return true;
            }
        }
        return false;
    }(), "PutTabletLog queue did not connect to target VDisk");

    return {
        .Info = info,
        .AllVDiskKinds = allVDiskKinds,
        .Counters = counters,
        .PDiskServiceId = pdiskServiceId,
        .VDiskActors = std::move(vdiskActors),
        .Edge = runtime.AllocateEdgeActor(NodeIndex),
        .PutQueue = putQueue,
    };
}

class TLocalSyncDataReplayEnv {
public:
    TTestActorRuntime Runtime;
    TRealPDiskTestConfig TestConfig;
    TString LocalSyncData;
    TRealStorage Storage;
    TVDiskID VDiskId;
    TActorId VDiskActorId;
    TActorId Edge;
    TActorId SkeletonId;
    TActorId CurrentVDiskFront;

    bool RestartRequested = false;
    bool DropNextLocalSyncDataLogResult = false;
    bool ObserveAfterRestart = false;
    bool LocalSyncDataOutOfSpace = false;
    bool OtherOutOfSpace = false;
    bool CutRequested = false;
    bool CutReachedInterruptedLsn = false;
    bool StartupTokenBeforeCut = false;
    bool CaptureCheckSpaceResult = false;
    bool CheckSpaceResultReceived = false;
    ui32 InterruptedWrites = 0;
    ui32 RestartNo = 0;
    ui32 CutRequestsAfterRestart = 0;
    ui64 InterruptedLsn = 0;
    ui64 LastCutFirstLsnToKeep = 0;
    i64 MaxLogChunkCount = 0;
    NPDisk::TOwner CutOwner = {};
    NPDisk::TOwnerRound CutOwnerRound = {};
    NKikimrProto::EReplyStatus CheckSpaceStatus = {};
    NPDisk::TStatusFlags CheckSpaceLogStatusFlags = {};
    TString CheckSpaceErrorReason;
    TString Details;
    std::function<bool(TAutoPtr<IEventHandle>&)> PreFilter;

    TLocalSyncDataReplayEnv(ui32 targetPayloadBytes, TRealPDiskTestConfig testConfig)
        : Runtime(1, false)
        , TestConfig(std::move(testConfig))
        , LocalSyncData(MakeLocalSyncDataBlockPayload(targetPayloadBytes))
    {
        auto previousRegistrationObserver = Runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase&, const TActorId& parentId, const TActorId& actorId) {
                Registrations.emplace_back(parentId, actorId);
                if (parentId == CurrentVDiskFront && !SkeletonId) {
                    SkeletonId = actorId;
                }
            });

        Storage = SetupRealPDiskAndRealVDisk(Runtime, TBlobStorageGroupType::ErasureNone, TestConfig);
        Runtime.GetAppData(NodeIndex).FeatureFlags
            .SetEnableVDiskWaitForRecoveryLogCutOnLocalSyncDataReplay(true);
        VDiskId = Storage.Info->GetVDiskId(0);
        VDiskActorId = Storage.Info->GetActorId(0);
        Edge = Storage.Edge;
        CurrentVDiskFront = Storage.VDiskActors[0];
        for (const auto& [parentId, actorId] : Registrations) {
            if (parentId == CurrentVDiskFront) {
                SkeletonId = actorId;
                break;
            }
        }

        auto previousEventFilter = Runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                return EventFilter(ev);
            });
        CallbackGuard = MakeHolder<TTestRuntimeCallbackGuard>(Runtime,
            &TTestActorRuntimeBase::DefaultObserverFunc, std::move(previousEventFilter),
            std::move(previousRegistrationObserver));
    }

    bool ObservedOutOfSpace() const {
        return LocalSyncDataOutOfSpace || OtherOutOfSpace;
    }

    void DispatchFor(TDuration timeout) {
        NKikimr::DispatchFor(Runtime, timeout);
    }

    template <typename TPredicate>
    bool PumpUntil(TPredicate predicate, ui32 iterations, TDuration step) {
        return NKikimr::PumpUntil(Runtime, std::move(predicate), iterations, step);
    }

    bool WaitReady() {
        return WaitVDiskReady(Runtime, VDiskActorId, VDiskId, Edge);
    }

    void SendInterruptedLocalSyncData() {
        RestartRequested = false;
        DropNextLocalSyncDataLogResult = true;
        Runtime.Send(new IEventHandle(SkeletonId, Edge,
            new TEvLocalSyncData(VDiskId, TSyncState(), LocalSyncData)), NodeIndex);

        UNIT_ASSERT_C(PumpUntil([&] { return RestartRequested || ObservedOutOfSpace(); },
                600, TDuration::MilliSeconds(10)),
            "LocalSyncData write was not committed to PDisk"
            << " interruptedWrites# " << InterruptedWrites
            << " payloadSize# " << LocalSyncData.size());
        DropNextLocalSyncDataLogResult = false;
    }

    void RestartVDisk() {
        Runtime.Send(new IEventHandle(Storage.VDiskActors[0], Edge, new TEvents::TEvPoisonPill), NodeIndex);
        DispatchFor(TDuration::MilliSeconds(100));

        const NPDisk::TOwnerRound nextOwnerRound = 200 + RestartNo++;
        auto vdiskConfig = MakeTestVDiskConfig(Storage.AllVDiskKinds, Storage.Info, Storage.PDiskServiceId,
            0, nextOwnerRound, TestConfig);
        const TActorId vdiskActor = Runtime.Register(CreateVDisk(vdiskConfig, Storage.Info,
            Storage.Counters->GetSubgroup("subsystem", "vdisk")->GetSubgroup("slot", "0")),
            NodeIndex, 0, TMailboxType::Revolving);
        CurrentVDiskFront = vdiskActor;
        SkeletonId = {};
        ObserveAfterRestart = true;
        Runtime.RegisterService(VDiskActorId, vdiskActor, NodeIndex);
        Storage.VDiskActors[0] = vdiskActor;
        UNIT_ASSERT_C(PumpUntil([&] { return bool(SkeletonId); }, 300, TDuration::MilliSeconds(10)),
            "failed to discover Skeleton actor after restart"
            << " interruptedWrites# " << InterruptedWrites);
    }

    void WaitForCut() {
        PumpUntil([&] { return CutRequested || ObservedOutOfSpace(); },
            100, TDuration::MilliSeconds(10));
        if (CutRequested) {
            PumpUntil([&] { return CutReachedInterruptedLsn || ObservedOutOfSpace(); },
                600, TDuration::MilliSeconds(10));
        }
    }

    void WaitForCommonLogChunkReclaim(
            NKikimrBlobStorage::TPDiskSpaceColor::E maxAcceptableColor) {
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        if (ObservedOutOfSpace()) {
            return;
        }
        UNIT_ASSERT_C(CutOwner && CutOwnerRound,
            "test did not capture the owner of the recovery log cut"
            << " interruptedWrites# " << InterruptedWrites);

        auto lastColor = TColor::BLACK;
        NPDisk::TStatusFlags lastFlags = {};
        auto sendCheckSpace = [&] {
            Runtime.Send(new IEventHandle(Storage.PDiskServiceId, Edge,
                new NPDisk::TEvCheckSpace(CutOwner, CutOwnerRound)), NodeIndex);
        };

        CaptureCheckSpaceResult = true;
        CheckSpaceResultReceived = false;
        sendCheckSpace();
        const bool waitCompleted = PumpUntil([&] {
            if (ObservedOutOfSpace()) {
                return true;
            }
            if (!CheckSpaceResultReceived) {
                return false;
            }
            CheckSpaceResultReceived = false;
            UNIT_ASSERT_C(CheckSpaceStatus == NKikimrProto::OK,
                "TEvCheckSpace failed"
                << " owner# " << ui32(CutOwner)
                << " ownerRound# " << CutOwnerRound
                << " status# " << NKikimrProto::EReplyStatus_Name(CheckSpaceStatus)
                << " error# " << CheckSpaceErrorReason);

            lastFlags = CheckSpaceLogStatusFlags;
            lastColor = StatusFlagToSpaceColor(lastFlags);
            if (lastColor <= maxAcceptableColor) {
                return true;
            }
            sendCheckSpace();
            return false;
        }, 600, TDuration::MilliSeconds(10));
        CaptureCheckSpaceResult = false;

        if (ObservedOutOfSpace()) {
            return;
        }

        UNIT_ASSERT_C(waitCompleted,
            "common-log quota was not returned after recovery log cut"
            << " owner# " << ui32(CutOwner)
            << " ownerRound# " << CutOwnerRound
            << " logStatusFlags# " << NPDisk::StatusFlagsToString(lastFlags)
            << " logSpaceColor# " << TColor::E_Name(lastColor)
            << " maxAcceptableColor# " << TColor::E_Name(maxAcceptableColor)
            << " interruptedWrites# " << InterruptedWrites);
    }

private:
    TVector<std::pair<TActorId, TActorId>> Registrations;
    THolder<TTestRuntimeCallbackGuard> CallbackGuard;

    void SetDetails(TString value) {
        if (Details.empty()) {
            Details = std::move(value);
        }
    }

    bool EventFilter(TAutoPtr<IEventHandle>& ev) {
        if (!ev) {
            return false;
        }
        if (PreFilter && PreFilter(ev)) {
            return true;
        }

        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvAcquireVDiskOperationToken:
                if (ObserveAfterRestart && !CutReachedInterruptedLsn) {
                    StartupTokenBeforeCut = true;
                }
                break;

            case TEvBlobStorage::EvAskForCutLog:
                if (ObserveAfterRestart) {
                    const auto *msg = ev->Get<NPDisk::TEvAskForCutLog>();
                    CutOwner = msg->Owner;
                    CutOwnerRound = msg->OwnerRound;
                    CutRequested = true;
                    ++CutRequestsAfterRestart;
                }
                break;

            case TEvBlobStorage::EvRecoveryLogCutDone:
                if (ObserveAfterRestart) {
                    const auto *msg = ev->Get<TEvRecoveryLogCutDone>();
                    LastCutFirstLsnToKeep = msg->FirstLsnToKeep;
                    if (InterruptedLsn && msg->FirstLsnToKeep >= InterruptedLsn + 1) {
                        CutReachedInterruptedLsn = true;
                    }
                }
                break;

            case TEvBlobStorage::EvCheckSpaceResult:
                if (CaptureCheckSpaceResult && ev->Recipient == Edge) {
                    const auto *msg = ev->Get<NPDisk::TEvCheckSpaceResult>();
                    CheckSpaceStatus = msg->Status;
                    CheckSpaceLogStatusFlags = msg->LogStatusFlags;
                    CheckSpaceErrorReason = msg->ErrorReason;
                    CheckSpaceResultReceived = true;
                    return true;
                }
                break;

            case TEvBlobStorage::EvLogResult: {
                const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                MaxLogChunkCount = Max(MaxLogChunkCount, msg->LogChunkCount);

                if (msg->Status == NKikimrProto::OUT_OF_SPACE) {
                    TString outOfSpaceDetails = TStringBuilder()
                        << "TEvLogResult recipient# " << ev->Recipient
                        << " status# " << NKikimrProto::EReplyStatus_Name(msg->Status)
                        << " error# " << msg->ErrorReason
                        << " logChunkCount# " << msg->LogChunkCount
                        << " interruptedWrites# " << InterruptedWrites;
                    if (DropNextLocalSyncDataLogResult) {
                        LocalSyncDataOutOfSpace = true;
                    } else {
                        OtherOutOfSpace = true;
                    }
                    SetDetails(std::move(outOfSpaceDetails));
                    return true;
                }

                if (DropNextLocalSyncDataLogResult && msg->Status == NKikimrProto::OK) {
                    DropNextLocalSyncDataLogResult = false;
                    RestartRequested = true;
                    ++InterruptedWrites;
                    Y_ABORT_UNLESS(!msg->Results.empty());
                    InterruptedLsn = msg->Results.front().Lsn;
                    CutRequested = false;
                    CutReachedInterruptedLsn = false;
                    CutOwner = {};
                    CutOwnerRound = {};
                    return true;
                }
                break;
            }

            case TEvBlobStorage::EvChunkReserveResult:
                if (ev->Get<NPDisk::TEvChunkReserveResult>()->Status == NKikimrProto::OUT_OF_SPACE) {
                    OtherOutOfSpace = true;
                    SetDetails("TEvChunkReserveResult OUT_OF_SPACE");
                    return true;
                }
                break;

            case TEvBlobStorage::EvChunkWriteResult:
                if (ev->Get<NPDisk::TEvChunkWriteResult>()->Status == NKikimrProto::OUT_OF_SPACE) {
                    OtherOutOfSpace = true;
                    SetDetails("TEvChunkWriteResult OUT_OF_SPACE");
                    return true;
                }
                break;
        }

        return false;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TBlobStorageSyncLogRealPDisk) {

    Y_UNIT_TEST(SyncLogUsesMemAndDiskLimits) {
        /*
        * This test intentionally goes through a real VDisk and a real PDisk instead of sending
        * synthetic TEvSyncLogPut events directly to SyncLog. The point is to verify that the configured
        * SyncLog memory and disk limits are honored on the production VDisk path. The group must use a
        * real replicated erasure species because SyncLog ignores ordinary put records for ErasureNone.
        *
        * VCollectGarbage with a large DoNotKeep list is used as a compact way to generate many ordinary
        * SyncLog records without depending on large blob payloads. The observer tracks real PDisk log
        * commits and requires a SyncLog commit with deleted chunks, so the final footprint check cannot
        * pass without exercising disk pressure. The final settling loop accounts for SyncLog's async
        * cleanup: after disk overflow has removed old chunks, the memory snapshot can still contain the
        * tail of the last batch until the next SyncLog action.
        */
        TTestActorRuntime runtime(1, false);
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 512_KB;
        testConfig.SyncLogAdvisedIndexedBlockSize = 1_KB;
        testConfig.SyncLogMaxMemAmount = 16_KB;
        testConfig.SyncLogMaxDiskAmount = 2 * testConfig.ChunkSize;
        testConfig.MaxLogoBlobDataSize = 192_KB;
        testConfig.MinHugeBlobInBytes = 64_KB;
        testConfig.MilestoneHugeBlobInBytes = 128_KB;
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "synclog_real_limits.dat";

        auto storage = SetupRealPDiskAndRealVDisk(runtime, TBlobStorageGroupType::ErasureMirror3of4, testConfig);
        const auto& info = storage.Info;
        const TActorId edge = storage.Edge;
        const TActorId putQueue = storage.PutQueue;
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 maxObservedDataLsn = 0;
        ui32 syncLogDataCommits = 0;
        ui32 syncLogDeleteCommits = 0;
        ui32 syncLogCommitDoneEvents = 0;
        TString lastPDiskErrorReason;

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const ui32 signature = msg.Signature.GetUnmasked();
            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;

            if (!syncLogEntryPointCommit) {
                if (!gotOwner) {
                    owner = msg.Owner;
                    gotOwner = true;
                } else if (msg.Owner != owner) {
                    return;
                }
                ownerRound = msg.OwnerRound;
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
                return;
            }

            if (!gotOwner || msg.Owner != owner) {
                return;
            }
            if (msg.CommitRecord.CommitChunks) {
                ++syncLogDataCommits;
            }
            if (msg.CommitRecord.DeleteChunks) {
                ++syncLogDeleteCommits;
            }
        };

        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        observePDiskLog(*ev->Get<NPDisk::TEvLog>());
                    }
                    break;

                case TEvBlobStorage::EvMultiLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        for (const auto& item : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            observePDiskLog(*item.Event);
                        }
                    }
                    break;

                case TEvBlobStorage::EvLogResult: {
                    const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                    if (msg->Status != NKikimrProto::OK) {
                        lastPDiskErrorReason = msg->ErrorReason;
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto previousEventFilter = runtime.SetEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
                return false;
            });
        TTestRuntimeCallbackGuard runtimeCallbackGuard(runtime,
            std::move(previousObserver), std::move(previousEventFilter));

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);

        auto writeTargetRecords = [&](ui64 tabletId, ui32 records) {
            const ui32 batchSize = 64;
            for (ui32 firstRecord = 0; firstRecord < records; firstRecord += batchSize) {
                const ui32 batch = Min(batchSize, records - firstRecord);
                auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, false);

                for (ui32 i = 0; i < batch; ++i) {
                    const TLogoBlobID blobId(TLogoBlobID(tabletId, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                    multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
                }

                runtime.Send(new IEventHandle(putQueue, edge, multiPut.release()), NodeIndex);
                auto putResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVMultiPutResult>(edge,
                    TDuration::Seconds(120));
                UNIT_ASSERT_C(putResult, "no put result; firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                UNIT_ASSERT_C(putResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "put status# " << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetStatus())
                    << " firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                UNIT_ASSERT_VALUES_EQUAL(putResult->Get()->Record.ItemsSize(), batch);
                for (ui32 i = 0; i < batch; ++i) {
                    UNIT_ASSERT_C(putResult->Get()->Record.GetItems(i).GetStatus() == NKikimrProto::OK,
                        "put item# " << i << " status# "
                        << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetItems(i).GetStatus())
                        << " firstRecord# " << firstRecord
                        << " lastPDiskError# " << lastPDiskErrorReason);
                }
                DispatchFor(runtime, TDuration::MilliSeconds(1));
            }
        };

        auto requestFootprint = [&] {
            runtime.Send(new IEventHandle(syncLogKeeperId, edge,
                new NSyncLog::TEvSyncLogSnapshot()), NodeIndex);
            auto res = runtime.GrabEdgeEvent<NSyncLog::TEvSyncLogSnapshotResult>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(res);
            return GetSyncLogFootprint(res->Get()->SnapshotPtr);
        };

        UNIT_ASSERT_C(WaitVDiskReady(runtime, vdiskActorId, vdiskId, edge), "target VDisk did not become ready");

        writeTargetRecords(42, 16);
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, 200, TDuration::MilliSeconds(10)),
            "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        ui32 nextCollectCounter = 1;
        auto collectDoNotKeep = [&](ui64 tabletId, ui32 records) {
            const ui32 batchSize = 2048;
            for (ui32 firstRecord = 0; firstRecord < records; firstRecord += batchSize) {
                const ui32 batch = Min(batchSize, records - firstRecord);
                UNIT_ASSERT(batch);
                TVector<TLogoBlobID> doNotKeep;
                doNotKeep.reserve(batch);
                for (ui32 i = 0; i < batch; ++i) {
                    doNotKeep.emplace_back(tabletId, 1, nextStep++, 0, 1, nextCookie++);
                }

                auto collect = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(tabletId, 1,
                    nextCollectCounter++, 0, false, 0, 0, false, nullptr, &doNotKeep, vdiskId,
                    TInstant::Max());
                runtime.Send(new IEventHandle(putQueue, edge, collect.release()), NodeIndex);
                auto collectResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge,
                    TDuration::Seconds(120));
                UNIT_ASSERT_C(collectResult, "no collect result; firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                UNIT_ASSERT_C(collectResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "collect status# " << NKikimrProto::EReplyStatus_Name(collectResult->Get()->Record.GetStatus())
                    << " firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                DispatchFor(runtime, TDuration::MilliSeconds(1));
            }
        };

        auto waitForSyncLogCommit = [&](ui32 previousDataCommits) {
            UNIT_ASSERT_C(PumpUntil(runtime, [&] {
                return syncLogDataCommits > previousDataCommits &&
                    syncLogCommitDoneEvents >= syncLogDataCommits;
            }, 1000, TDuration::MilliSeconds(10)),
                "SyncLog commit did not settle; previousDataCommits# " << previousDataCommits
                << " dataCommits# " << syncLogDataCommits
                << " commitDone# " << syncLogCommitDoneEvents
                << " lastPDiskError# " << lastPDiskErrorReason);
        };

        auto requestSyncLogCut = [&] {
            runtime.Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), NodeIndex);
            DispatchFor(runtime, TDuration::MilliSeconds(1));
        };

        for (ui32 pressureIterations = 0; syncLogDeleteCommits < 1 && pressureIterations < 96;
                ++pressureIterations) {
            const ui32 previousDataCommits = syncLogDataCommits;
            collectDoNotKeep(43, 2048);
            requestSyncLogCut();
            waitForSyncLogCommit(previousDataCommits);
        }

        UNIT_ASSERT_C(syncLogDeleteCommits,
            "test did not exercise SyncLog disk pressure");

        auto expectedMaxMemPages = [&](const TSyncLogFootprint& currentFootprint) {
            UNIT_ASSERT_C(currentFootprint.AppendBlockSize,
                "SyncLog snapshot has zero AppendBlockSize"
                << " footprint# " << currentFootprint.ToString());
            return Max<ui32>(2, testConfig.SyncLogMaxMemAmount / currentFootprint.AppendBlockSize);
        };

        auto expectedMaxDiskChunks = [&](const TSyncLogFootprint& currentFootprint) {
            UNIT_ASSERT_C(currentFootprint.ChunkSize,
                "SyncLog snapshot has zero ChunkSize"
                << " footprint# " << currentFootprint.ToString());
            return Max<ui32>(2, testConfig.SyncLogMaxDiskAmount / currentFootprint.ChunkSize);
        };

        auto expectedMaxDiskPages = [&](const TSyncLogFootprint& currentFootprint) {
            UNIT_ASSERT_C(currentFootprint.AppendBlockSize,
                "SyncLog snapshot has zero AppendBlockSize"
                << " footprint# " << currentFootprint.ToString());
            return expectedMaxDiskChunks(currentFootprint) *
                (currentFootprint.ChunkSize / currentFootprint.AppendBlockSize);
        };

        TSyncLogFootprint footprint;
        bool footprintWithinLimits = false;
        for (ui32 i = 0; i < 32 && !footprintWithinLimits; ++i) {
            footprint = requestFootprint();
            const bool memWithinLimits = footprint.MemPages <= expectedMaxMemPages(footprint);
            const bool diskWithinLimits = footprint.DiskChunks <= expectedMaxDiskChunks(footprint) &&
                footprint.DiskUsedPages <= expectedMaxDiskPages(footprint);
            footprintWithinLimits = memWithinLimits && diskWithinLimits;
            if (!footprintWithinLimits) {
                if (diskWithinLimits && !memWithinLimits) {
                    collectDoNotKeep(44, 1);
                    DispatchFor(runtime, TDuration::MilliSeconds(10));
                } else {
                    const ui32 previousDataCommits = syncLogDataCommits;
                    collectDoNotKeep(44, 512);
                    requestSyncLogCut();
                    waitForSyncLogCommit(previousDataCommits);
                }
            }
        }

        const ui32 maxMemPages = expectedMaxMemPages(footprint);
        const ui32 maxDiskChunks = expectedMaxDiskChunks(footprint);
        const ui32 maxDiskPages = expectedMaxDiskPages(footprint);

        UNIT_ASSERT_C(footprintWithinLimits,
            "SyncLog exceeded configured limits under pressure"
            << " footprint# " << footprint.ToString()
            << " expectedMaxMemPages# " << maxMemPages
            << " expectedMaxDiskChunks# " << maxDiskChunks
            << " expectedMaxDiskPages# " << maxDiskPages
            << " dataCommits# " << syncLogDataCommits
            << " deleteCommits# " << syncLogDeleteCommits
            << " commitDone# " << syncLogCommitDoneEvents
            << " lastPDiskError# " << lastPDiskErrorReason);
    }

    void TestCutLogAppendsDoNotFragmentDiskIndex(ui64 syncLogMaxMemAmount,
            bool requireNoMemoryOverflow, const TString& pdiskPathSuffix) {
        /*
         * A production VDisk periodically receives TEvCutLog even when SyncLog is far below its
         * memory limit. If only a few new pages appeared since the previous cut, BuildSwapSnap
         * writes just those pages. Historically every such append created a separate
         * TDiskIndexRecord, so every cut increased IndexRecsNum by one and the serialized entry
         * point by sizeof(TDiskIndexRecord) (12 bytes).
         *
         * Keep the real VDisk/PDisk path and explicitly reproduce that cadence. CutLog lags one
         * batch behind: after batch N is written, request a recovery-log cut at the last LSN of
         * batch N-1 plus one. Thus the pages selected for the swap no longer contain mutable
         * Pages.back(). Wait for the complete commit chain before starting another batch. Every
         * append is much smaller than IndexBulk, so adjacent appends should share index records.
         * The 1.25 GiB SyncLog disk limit is deliberately large enough that disk-pressure trimming
         * cannot hide linear index growth.
         */
        TTestActorRuntime runtime(1, false);
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 512_KB;
        testConfig.SyncLogAdvisedIndexedBlockSize = 1_MB;
        testConfig.SyncLogMaxMemAmount = syncLogMaxMemAmount;
        testConfig.SyncLogMaxDiskAmount = 1_GB + 256_MB;
        testConfig.MaxLogoBlobDataSize = 192_KB;
        testConfig.MinHugeBlobInBytes = 64_KB;
        testConfig.MilestoneHugeBlobInBytes = 128_KB;
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = pdiskPathSuffix;

        auto storage = SetupRealPDiskAndRealVDisk(runtime, TBlobStorageGroupType::ErasureMirror3of4, testConfig);
        const auto& info = storage.Info;
        const TActorId edge = storage.Edge;
        const TActorId putQueue = storage.PutQueue;
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 maxObservedDataLsn = 0;
        ui64 lastObservedGcLsn = 0;
        ui64 lastReportedSyncLogFirstLsnToKeep = 0;
        ui32 entryPointCommits = 0;
        ui32 syncLogCommitDoneEvents = 0;
        TVector<ui32> committedAppendPages;
        TString lastPDiskErrorReason;

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const bool syncLogEntryPointCommit =
                msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;

            if (!syncLogEntryPointCommit) {
                if (!gotOwner) {
                    owner = msg.Owner;
                    gotOwner = true;
                } else if (msg.Owner != owner) {
                    return;
                }
                ownerRound = msg.OwnerRound;
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
                if (msg.Signature.GetUnmasked() == TLogSignature::SignatureGC) {
                    lastObservedGcLsn = msg.Lsn;
                }
            } else if (gotOwner && msg.Owner == owner) {
                ++entryPointCommits;
            }
        };

        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        observePDiskLog(*ev->Get<NPDisk::TEvLog>());
                    }
                    break;

                case TEvBlobStorage::EvMultiLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        for (const auto& item : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            observePDiskLog(*item.Event);
                        }
                    }
                    break;

                case TEvBlobStorage::EvLogResult: {
                    const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                    if (msg->Status != NKikimrProto::OK) {
                        lastPDiskErrorReason = msg->ErrorReason;
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    if (ev->Recipient == syncLogKeeperId) {
                        ++syncLogCommitDoneEvents;
                        const auto *msg = ev->Get<NSyncLog::TEvSyncLogCommitDone>();
                        for (const auto& append : msg->Delta.AllAppends) {
                            committedAppendPages.push_back(append.Pages.size());
                            UNIT_ASSERT_C(append.Pages.size() < msg->Delta.IndexBulk,
                                "CutLog disk append must be smaller than IndexBulk"
                                << " appendPages# " << append.Pages.size()
                                << " IndexBulk# " << msg->Delta.IndexBulk);
                        }
                    }
                    break;

                case TEvBlobStorage::EvVDiskCutLog: {
                    const auto *msg = ev->Get<TEvVDiskCutLog>();
                    if (ev->Sender == syncLogKeeperId && msg->Component == TEvVDiskCutLog::SyncLog) {
                        lastReportedSyncLogFirstLsnToKeep =
                            Max(lastReportedSyncLogFirstLsnToKeep, msg->LastKeepLsn);
                    }
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto previousEventFilter = runtime.SetEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
                return false;
            });
        TTestRuntimeCallbackGuard runtimeCallbackGuard(runtime,
            std::move(previousObserver), std::move(previousEventFilter));

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);
        auto writeTargetRecords = [&](ui32 records) {
            auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
            for (ui32 i = 0; i < records; ++i) {
                const TLogoBlobID blobId(TLogoBlobID(42, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
            }

            runtime.Send(new IEventHandle(putQueue, edge, multiPut.release()), NodeIndex);
            auto putResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVMultiPutResult>(edge,
                TDuration::Seconds(120));
            UNIT_ASSERT_C(putResult, "no put result; lastPDiskError# " << lastPDiskErrorReason);
            UNIT_ASSERT_C(putResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                "put status# " << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetStatus())
                << " lastPDiskError# " << lastPDiskErrorReason);
        };

        ui32 nextCollectCounter = 1;
        auto collectDoNotKeep = [&](ui32 records) {
            TVector<TLogoBlobID> doNotKeep;
            doNotKeep.reserve(records);
            for (ui32 i = 0; i < records; ++i) {
                doNotKeep.emplace_back(43, 1, nextStep++, 0, 1, nextCookie++);
            }

            auto collect = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(43, 1,
                nextCollectCounter++, 0, false, 0, 0, false, nullptr, &doNotKeep, vdiskId,
                TInstant::Max());
            runtime.Send(new IEventHandle(putQueue, edge, collect.release()), NodeIndex);
            auto collectResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge,
                TDuration::Seconds(120));
            UNIT_ASSERT_C(collectResult,
                "no collect result; lastPDiskError# " << lastPDiskErrorReason);
            UNIT_ASSERT_C(collectResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                "collect status# "
                << NKikimrProto::EReplyStatus_Name(collectResult->Get()->Record.GetStatus())
                << " lastPDiskError# " << lastPDiskErrorReason);
        };

        auto requestFootprint = [&] {
            runtime.Send(new IEventHandle(syncLogKeeperId, edge,
                new NSyncLog::TEvSyncLogSnapshot()), NodeIndex);
            auto res = runtime.GrabEdgeEvent<NSyncLog::TEvSyncLogSnapshotResult>(edge,
                TDuration::Seconds(5));
            UNIT_ASSERT(res);
            return GetSyncLogFootprint(res->Get()->SnapshotPtr);
        };

        UNIT_ASSERT_C(WaitVDiskReady(runtime, vdiskActorId, vdiskId, edge),
            "target VDisk did not become ready");
        writeTargetRecords(16);
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return syncLogId && syncLogKeeperId && gotOwner && ownerRound && maxObservedDataLsn;
        }, 200, TDuration::MilliSeconds(10)),
            "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        TSyncLogFootprint previousFootprint = requestFootprint();
        const TSyncLogFootprint initialFootprint = previousFootprint;
        const ui32 initialEntryPointCommits = entryPointCommits;
        const ui32 initialCommitDoneEvents = syncLogCommitDoneEvents;
        const ui32 initialCommittedAppends = committedAppendPages.size();
        const ui32 cutIterations = 200;
        const ui32 recordsPerCut = 1'600;
        const ui32 minPagesPerAppend = 14;
        const ui32 maxPagesPerAppend = 20;
        ui32 minObservedPagesPerAppend = Max<ui32>();
        ui32 maxObservedPagesPerAppend = 0;
        ui32 completedCutLogRequests = 0;
        ui32 cutsThatAddedIndexRecords = 0;
        ui64 lastRequestedFreeUpToLsn = 0;

        // Batch zero is intentionally left in memory. Every loop iteration first writes the next
        // batch and only then asks CutLog to persist the preceding one.
        collectDoNotKeep(recordsPerCut);
        ui64 previousBatchLastLsn = lastObservedGcLsn;
        UNIT_ASSERT_C(previousBatchLastLsn,
            "initial CutLog batch did not advance the target VDisk recovery log");
        const TSyncLogFootprint afterInitialBatch = requestFootprint();
        UNIT_ASSERT_VALUES_EQUAL_C(entryPointCommits, initialEntryPointCommits,
            "initial batch caused a SyncLog commit before TEvCutLog");
        UNIT_ASSERT_VALUES_EQUAL_C(syncLogCommitDoneEvents, initialCommitDoneEvents,
            "initial batch completed a SyncLog commit before TEvCutLog");
        UNIT_ASSERT_VALUES_EQUAL_C(afterInitialBatch.IndexedPages, initialFootprint.IndexedPages,
            "initial batch reached SyncLog disk before TEvCutLog");

        for (ui32 iteration = 0; iteration < cutIterations; ++iteration) {
            const ui64 previousWrittenLsn = lastObservedGcLsn;
            const ui32 commitsBeforeWrite = entryPointCommits;
            const ui32 commitDoneBeforeWrite = syncLogCommitDoneEvents;
            collectDoNotKeep(recordsPerCut);
            const ui64 currentBatchLastLsn = lastObservedGcLsn;
            UNIT_ASSERT_C(currentBatchLastLsn > previousWrittenLsn,
                "small write batch did not advance the target VDisk recovery log"
                << " iteration# " << iteration
                << " previousWrittenLsn# " << previousWrittenLsn
                << " currentBatchLastLsn# " << currentBatchLastLsn);

            // Taking a snapshot queues a round trip through SyncLogKeeper after the write. If a
            // memory-overflow commit was scheduled by the batch, it is visible here, before CutLog.
            const TSyncLogFootprint beforeCutFootprint = requestFootprint();
            UNIT_ASSERT_VALUES_EQUAL_C(entryPointCommits, commitsBeforeWrite,
                "SyncLog entry-point commit was triggered by memory overflow, not TEvCutLog"
                << " iteration# " << iteration
                << " beforeCutFootprint# " << beforeCutFootprint.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(syncLogCommitDoneEvents, commitDoneBeforeWrite,
                "SyncLog commit completed between the batch write and TEvCutLog"
                << " iteration# " << iteration
                << " beforeCutFootprint# " << beforeCutFootprint.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(beforeCutFootprint.IndexedPages, previousFootprint.IndexedPages,
                "batch reached SyncLog disk before TEvCutLog"
                << " iteration# " << iteration
                << " previousFootprint# " << previousFootprint.ToString()
                << " beforeCutFootprint# " << beforeCutFootprint.ToString());

            const ui32 previousEntryPointCommits = entryPointCommits;
            const ui32 previousCommitDoneEvents = syncLogCommitDoneEvents;
            const ui32 previousCommittedAppends = committedAppendPages.size();
            lastRequestedFreeUpToLsn = previousBatchLastLsn + 1;
            runtime.Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, lastRequestedFreeUpToLsn, 0, 0, 0, 0)), NodeIndex);

            // One CutLog may require multiple entry-point commits. Do not write the next portion
            // until the keeper reports that the requested recovery-log position is safe to cut.
            UNIT_ASSERT_C(PumpUntil(runtime, [&] {
                return lastReportedSyncLogFirstLsnToKeep >= lastRequestedFreeUpToLsn;
            }, 1000, TDuration::MilliSeconds(10)),
                "SyncLog CutLog commit did not settle"
                << " iteration# " << iteration
                << " entryPointCommits# " << entryPointCommits
                << " commitDone# " << syncLogCommitDoneEvents
                << " previousBatchLastLsn# " << previousBatchLastLsn
                << " currentBatchLastLsn# " << currentBatchLastLsn
                << " freeUpToLsn# " << lastRequestedFreeUpToLsn
                << " reportedFirstLsnToKeep# " << lastReportedSyncLogFirstLsnToKeep
                << " lastPDiskError# " << lastPDiskErrorReason);
            UNIT_ASSERT_C(entryPointCommits > previousEntryPointCommits &&
                    syncLogCommitDoneEvents > previousCommitDoneEvents,
                "CutLog reached the requested LSN without completing a new SyncLog commit"
                << " iteration# " << iteration
                << " entryPointCommits# " << entryPointCommits
                << " commitDone# " << syncLogCommitDoneEvents);
            ++completedCutLogRequests;

            const TSyncLogFootprint footprint = requestFootprint();
            UNIT_ASSERT_C(footprint.EntryPointIndexRecs == footprint.DiskIndexRecs,
                "entry point does not describe the live SyncLog disk index"
                << " iteration# " << iteration
                << " footprint# " << footprint.ToString());
            UNIT_ASSERT_C(footprint.DiskLastLsn >= previousBatchLastLsn,
                "CutLog commit did not flush the requested SyncLog records to disk"
                << " iteration# " << iteration
                << " previousBatchLastLsn# " << previousBatchLastLsn
                << " freeUpToLsn# " << lastRequestedFreeUpToLsn
                << " footprint# " << footprint.ToString());
            UNIT_ASSERT_C(footprint.DiskLastLsn < currentBatchLastLsn,
                "CutLog did not leave the current batch's mutable tail in memory"
                << " iteration# " << iteration
                << " previousBatchLastLsn# " << previousBatchLastLsn
                << " currentBatchLastLsn# " << currentBatchLastLsn
                << " footprint# " << footprint.ToString());

            const ui32 appendedPages = footprint.IndexedPages - previousFootprint.IndexedPages;
            UNIT_ASSERT_C(minPagesPerAppend <= appendedPages && appendedPages <= maxPagesPerAppend,
                "CutLog append is outside the intended small-append range"
                << " iteration# " << iteration
                << " appendedPages# " << appendedPages
                << " expectedRange# [" << minPagesPerAppend << ", " << maxPagesPerAppend << "]"
                << " previousFootprint# " << previousFootprint.ToString()
                << " footprint# " << footprint.ToString());
            UNIT_ASSERT_C(committedAppendPages.size() > previousCommittedAppends,
                "CutLog commit did not contain a disk append"
                << " iteration# " << iteration
                << " previousCommittedAppends# " << previousCommittedAppends
                << " committedAppends# " << committedAppendPages.size());
            ui32 deltaPages = 0;
            for (ui32 i = previousCommittedAppends; i < committedAppendPages.size(); ++i) {
                deltaPages += committedAppendPages[i];
            }
            UNIT_ASSERT_VALUES_EQUAL_C(deltaPages, appendedPages,
                "TEvSyncLogCommitDone delta differs from disk index page growth"
                << " iteration# " << iteration
                << " footprint# " << footprint.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(footprint.DiskUsedPages, footprint.IndexedPages,
                "closed-batch CutLog workload unexpectedly created physical holes"
                << " iteration# " << iteration
                << " footprint# " << footprint.ToString());
            cutsThatAddedIndexRecords += footprint.DiskIndexRecs > previousFootprint.DiskIndexRecs;
            minObservedPagesPerAppend = Min(minObservedPagesPerAppend, appendedPages);
            maxObservedPagesPerAppend = Max(maxObservedPagesPerAppend, appendedPages);
            previousFootprint = footprint;
            previousBatchLastLsn = currentBatchLastLsn;
        }

        const ui32 indexedPagesGrowth = previousFootprint.IndexedPages - initialFootprint.IndexedPages;
        const ui32 diskIndexGrowth = previousFootprint.DiskIndexRecs - initialFootprint.DiskIndexRecs;
        const ui32 cutDrivenCommits = entryPointCommits - initialEntryPointCommits;
        const ui32 completedCutCommits = syncLogCommitDoneEvents - initialCommitDoneEvents;
        Cerr << "SYNCLOG_CUTLOG_INDEX_FRAGMENTATION"
            << " syncLogMaxMemAmount# " << syncLogMaxMemAmount
            << " cutIterations# " << cutIterations
            << " completedCutLogRequests# " << completedCutLogRequests
            << " cutDrivenCommits# " << cutDrivenCommits
            << " completedCutCommits# " << completedCutCommits
            << " minPagesPerAppend# " << minObservedPagesPerAppend
            << " maxPagesPerAppend# " << maxObservedPagesPerAppend
            << " IndexBulk# " << previousFootprint.IndexBulk
            << " indexedPagesGrowth# " << indexedPagesGrowth
            << " diskIndexGrowth# " << diskIndexGrowth
            << " cutsThatAddedIndexRecords# " << cutsThatAddedIndexRecords
            << " lastFreeUpToLsn# " << lastRequestedFreeUpToLsn
            << " initialFootprint# " << initialFootprint.ToString()
            << " finalFootprint# " << previousFootprint.ToString()
            << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(completedCutLogRequests, cutIterations,
            "not every CutLog request reached its requested FreeUpToLsn"
            << " lastFreeUpToLsn# " << lastRequestedFreeUpToLsn
            << " reportedFirstLsnToKeep# " << lastReportedSyncLogFirstLsnToKeep);
        UNIT_ASSERT_VALUES_EQUAL_C(completedCutCommits, cutDrivenCommits,
            "a SyncLog entry-point commit was still in flight after the last CutLog"
            << " initialEntryPointCommits# " << initialEntryPointCommits
            << " finalEntryPointCommits# " << entryPointCommits
            << " initialCommitDoneEvents# " << initialCommitDoneEvents
                << " finalCommitDoneEvents# " << syncLogCommitDoneEvents);
        UNIT_ASSERT_C(committedAppendPages.size() > initialCommittedAppends,
            "CutLog scenario did not produce any SyncLog disk appends");

        if (requireNoMemoryOverflow) {
            const ui32 maxMemPages = Max<ui32>(2,
                syncLogMaxMemAmount / previousFootprint.AppendBlockSize);
            UNIT_ASSERT_C(previousFootprint.MemPages < maxMemPages,
                "64 MiB scenario unexpectedly reached the SyncLog memory limit"
                << " maxMemPages# " << maxMemPages
                << " footprint# " << previousFootprint.ToString());
        }

        UNIT_ASSERT_C(diskIndexGrowth > 0,
            "the test must allow the SyncLog disk index to grow as data pages are added");
        UNIT_ASSERT_C(cutsThatAddedIndexRecords < cutIterations / 2,
            "SyncLog disk index growth is linear in the number of CutLog commits"
            << " cutIterations# " << cutIterations
            << " cutsThatAddedIndexRecords# " << cutsThatAddedIndexRecords
            << " indexedPagesGrowth# " << indexedPagesGrowth
            << " diskIndexGrowth# " << diskIndexGrowth
            << " IndexBulk# " << previousFootprint.IndexBulk
            << " initialFootprint# " << initialFootprint.ToString()
            << " finalFootprint# " << previousFootprint.ToString());
        const ui32 denseIndexBound = (previousFootprint.IndexedPages + previousFootprint.IndexBulk - 1)
            / previousFootprint.IndexBulk + previousFootprint.DiskChunks;
        UNIT_ASSERT_C(previousFootprint.DiskIndexRecs <= denseIndexBound,
            "SyncLog disk index is larger than the physical-page/IndexBulk bound"
            << " denseIndexBound# " << denseIndexBound
            << " footprint# " << previousFootprint.ToString());
    }

    Y_UNIT_TEST(CutLogAppendsDoNotFragmentDiskIndex) {
        // First prove that CutLog is the source and memory overflow is not involved.
        TestCutLogAppendsDoNotFragmentDiskIndex(64_MB, true,
            "synclog_real_cutlog_index_fragmentation_64mib.dat");

        // Then repeat the same scenario with the production memory limit from the incident.
        TestCutLogAppendsDoNotFragmentDiskIndex(2_MB, false,
            "synclog_real_cutlog_index_fragmentation_2mib.dat");
    }

    Y_UNIT_TEST(MutableTailVersionsAreDeduplicatedAfterRestart) {
        TTestActorRuntime runtime(1, false);
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 512_KB;
        testConfig.SyncLogAdvisedIndexedBlockSize = 1_MB;
        testConfig.SyncLogMaxMemAmount = 64_MB;
        testConfig.SyncLogMaxDiskAmount = 1_GB + 256_MB;
        testConfig.EnablePhantomFlagStorage = true;
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "synclog_real_mutable_tail_versions.dat";

        auto storage = SetupRealPDiskAndRealVDisk(runtime,
            TBlobStorageGroupType::ErasureMirror3of4, testConfig);
        const TVDiskID targetVDisk = storage.Info->GetVDiskId(0);
        const TVDiskID sourceVDisk = storage.Info->GetVDiskId(1);
        const TActorId targetVDiskId = storage.Info->GetActorId(0);
        const TActorId edge = storage.Edge;
        const TActorId putQueue = storage.PutQueue;

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 lastObservedGcLsn = 0;
        ui64 lastReportedSyncLogFirstLsnToKeep = 0;
        ui32 entryPointCommits = 0;
        ui32 syncLogCommitDoneEvents = 0;
        bool phantomFlagBuilderFinished = false;
        TString lastPDiskErrorReason;

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const auto signature = msg.Signature.GetUnmasked();
            if (signature == TLogSignature::SignatureGC) {
                owner = msg.Owner;
                ownerRound = msg.OwnerRound;
                lastObservedGcLsn = Max(lastObservedGcLsn, msg.Lsn);
            } else if (signature == TLogSignature::SignatureSyncLogIdx &&
                    msg.CommitRecord.IsStartingPoint && owner && msg.Owner == owner) {
                ++entryPointCommits;
            }
        };

        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        observePDiskLog(*ev->Get<NPDisk::TEvLog>());
                    }
                    break;

                case TEvBlobStorage::EvMultiLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        for (const auto& item : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            observePDiskLog(*item.Event);
                        }
                    }
                    break;

                case TEvBlobStorage::EvLogResult: {
                    const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                    if (msg->Status != NKikimrProto::OK) {
                        lastPDiskErrorReason = msg->ErrorReason;
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    if (ev->Recipient == syncLogKeeperId) {
                        ++syncLogCommitDoneEvents;
                    }
                    break;

                case TEvBlobStorage::EvPhantomFlagStorageFinishBuilder:
                    if (ev->Recipient == syncLogKeeperId) {
                        phantomFlagBuilderFinished = true;
                    }
                    break;

                case TEvBlobStorage::EvVDiskCutLog: {
                    const auto *msg = ev->Get<TEvVDiskCutLog>();
                    if (ev->Sender == syncLogKeeperId && msg->Component == TEvVDiskCutLog::SyncLog) {
                        lastReportedSyncLogFirstLsnToKeep =
                            Max(lastReportedSyncLogFirstLsnToKeep, msg->LastKeepLsn);
                    }
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto previousEventFilter = runtime.SetEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
                return false;
            });
        TTestRuntimeCallbackGuard runtimeCallbackGuard(runtime,
            std::move(previousObserver), std::move(previousEventFilter));

        UNIT_ASSERT_C(WaitVDiskReady(runtime, targetVDiskId, targetVDisk, edge),
            "target VDisk did not become ready");

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        ui32 nextCollectCounter = 1;
        auto collectDoNotKeep = [&](const TActorId& recipient) {
            TLogoBlobID blobId;
            do {
                blobId = TLogoBlobID(TLogoBlobID(123, 1, nextStep++, 0,
                    1, nextCookie++), 0);
            } while (!storage.Info->BelongsToSubgroup(targetVDisk, blobId.Hash()) ||
                    !storage.Info->BelongsToSubgroup(sourceVDisk, blobId.Hash()));

            for (ui32 attempt = 0; attempt < 100; ++attempt) {
                TVector<TLogoBlobID> doNotKeep{blobId};
                auto collect = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(123, 1,
                    nextCollectCounter, 0, false, 0, 0, false, nullptr, &doNotKeep,
                    targetVDisk, TInstant::Max());
                runtime.Send(new IEventHandle(recipient, edge, collect.release()), NodeIndex);
                auto result = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge,
                    TDuration::Seconds(120));
                UNIT_ASSERT_C(result,
                    "no collect result; blobId# " << blobId
                    << " lastPDiskError# " << lastPDiskErrorReason);
                const auto status = result->Get()->Record.GetStatus();
                if (status == NKikimrProto::NOTREADY || status == NKikimrProto::TRYLATER) {
                    DispatchFor(runtime, TDuration::MilliSeconds(10));
                    continue;
                }
                UNIT_ASSERT_C(status == NKikimrProto::OK,
                    "collect status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " blobId# " << blobId
                    << " lastPDiskError# " << lastPDiskErrorReason);
                ++nextCollectCounter;
                return blobId;
            }
            UNIT_FAIL("collect request did not become ready; blobId# " << blobId);
            return blobId;
        };

        auto requestFootprint = [&] {
            runtime.Send(new IEventHandle(syncLogKeeperId, edge,
                new NSyncLog::TEvSyncLogSnapshot()), NodeIndex);
            auto result = runtime.GrabEdgeEvent<NSyncLog::TEvSyncLogSnapshotResult>(edge,
                TDuration::Seconds(5));
            UNIT_ASSERT(result);
            return GetSyncLogFootprint(result->Get()->SnapshotPtr);
        };

        constexpr ui32 iterations = 12;
        TVector<ui64> expectedLsns;
        TVector<TLogoBlobID> expectedPhantomFlags;
        expectedLsns.reserve(iterations);
        expectedPhantomFlags.reserve(iterations + 1);
        for (ui32 iteration = 0; iteration < iterations; ++iteration) {
            const ui64 previousGcLsn = lastObservedGcLsn;
            expectedPhantomFlags.push_back(collectDoNotKeep(putQueue));
            UNIT_ASSERT_C(lastObservedGcLsn > previousGcLsn,
                "DoNotKeep write did not advance the recovery log"
                << " iteration# " << iteration
                << " previousGcLsn# " << previousGcLsn
                << " lastObservedGcLsn# " << lastObservedGcLsn);
            expectedLsns.push_back(lastObservedGcLsn);

            UNIT_ASSERT_C(syncLogId && syncLogKeeperId && owner && ownerRound,
                "failed to discover SyncLog actors or target PDisk owner");
            const ui32 commitsBeforeCut = entryPointCommits;
            const ui32 commitDoneBeforeCut = syncLogCommitDoneEvents;
            const ui64 freeUpToLsn = lastObservedGcLsn + 1;
            runtime.Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, freeUpToLsn, 0, 0, 0, 0)), NodeIndex);
            UNIT_ASSERT_C(PumpUntil(runtime, [&] {
                return lastReportedSyncLogFirstLsnToKeep >= freeUpToLsn;
            }, 1000, TDuration::MilliSeconds(10)),
                "mutable-tail CutLog commit did not settle"
                << " iteration# " << iteration
                << " freeUpToLsn# " << freeUpToLsn
                << " reportedFirstLsnToKeep# " << lastReportedSyncLogFirstLsnToKeep
                << " lastPDiskError# " << lastPDiskErrorReason);
            UNIT_ASSERT_C(entryPointCommits > commitsBeforeCut &&
                    syncLogCommitDoneEvents > commitDoneBeforeCut,
                "mutable-tail CutLog did not complete a new entry-point commit"
                << " iteration# " << iteration
                << " entryPointCommits# " << entryPointCommits
                << " commitDone# " << syncLogCommitDoneEvents);
        }

        const TSyncLogFootprint beforeRestart = requestFootprint();
        UNIT_ASSERT_VALUES_EQUAL_C(beforeRestart.IndexedPages, 1u,
            "every CutLog rewrite must drop the superseded mutable page version from the index"
            << " footprint# " << beforeRestart.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(beforeRestart.DiskUsedPages, iterations,
            "mutable-page rewrites must advance the physical append position and leave holes"
            << " footprint# " << beforeRestart.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(beforeRestart.DiskIndexRecs, 1u,
            "only the latest mutable page version must stay indexed"
            << " footprint# " << beforeRestart.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(beforeRestart.DiskLastLsn, expectedLsns.back(),
            "last mutable page version is missing from the committed entry point"
            << " footprint# " << beforeRestart.ToString());

        runtime.Send(new IEventHandle(storage.VDiskActors[0], edge,
            new TEvents::TEvPoisonPill), NodeIndex);
        DispatchFor(runtime, TDuration::MilliSeconds(100));
        syncLogId = {};
        syncLogKeeperId = {};
        auto restartedConfig = MakeTestVDiskConfig(storage.AllVDiskKinds, storage.Info,
            storage.PDiskServiceId, 0, 200, testConfig);
        const TActorId restartedVDisk = runtime.Register(CreateVDisk(restartedConfig, storage.Info,
            storage.Counters->GetSubgroup("subsystem", "vdisk")->GetSubgroup("slot", "0")),
            NodeIndex, 0, TMailboxType::Revolving);
        runtime.RegisterService(targetVDiskId, restartedVDisk, NodeIndex);
        storage.VDiskActors[0] = restartedVDisk;
        UNIT_ASSERT_C(WaitVDiskReady(runtime, targetVDiskId, targetVDisk, edge),
            "target VDisk did not recover the SyncLog entry point after restart");

        // RunSyncer=false is intentional for the test topology, so it does not deliver
        // TEvSyncLogDbBirthLsn. A post-restart write discovers the new SyncLog actor and also
        // becomes part of the expected VSync stream; initialize DbBirthLsn explicitly afterwards.
        const ui64 preRestartLastLsn = expectedLsns.back();
        expectedPhantomFlags.push_back(collectDoNotKeep(targetVDiskId));
        UNIT_ASSERT_C(lastObservedGcLsn > preRestartLastLsn,
            "post-restart probe write did not advance the recovery log");
        expectedLsns.push_back(lastObservedGcLsn);
        UNIT_ASSERT_C(syncLogId && syncLogKeeperId,
            "failed to discover restarted SyncLog actors");
        runtime.Send(new IEventHandle(syncLogId, edge,
            new NSyncLog::TEvSyncLogDbBirthLsn(0)), NodeIndex);
        DispatchFor(runtime, TDuration::MilliSeconds(10));

        TVector<ui64> actualLsns;
        TSyncState syncState;
        ui32 diskReads = 0;
        bool finished = false;
        for (ui32 request = 0; request < 100 && !finished; ++request) {
            runtime.Send(new IEventHandle(targetVDiskId, edge,
                new TEvBlobStorage::TEvVSync(syncState, sourceVDisk, targetVDisk)), NodeIndex);
            auto result = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVSyncResult>(edge,
                TDuration::Seconds(120));
            UNIT_ASSERT_C(result, "no VSync result after VDisk restart");
            const auto& record = result->Get()->Record;
            if (record.GetStatus() == NKikimrProto::RESTART) {
                syncState = SyncStateFromSyncState(record.GetNewSyncState());
                continue;
            }

            UNIT_ASSERT_C(record.GetStatus() == NKikimrProto::OK,
                "unexpected VSync status after restart# "
                << NKikimrProto::EReplyStatus_Name(record.GetStatus())
                << " syncState# " << syncState.ToString());
            if (record.HasData() && !record.GetData().empty()) {
                NSyncLog::TFragmentReader reader(record.GetData());
                TString error;
                UNIT_ASSERT_C(reader.Check(error), "invalid VSync fragment# " << error);
                for (const NSyncLog::TRecordHdr* hdr : reader.ListRecords()) {
                    actualLsns.push_back(hdr->Lsn);
                }
            }
            if (record.HasStat()) {
                diskReads += record.GetStat().GetDiskReads();
            }
            syncState = SyncStateFromSyncState(record.GetNewSyncState());
            finished = record.GetFinished();
        }

        UNIT_ASSERT_C(finished, "VSync did not finish after VDisk restart");
        UNIT_ASSERT_C(diskReads > 0,
            "VSync did not read the recovered mutable-tail versions from SyncLog disk chunks");
        UNIT_ASSERT_VALUES_EQUAL(actualLsns, expectedLsns);
        for (ui32 i = 1; i < actualLsns.size(); ++i) {
            UNIT_ASSERT_C(actualLsns[i - 1] < actualLsns[i],
                "VSync returned duplicate or unordered LSNs"
                << " previous# " << actualLsns[i - 1]
                << " current# " << actualLsns[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(syncState.SyncedLsn, expectedLsns.back());

        // Force the volatile Phantom Flag Storage to build from the SyncLog snapshot. BaldLog
        // removes the disk chunk from the live index, while the builder retains and reads the old
        // snapshot with the latest version of the mutable page.
        runtime.Send(new IEventHandle(targetVDiskId, edge,
            new TEvBlobStorage::TEvVBaldSyncLog(targetVDisk, true)), NodeIndex);
        auto baldResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVBaldSyncLogResult>(edge,
            TDuration::Seconds(120));
        UNIT_ASSERT_C(baldResult, "no BaldSyncLog result");
        UNIT_ASSERT_VALUES_EQUAL(baldResult->Get()->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return phantomFlagBuilderFinished;
        }, 1000, TDuration::MilliSeconds(10)),
            "Phantom Flag Storage builder did not finish reading the SyncLog snapshot");

        runtime.Send(new IEventHandle(syncLogKeeperId, edge,
            new NSyncLog::TEvPhantomFlagStorageGetSnapshot()), NodeIndex);
        auto phantomSnapshot = runtime.GrabEdgeEvent<NSyncLog::TEvPhantomFlagStorageGetSnapshotResult>(edge,
            TDuration::Seconds(120));
        UNIT_ASSERT_C(phantomSnapshot, "no Phantom Flag Storage snapshot result");
        UNIT_ASSERT_C(phantomSnapshot->Get()->Eof,
            "volatile Phantom Flag Storage snapshot must fit in one response");

        TVector<TLogoBlobID> actualPhantomFlags;
        for (const auto& flag : phantomSnapshot->Get()->Flags) {
            actualPhantomFlags.push_back(flag.LogoBlobID());
        }
        Sort(expectedPhantomFlags);
        Sort(actualPhantomFlags);
        UNIT_ASSERT_VALUES_EQUAL(actualPhantomFlags, expectedPhantomFlags);
    }

    Y_UNIT_TEST(SyncLogMemOverflowSwapsDoNotFragmentDiskIndex) {
        /*
         * Reproduces a production incident: a group loses one VDisk (node is down), so SyncLog gets
         * no sync confirmations and is never trimmed; with a small SyncLogMaxMemAmount and a large
         * SyncLogMaxDiskAmount the keeper hits memory overflow on nearly every put batch and swaps
         * only the overflow excess -- typically a single page -- to the disk sync log. Every such
         * tiny append adds a separate TDiskIndexRecord (TOneChunkIndex never merges neighboring
         * appends; IndexBulk only splits large ones), and the whole index is re-serialized into the
         * SyncLog entry point and written into the PDisk common log on every commit. In production
         * this produced entry points of several megabytes (IndexRecsNum ~ number of disk pages)
         * rewritten several times per second per VDisk, which exhausted PDisk log chunks.
         *
         * The test drives many small writes through a real VDisk while no group neighbor ever
         * confirms sync (RunSyncer is false for every group member, which models the dead node),
         * lets memory-overflow-driven commits move data to disk, and then requires the disk index
         * to stay dense: on average an append must cover at least minAvgPagesPerAppend pages.
         * Before BuildSwapSnap started swapping down to a low watermark instead of swapping only
         * the excess, this produced one index record per page, matching the production pattern.
         */
        TTestActorRuntime runtime(1, false);
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 512_KB;
        testConfig.SyncLogAdvisedIndexedBlockSize = 1_MB;
        testConfig.SyncLogMaxMemAmount = 256_KB;
        testConfig.SyncLogMaxDiskAmount = 8 * 512_KB;
        testConfig.MaxLogoBlobDataSize = 192_KB;
        testConfig.MinHugeBlobInBytes = 64_KB;
        testConfig.MilestoneHugeBlobInBytes = 128_KB;
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "synclog_real_index_fragmentation.dat";

        auto storage = SetupRealPDiskAndRealVDisk(runtime, TBlobStorageGroupType::ErasureMirror3of4, testConfig);
        const auto& info = storage.Info;
        const TActorId edge = storage.Edge;
        const TActorId putQueue = storage.PutQueue;
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        ui64 maxObservedDataLsn = 0;
        ui32 entryPointCommits = 0;
        ui64 entryPointBytesMax = 0;
        ui32 syncLogCommitDoneEvents = 0;
        TString lastPDiskErrorReason;

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const ui32 signature = msg.Signature.GetUnmasked();
            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;

            if (!syncLogEntryPointCommit) {
                if (!gotOwner) {
                    owner = msg.Owner;
                    gotOwner = true;
                } else if (msg.Owner != owner) {
                    return;
                }
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
                return;
            }

            if (!gotOwner || msg.Owner != owner) {
                return;
            }
            ++entryPointCommits;
            entryPointBytesMax = Max<ui64>(entryPointBytesMax, msg.Data.size());
        };

        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        observePDiskLog(*ev->Get<NPDisk::TEvLog>());
                    }
                    break;

                case TEvBlobStorage::EvMultiLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        for (const auto& item : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            observePDiskLog(*item.Event);
                        }
                    }
                    break;

                case TEvBlobStorage::EvLogResult: {
                    const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                    if (msg->Status != NKikimrProto::OK) {
                        lastPDiskErrorReason = msg->ErrorReason;
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto previousEventFilter = runtime.SetEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
                return false;
            });
        TTestRuntimeCallbackGuard runtimeCallbackGuard(runtime,
            std::move(previousObserver), std::move(previousEventFilter));

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);

        auto writeTargetRecords = [&](ui64 tabletId, ui32 records) {
            const ui32 batchSize = 64;
            for (ui32 firstRecord = 0; firstRecord < records; firstRecord += batchSize) {
                const ui32 batch = Min(batchSize, records - firstRecord);
                auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, false);

                for (ui32 i = 0; i < batch; ++i) {
                    const TLogoBlobID blobId(TLogoBlobID(tabletId, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                    multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
                }

                runtime.Send(new IEventHandle(putQueue, edge, multiPut.release()), NodeIndex);
                auto putResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVMultiPutResult>(edge,
                    TDuration::Seconds(120));
                UNIT_ASSERT_C(putResult, "no put result; firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                UNIT_ASSERT_C(putResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "put status# " << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetStatus())
                    << " firstRecord# " << firstRecord
                    << " lastPDiskError# " << lastPDiskErrorReason);
                DispatchFor(runtime, TDuration::MilliSeconds(1));
            }
        };

        ui32 nextCollectCounter = 1;
        auto collectDoNotKeep = [&](ui64 tabletId, ui32 records) {
            TVector<TLogoBlobID> doNotKeep;
            doNotKeep.reserve(records);
            for (ui32 i = 0; i < records; ++i) {
                doNotKeep.emplace_back(tabletId, 1, nextStep++, 0, 1, nextCookie++);
            }

            auto collect = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(tabletId, 1,
                nextCollectCounter++, 0, false, 0, 0, false, nullptr, &doNotKeep, vdiskId,
                TInstant::Max());
            runtime.Send(new IEventHandle(putQueue, edge, collect.release()), NodeIndex);
            auto collectResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge,
                TDuration::Seconds(120));
            UNIT_ASSERT_C(collectResult, "no collect result; lastPDiskError# " << lastPDiskErrorReason);
            UNIT_ASSERT_C(collectResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                "collect status# " << NKikimrProto::EReplyStatus_Name(collectResult->Get()->Record.GetStatus())
                << " lastPDiskError# " << lastPDiskErrorReason);
            DispatchFor(runtime, TDuration::MilliSeconds(1));
        };

        auto requestFootprint = [&] {
            runtime.Send(new IEventHandle(syncLogKeeperId, edge,
                new NSyncLog::TEvSyncLogSnapshot()), NodeIndex);
            auto res = runtime.GrabEdgeEvent<NSyncLog::TEvSyncLogSnapshotResult>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(res);
            return GetSyncLogFootprint(res->Get()->SnapshotPtr);
        };

        UNIT_ASSERT_C(WaitVDiskReady(runtime, vdiskActorId, vdiskId, edge), "target VDisk did not become ready");

        writeTargetRecords(42, 16);
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, 200, TDuration::MilliSeconds(10)),
            "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        // sanity check of the geometry: the invariant below is only meaningful when memory
        // holds substantially more pages than the required average append size
        TSyncLogFootprint footprint = requestFootprint();
        UNIT_ASSERT(footprint.AppendBlockSize);
        const ui32 maxMemPages = Max<ui32>(2, testConfig.SyncLogMaxMemAmount / footprint.AppendBlockSize);
        UNIT_ASSERT_C(maxMemPages >= 16,
            "test geometry is degenerate, SyncLog memory holds too few pages: " << maxMemPages);

        // Never send NPDisk::TEvCutLog here: all commits must be driven by memory overflow,
        // exactly like on a loaded VDisk whose group neighbor is down.
        const ui32 targetIndexedPages = 128;
        const ui32 recordsPerBatch = 128;
        for (ui32 i = 0; i < 1500 && footprint.IndexedPages < targetIndexedPages; ++i) {
            collectDoNotKeep(43, recordsPerBatch);
            if ((i + 1) % 8 == 0) {
                footprint = requestFootprint();
            }
        }

        // let the last commit settle and take the final snapshot
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return entryPointCommits && syncLogCommitDoneEvents >= entryPointCommits;
        }, 300, TDuration::MilliSeconds(10)),
            "SyncLog commit did not settle; entryPointCommits# " << entryPointCommits
            << " commitDone# " << syncLogCommitDoneEvents
            << " lastPDiskError# " << lastPDiskErrorReason);
        footprint = requestFootprint();

        UNIT_ASSERT_C(footprint.IndexedPages >= targetIndexedPages,
            "test did not push enough SyncLog data to disk"
            << " footprint# " << footprint.ToString()
            << " lastPDiskError# " << lastPDiskErrorReason);
        UNIT_ASSERT(entryPointCommits > 0 && footprint.DiskIndexRecs > 0);

        // The regression itself: overflow-driven swaps must be batched, otherwise the disk index
        // degenerates into one record per page and the entry point (the full index re-serialized
        // into the PDisk common log on every commit) grows unbounded together with commit rate.
        const ui32 minAvgPagesPerAppend = 4;
        UNIT_ASSERT_C(footprint.IndexedPages >= footprint.DiskIndexRecs * minAvgPagesPerAppend,
            "SyncLog disk index is fragmented by tiny memory-overflow swaps"
            << " avgPagesPerAppend# " << (footprint.IndexedPages / footprint.DiskIndexRecs)
            << " footprint# " << footprint.ToString()
            << " entryPointCommits# " << entryPointCommits
            << " entryPointBytesMax# " << entryPointBytesMax);
    }

    Y_UNIT_TEST(SyncLogDuplicateFreeChunkRealPDiskWithoutRestart) {
        /*
         * Same live race as SyncLogDiskOverflowOldSnapshotCreatesDuplicateFreeChunkWithoutRestart
         * in ut_blobstorage/sync.cpp, but with a real TPDiskActor instead of PDiskMock.
         *
         * The test does not inject a bad entry point and does not restart the VDisk. It drives
         * ordinary VDisk writes until SyncLog disk overflow removes an old last chunk. The old
         * bug let the committer append through the snapshot taken before overflow and produced two
         * distinct TEvSyncLogFreeChunk notifications for one chunkIdx; with real PDisk this could
         * then report BPD77. The regression expectation is that neither duplicate free nor BPD77
         * happens.
        */
        TTestActorRuntime runtime(1, false);
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 512_KB;
        testConfig.SyncLogMaxMemAmount = 8_MB;
        testConfig.SyncLogMaxDiskAmount = 512_KB;
        testConfig.MaxLogoBlobDataSize = 192_KB;
        testConfig.MinHugeBlobInBytes = 64_KB;
        testConfig.MilestoneHugeBlobInBytes = 128_KB;
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "synclog_real_live_bpd77.dat";
        auto storage = SetupRealPDiskAndRealVDisk(runtime, TBlobStorageGroupType::ErasureMirror3of4, testConfig);
        const auto& info = storage.Info;
        const TActorId edge = storage.Edge;
        const TActorId putQueue = storage.PutQueue;
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 maxObservedDataLsn = 0;
        ui32 syncLogCommitDoneEvents = 0;
        ui32 freeChunkNotifications = 0;
        ui32 stopWritesAtFreeChunkNotification = Max<ui32>();
        bool observedSyncLogDataCommit = false;
        bool observedAppendToDeletedChunk = false;
        bool observedDuplicateFreeChunk = false;
        bool observedBpd77 = false;
        bool postponeNextSyncLogDataCommit = false;
        bool dataCommitPostponed = false;
        bool reorderRaceFreeChunks = false;
        ui64 postponedDataCommitLsn = 0;
        TVector<TChunkIdx> postponedDataCommitChunks;
        TMaybe<TChunkIdx> raceChunkToFreeFirst;
        TActorId postponedDataCommitSender;
        TAutoPtr<IEventHandle> postponedDataCommit;
        std::deque<TAutoPtr<IEventHandle>> postponedFreeChunkEvents;
        TString lastPDiskErrorReason;
        TString bpd77ErrorReason;
        THashMap<TChunkIdx, ui32> freeChunkNotificationCounts;
        TVector<TString> appendToDeletedChunkDetails;
        TVector<TString> duplicateFreeChunkDetails;
        TVector<TString> syncLogDeleteDetails;
        TVector<TString> syncLogCommitDetails;

        auto formatFreeChunkNotifications = [&] {
            TVector<TString> items;
            for (const auto& [chunkIdx, count] : freeChunkNotificationCounts) {
                items.push_back(TStringBuilder()
                    << "{chunkIdx# " << chunkIdx
                    << " count# " << count
                    << "}");
            }
            return FormatList(items);
        };

        auto markAppendToDeletedChunks = [&](const NPDisk::TEvLog& msg) {
            const bool syncLogEntryPointCommit = msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;
            if (!syncLogEntryPointCommit || !msg.CommitRecord.CommitChunks || !msg.CommitRecord.DeleteChunks) {
                return;
            }

            THashSet<TChunkIdx> deletedChunks(msg.CommitRecord.DeleteChunks.begin(), msg.CommitRecord.DeleteChunks.end());
            for (const TChunkIdx chunkIdx : msg.CommitRecord.CommitChunks) {
                if (deletedChunks.contains(chunkIdx)) {
                    observedAppendToDeletedChunk = true;
                    appendToDeletedChunkDetails.push_back(TStringBuilder()
                        << "{lsn# " << msg.Lsn
                        << " chunkIdx# " << chunkIdx
                        << " commitChunks# " << FormatList(msg.CommitRecord.CommitChunks)
                        << " deleteChunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                        << "}");
                }
            }
        };

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const ui32 signature = msg.Signature.GetUnmasked();
            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;

            if (!syncLogEntryPointCommit) {
                if (!gotOwner) {
                    owner = msg.Owner;
                    gotOwner = true;
                } else if (msg.Owner != owner) {
                    return;
                }
                ownerRound = msg.OwnerRound;
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
            } else if (!gotOwner || msg.Owner != owner) {
                return;
            }

            if (!syncLogEntryPointCommit) {
                return;
            }

            if (msg.CommitRecord.CommitChunks) {
                observedSyncLogDataCommit = true;
                syncLogCommitDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " chunks# " << FormatList(msg.CommitRecord.CommitChunks)
                    << "}");
            }

            if (msg.CommitRecord.DeleteChunks) {
                syncLogDeleteDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " chunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                    << " deleteToDecommitted# " << msg.CommitRecord.DeleteToDecommitted
                    << "}");
            }

            markAppendToDeletedChunks(msg);
        };

        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (gotOwner) {
                        if (!syncLogId) {
                            syncLogId = ev->Recipient;
                        } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                            syncLogKeeperId = ev->Recipient;
                        }
                    }
                    break;

                case TEvBlobStorage::EvLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        observePDiskLog(*ev->Get<NPDisk::TEvLog>());
                    }
                    break;

                case TEvBlobStorage::EvMultiLog:
                    if (ev->Recipient == storage.PDiskServiceId) {
                        for (const auto& item : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            observePDiskLog(*item.Event);
                        }
                    }
                    break;

                case TEvBlobStorage::EvLogResult: {
                    const auto *msg = ev->Get<NPDisk::TEvLogResult>();
                    if (msg->Status == NKikimrProto::ERROR) {
                        lastPDiskErrorReason = msg->ErrorReason;
                        if (msg->ErrorReason.Contains("BPD77")) {
                            observedBpd77 = true;
                            bpd77ErrorReason = msg->ErrorReason;
                        }
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;

                case TEvBlobStorage::EvSyncLogFreeChunk: {
                    const auto *msg = ev->Get<NSyncLog::TEvSyncLogFreeChunk>();
                    ui32& notificationCount = freeChunkNotificationCounts[msg->ChunkIdx];
                    ++notificationCount;
                    ++freeChunkNotifications;
                    if (notificationCount > 1) {
                        observedDuplicateFreeChunk = true;
                        duplicateFreeChunkDetails.push_back(TStringBuilder()
                            << "{chunkIdx# " << msg->ChunkIdx
                            << " count# " << notificationCount
                            << "}");
                    }
                    break;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto previousEventFilter = runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (!ev) {
                return false;
            }

            if (reorderRaceFreeChunks && ev->GetTypeRewrite() == TEvBlobStorage::EvSyncLogFreeChunk &&
                    raceChunkToFreeFirst) {
                const auto *msg = ev->Get<NSyncLog::TEvSyncLogFreeChunk>();
                if (msg->ChunkIdx != *raceChunkToFreeFirst) {
                    Cerr << "SYNCLOG_REAL_PDISK_LIVE_BPD77 postpone_free_chunk"
                        << " chunkIdx# " << msg->ChunkIdx
                        << " preferredFirst# " << *raceChunkToFreeFirst
                        << Endl;
                    postponedFreeChunkEvents.emplace_back(ev.Release());
                    return true;
                }

                Cerr << "SYNCLOG_REAL_PDISK_LIVE_BPD77 allow_preferred_free_chunk_first"
                    << " chunkIdx# " << msg->ChunkIdx
                    << Endl;
                reorderRaceFreeChunks = false;
            }

            if (ev->Recipient != storage.PDiskServiceId || ev->GetTypeRewrite() != TEvBlobStorage::EvLog) {
                return false;
            }

            const auto *msg = ev->Get<NPDisk::TEvLog>();
            if (!gotOwner || msg->Owner != owner ||
                    msg->Signature.GetUnmasked() != TLogSignature::SignatureSyncLogIdx ||
                    !msg->CommitRecord.IsStartingPoint ||
                    !msg->CommitRecord.CommitChunks ||
                    !msg->CommitRecord.DeleteChunks) {
                return false;
            }

            if (!postponeNextSyncLogDataCommit) {
                return false;
            }

            postponeNextSyncLogDataCommit = false;
            dataCommitPostponed = true;
            postponedDataCommitLsn = msg->Lsn;
            postponedDataCommitChunks = msg->CommitRecord.CommitChunks;
            markAppendToDeletedChunks(*msg);
            if (postponedDataCommitChunks) {
                raceChunkToFreeFirst = postponedDataCommitChunks.back();
                reorderRaceFreeChunks = true;
            }
            postponedDataCommitSender = ev->Sender;
            postponedDataCommit.Reset(ev.Release());

            Cerr << "SYNCLOG_REAL_PDISK_LIVE_BPD77 postponed_data_commit"
                << " lsn# " << postponedDataCommitLsn
                << " chunks# " << FormatList(postponedDataCommitChunks)
                << " deletes# " << FormatList(msg->CommitRecord.DeleteChunks)
                << " sender# " << postponedDataCommitSender
                << Endl;

            return true;
        });
        TTestRuntimeCallbackGuard runtimeCallbackGuard(runtime,
            std::move(previousObserver), std::move(previousEventFilter));

        auto releasePostponedFreeChunkEvents = [&] {
            while (!postponedFreeChunkEvents.empty()) {
                TAutoPtr<IEventHandle> ev(postponedFreeChunkEvents.front().Release());
                postponedFreeChunkEvents.pop_front();
                runtime.Send(ev, NodeIndex);
            }
        };

        UNIT_ASSERT_C(WaitVDiskReady(runtime, vdiskActorId, vdiskId, edge), "target VDisk did not become ready");

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);

        auto writeTargetRecords = [&](ui64 tabletId, ui32 records, const TString& phase) {
            const ui32 batchSize = 4'096;
            for (ui32 firstRecord = 0; firstRecord < records &&
                    !observedBpd77 && !observedAppendToDeletedChunk && !observedDuplicateFreeChunk &&
                    freeChunkNotifications < stopWritesAtFreeChunkNotification; firstRecord += batchSize) {
                const ui32 batch = Min(batchSize, records - firstRecord);
                auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, false);

                for (ui32 i = 0; i < batch; ++i) {
                    const TLogoBlobID blobId(TLogoBlobID(tabletId, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                    multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
                }

                runtime.Send(new IEventHandle(putQueue, edge, multiPut.release()), NodeIndex);
                TEvBlobStorage::TEvVMultiPutResult::TPtr putResult;
                try {
                    putResult = runtime.GrabEdgeEvent<TEvBlobStorage::TEvVMultiPutResult>(edge,
                        TDuration::Seconds(120));
                } catch (const TEmptyEventQueueException&) {
                    UNIT_ASSERT_C(observedBpd77,
                        "timed out waiting for TEvVMultiPutResult during " << phase
                        << " at firstRecord# " << firstRecord
                        << " lastPDiskError# " << lastPDiskErrorReason);
                    return;
                }
                if (putResult->Get()->Record.GetStatus() != NKikimrProto::OK && observedBpd77) {
                    return;
                }
                UNIT_ASSERT_C(putResult->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "TEvVMultiPut failed during " << phase << " at firstRecord# " << firstRecord
                    << " status# "
                    << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetStatus())
                    << " lastPDiskError# " << lastPDiskErrorReason);
                UNIT_ASSERT_VALUES_EQUAL(putResult->Get()->Record.ItemsSize(), batch);
                for (ui32 i = 0; i < batch; ++i) {
                    if (putResult->Get()->Record.GetItems(i).GetStatus() != NKikimrProto::OK && observedBpd77) {
                        return;
                    }
                    UNIT_ASSERT_C(putResult->Get()->Record.GetItems(i).GetStatus() == NKikimrProto::OK,
                        "TEvVMultiPut item failed during " << phase
                        << " firstRecord# " << firstRecord
                        << " item# " << i
                        << " status# "
                        << NKikimrProto::EReplyStatus_Name(putResult->Get()->Record.GetItems(i).GetStatus())
                        << " lastPDiskError# " << lastPDiskErrorReason);
                }
                DispatchFor(runtime, TDuration::MilliSeconds(1));
            }
        };

        auto requestSyncLogCut = [&] {
            runtime.Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), NodeIndex);
            DispatchFor(runtime, TDuration::MilliSeconds(1));
        };

        writeTargetRecords(42, 50'000, "initial real-pdisk sync-log fill");

        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, 300, TDuration::MilliSeconds(10)),
            "failed to discover SyncLog, SyncLogKeeper, PDisk owner, or data LSN");

        requestSyncLogCut();
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return observedSyncLogDataCommit && syncLogCommitDoneEvents;
        }, 3000, TDuration::MilliSeconds(10)),
            "initial writes did not move SyncLog data to real PDisk chunks");

        const ui32 commitDoneBeforePostponedDataCommit = syncLogCommitDoneEvents;
        writeTargetRecords(42, 55'000, "pre-race real-pdisk sync-log fill");
        postponeNextSyncLogDataCommit = true;
        requestSyncLogCut();
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return dataCommitPostponed;
        }, 3000, TDuration::MilliSeconds(10)),
            "failed to postpone a natural SyncLog data commit before the large swap; "
            "commits# " << FormatList(syncLogCommitDetails)
            << " deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(postponedDataCommit, "data commit was marked as postponed but no event was saved");
        {
            TAutoPtr<IEventHandle> ev(postponedDataCommit.Release());
            runtime.Send(ev, NodeIndex);
        }
        UNIT_ASSERT_C(PumpUntil(runtime, [&] {
            return syncLogCommitDoneEvents > commitDoneBeforePostponedDataCommit ||
                observedAppendToDeletedChunk || observedBpd77;
        }, 3000, TDuration::MilliSeconds(10)),
            "postponed SyncLog data commit did not complete; lsn# " << postponedDataCommitLsn
            << " chunks# " << FormatList(postponedDataCommitChunks));

        releasePostponedFreeChunkEvents();

        UNIT_ASSERT_C(!observedAppendToDeletedChunk,
            "real SyncLog committer appended swap data into a chunk deleted by the same entrypoint commit: "
            << FormatList(appendToDeletedChunkDetails)
            << " commits# " << FormatList(syncLogCommitDetails)
            << " deletes# " << FormatList(syncLogDeleteDetails)
            << " lastPDiskError# " << lastPDiskErrorReason);

        UNIT_ASSERT_C(!observedDuplicateFreeChunk,
            "real VDisk reproduced duplicate SyncLog TOneChunk free; freeNotifications# "
            << formatFreeChunkNotifications()
            << " commits# " << FormatList(syncLogCommitDetails)
            << " deletes# " << FormatList(syncLogDeleteDetails)
            << " lastPDiskError# " << lastPDiskErrorReason);

        UNIT_ASSERT_C(!observedBpd77,
            "real PDisk reported BPD77 after duplicate SyncLog free race; "
            "duplicateFree# " << FormatList(duplicateFreeChunkDetails)
            << " commits# " << FormatList(syncLogCommitDetails)
            << " deletes# " << FormatList(syncLogDeleteDetails)
            << " lastPDiskError# " << lastPDiskErrorReason);

    }

    Y_UNIT_TEST(LocalSyncDataUnsuccessfulFullRecoveryDoesNotExhaustLogChunks) {
        /*
         * Reproduces the VSyncFull retry window:
         *
         * 1. VDisk writes data received from full sync as SignatureLocalSyncData.
         * 2. The node dies after PDisk has committed this record, but before VDisk observes the
         *    log result and before the final SyncerState/cut becomes durable.
         * 3. On restart the sync state is still old, but local recovery has already replayed the
         *    durable local sync data; startup data sync must not start fetching more remote data
         *    until recovery log cut durably passes the replayed record.
         *
         * The first TEvLocalSyncData is injected directly into Skeleton; this exercises the same
         * recovery-log writer, PDisk common log, and local-recovery replay path without building
         * a multi-vdisk sync topology. If the feature flag below is disabled by hand, the same
         * loop keeps repeating this interrupted write and must fail on common-log OUT_OF_SPACE.
         */
        const ui32 targetPayloadBytes = 3_MB;
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 64_KB;
        testConfig.DiskSize = 128_MB;
        testConfig.MaxCommonLogChunks = 5;
        // LIGHT_ORANGE or better means that at least four of the five common-log
        // chunks are free, leaving enough headroom for the next LocalSyncData
        // record to allocate up to two log chunks.
        constexpr auto maxAcceptableLogColor =
            NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE;
        testConfig.EnableSmallDiskOptimization = false;
        testConfig.RunSyncer = true;
        testConfig.GroupId = TGroupID(EGroupConfigurationType::Dynamic, DomainId, 2).GetRaw();
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "local_sync_data_cut_wait.dat";

        TLocalSyncDataReplayEnv env(targetPayloadBytes, testConfig);
        UNIT_ASSERT_C(env.LocalSyncData.size() >= targetPayloadBytes,
            "LocalSyncData payload is too small; expected at least# " << targetPayloadBytes
            << " actual# " << env.LocalSyncData.size());
        UNIT_ASSERT_C(env.SkeletonId, "failed to discover initial Skeleton actor");
        UNIT_ASSERT_C(env.WaitReady(), "target VDisk did not become ready before LocalSyncData injection");
        // Persist LocalSyncData in PDisk, then drop the successful log result to emulate a crash
        // after durable commit but before VDisk advances sync state or recovery-log cut.
        env.SendInterruptedLocalSyncData();
        UNIT_ASSERT_C(!env.ObservedOutOfSpace(),
            "initial LocalSyncData write exhausted space"
            << " details# " << env.Details);
        UNIT_ASSERT_C(env.InterruptedLsn, "test did not capture interrupted LocalSyncData LSN");

        // After restart, local recovery replays the durable LocalSyncData. Startup sync must wait
        // until the recovery log is durably cut past that replayed record.
        env.RestartVDisk();
        UNIT_ASSERT_C(env.WaitReady(),
            "target VDisk did not become ready after interrupted LocalSyncData"
            << " interruptedLsn# " << env.InterruptedLsn);
        env.WaitForCut();
        env.WaitForCommonLogChunkReclaim(maxAcceptableLogColor);

        // Repeat the same interrupted recovery window. Without the cut wait, these duplicate
        // LocalSyncData writes eventually exhaust common log chunks.
        for (ui32 i = 0; i < 8 && !env.ObservedOutOfSpace(); ++i) {
            env.SendInterruptedLocalSyncData();
            if (!env.ObservedOutOfSpace()) {
                env.RestartVDisk();
                UNIT_ASSERT_C(env.WaitReady(),
                    "target VDisk did not become ready after duplicate LocalSyncData"
                    << " iteration# " << i
                    << " interruptedWrites# " << env.InterruptedWrites);
                env.WaitForCut();
                env.WaitForCommonLogChunkReclaim(maxAcceptableLogColor);
            }
        }
        UNIT_ASSERT_C(!env.LocalSyncDataOutOfSpace && !env.OtherOutOfSpace,
            "replayed LocalSyncData exhausted space"
            << " localSyncDataOutOfSpace# " << env.LocalSyncDataOutOfSpace
            << " otherOutOfSpace# " << env.OtherOutOfSpace
            << " interruptedWrites# " << env.InterruptedWrites
            << " maxLogChunkCount# " << env.MaxLogChunkCount
            << " details# " << env.Details);
        UNIT_ASSERT_C(env.CutRequested,
            "startup data sync did not request recovery log cut"
            << " interruptedLsn# " << env.InterruptedLsn);
        UNIT_ASSERT_C(env.CutReachedInterruptedLsn,
            "recovery log cut did not pass replayed LocalSyncData"
            << " interruptedLsn# " << env.InterruptedLsn
            << " lastCutFirstLsnToKeep# " << env.LastCutFirstLsnToKeep);
        UNIT_ASSERT_C(!env.StartupTokenBeforeCut,
            "startup data sync token was requested before durable recovery log cut"
            << " interruptedLsn# " << env.InterruptedLsn
            << " lastCutFirstLsnToKeep# " << env.LastCutFirstLsnToKeep);
    }

    Y_UNIT_TEST(LocalSyncDataCutWaitRetriesAfterInsufficientCut) {
        /*
         * TEvAskForCutLog asks PDisk for an owner cut, not for a specific target LSN. If the
         * first durable cut is still below the replayed LocalSyncData record, startup data sync
         * must ask again instead of getting stuck. The exact PDisk chunk layout may produce an
         * already sufficient first cut, so this test replaces the first cut notification with an
         * insufficient one and checks the syncer retry behavior.
         */
        const ui32 targetPayloadBytes = 128_KB;
        TRealPDiskTestConfig testConfig;
        testConfig.ChunkSize = 64_KB;
        testConfig.DiskSize = 128_MB;
        testConfig.MaxCommonLogChunks = 10;
        testConfig.EnableSmallDiskOptimization = false;
        testConfig.RunSyncer = true;
        testConfig.GroupId = TGroupID(EGroupConfigurationType::Dynamic, DomainId, 3).GetRaw();
        testConfig.AdvanceEntryPointTimeout = TDuration::Hours(1);
        testConfig.RecoveryLogCutterFirstDuration = TDuration::Hours(1);
        testConfig.RecoveryLogCutterRegularDuration = TDuration::Hours(1);
        testConfig.PDiskPathSuffix = "local_sync_data_cut_retry.dat";

        TLocalSyncDataReplayEnv env(targetPayloadBytes, testConfig);
        ui64 firstCutFirstLsnToKeep = 0;
        bool observedInsufficientCut = false;
        bool insufficientCutInjected = false;
        env.PreFilter = [&](TAutoPtr<IEventHandle>& ev) {
            // Force the first cut notification to stop just before the replayed LocalSyncData.
            // This makes the test deterministic even when the real PDisk layout would produce a
            // sufficient first cut.
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvRecoveryLogCutDone &&
                    env.ObserveAfterRestart && env.InterruptedLsn && !insufficientCutInjected) {
                const ui64 insufficientFirstLsnToKeep = env.InterruptedLsn;
                insufficientCutInjected = true;
                observedInsufficientCut = true;
                firstCutFirstLsnToKeep = insufficientFirstLsnToKeep;
                env.LastCutFirstLsnToKeep = insufficientFirstLsnToKeep;
                ev.Reset(new IEventHandle(ev->Recipient, ev->Sender,
                    new TEvRecoveryLogCutDone(insufficientFirstLsnToKeep),
                    ev->Flags, ev->Cookie));
            }
            return false;
        };

        UNIT_ASSERT_C(env.SkeletonId, "failed to discover initial Skeleton actor");
        UNIT_ASSERT_C(env.WaitReady(), "target VDisk did not become ready before LocalSyncData injection");
        // Write LocalSyncData durably but hide the OK result from VDisk, matching the OOM/restart
        // window covered by the main regression test.
        env.SendInterruptedLocalSyncData();
        UNIT_ASSERT_C(!env.ObservedOutOfSpace(), "initial LocalSyncData write exhausted space");
        UNIT_ASSERT_C(env.InterruptedLsn, "test did not capture interrupted LocalSyncData LSN");

        env.RestartVDisk();
        UNIT_ASSERT_C(env.WaitReady(),
            "target VDisk did not become ready after interrupted LocalSyncData"
            << " interruptedLsn# " << env.InterruptedLsn);

        UNIT_ASSERT_C(env.PumpUntil([&] { return observedInsufficientCut || env.ObservedOutOfSpace(); },
                600, TDuration::MilliSeconds(10)),
            "test did not observe an insufficient recovery log cut"
            << " interruptedLsn# " << env.InterruptedLsn
            << " firstCutFirstLsnToKeep# " << firstCutFirstLsnToKeep
            << " lastCutFirstLsnToKeep# " << env.LastCutFirstLsnToKeep
            << " cutRequestsAfterRestart# " << env.CutRequestsAfterRestart);

        UNIT_ASSERT_C(!env.ObservedOutOfSpace(), "test exhausted space before checking cut retry");
        UNIT_ASSERT_C(env.PumpUntil([&] { return env.CutRequestsAfterRestart > 1 || env.ObservedOutOfSpace(); },
                100, TDuration::MilliSeconds(10)),
            "startup data sync did not retry recovery log cut after an insufficient cut"
            << " interruptedLsn# " << env.InterruptedLsn
            << " firstCutFirstLsnToKeep# " << firstCutFirstLsnToKeep
            << " lastCutFirstLsnToKeep# " << env.LastCutFirstLsnToKeep
            << " cutRequestsAfterRestart# " << env.CutRequestsAfterRestart);
    }

} // Y_UNIT_TEST_SUITE(TBlobStorageSyncLogRealPDisk)

} // namespace NKikimr
