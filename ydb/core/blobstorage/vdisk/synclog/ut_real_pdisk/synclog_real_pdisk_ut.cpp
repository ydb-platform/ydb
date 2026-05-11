#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogdata.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper_committer.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/pdisk_io/aio.h>

#include <library/cpp/testing/unittest/registar.h>

#include <deque>

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
    vdiskConfig->RunRepl = false;
    vdiskConfig->RunSyncer = false;
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

class TTestRuntimeCallbackGuard {
public:
    TTestRuntimeCallbackGuard(TTestActorRuntimeBase& runtime,
            TTestActorRuntimeBase::TEventObserver previousObserver,
            TTestActorRuntimeBase::TEventFilter previousEventFilter)
        : Runtime(runtime)
        , PreviousObserver(std::move(previousObserver))
        , PreviousEventFilter(std::move(previousEventFilter))
    {}

    TTestRuntimeCallbackGuard(const TTestRuntimeCallbackGuard&) = delete;
    TTestRuntimeCallbackGuard& operator=(const TTestRuntimeCallbackGuard&) = delete;

    ~TTestRuntimeCallbackGuard() {
        Runtime.SetEventFilter(std::move(PreviousEventFilter));
        Runtime.SetObserverFunc(std::move(PreviousObserver));
    }

private:
    TTestActorRuntimeBase& Runtime;
    TTestActorRuntimeBase::TEventObserver PreviousObserver;
    TTestActorRuntimeBase::TEventFilter PreviousEventFilter;
};

TRealStorage SetupRealPDiskAndRealVDisk(TTestActorRuntime& runtime,
        TBlobStorageGroupType::EErasureSpecies erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        const TRealPDiskTestConfig& testConfig = {}) {
    runtime.Initialize(MakeRuntimeEgg());

    const ui32 nodeId = runtime.GetNodeId(NodeIndex);
    const ui32 groupId = 0;
    const TBlobStorageGroupType groupType(erasure);
    const ui32 numVDisks = groupType.BlobSubgroupSize();
    const TString pdiskPath = TStringBuilder() << runtime.GetTempDir() << testConfig.PDiskPathSuffix;
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    const TActorId pdiskServiceId = MakeBlobStoragePDiskID(nodeId, PDiskId);

    const auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(testConfig.DiskSize);
    TFormatOptions formatOptions;
    formatOptions.SectorMap = sectorMap;
    formatOptions.EnableSmallDiskOptimization = false;
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
    pdiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
    pdiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;

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

} // namespace

Y_UNIT_TEST_SUITE(TBlobStorageSyncLogRealPDisk) {

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
        auto storage = SetupRealPDiskAndRealVDisk(runtime, TBlobStorageGroupType::ErasureNone, testConfig);
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

        auto dispatchFor = [&](TDuration timeout) {
            TDispatchOptions options;
            const TDuration oldDispatchTimeout = runtime.SetDispatchTimeout(TDuration::MilliSeconds(10));
            try {
                runtime.DispatchEvents(options, timeout);
            } catch (const TEmptyEventQueueException&) {
            }
            runtime.SetDispatchTimeout(oldDispatchTimeout);
        };

        auto pumpUntil = [&](auto predicate, ui32 iterations, TDuration step) {
            for (ui32 i = 0; i < iterations && !predicate(); ++i) {
                dispatchFor(step);
            }
            return predicate();
        };

        auto releasePostponedFreeChunkEvents = [&] {
            while (!postponedFreeChunkEvents.empty()) {
                TAutoPtr<IEventHandle> ev(postponedFreeChunkEvents.front().Release());
                postponedFreeChunkEvents.pop_front();
                runtime.Send(ev, NodeIndex);
            }
        };

        auto sendVStatus = [&] {
            runtime.Send(new IEventHandle(vdiskActorId, edge, new TEvBlobStorage::TEvVStatus(vdiskId),
                IEventHandle::FlagTrackDelivery), NodeIndex);
            return runtime.GrabEdgeEvent<TEvBlobStorage::TEvVStatusResult>(edge, TDuration::Seconds(1));
        };

        auto waitReady = [&] {
            return pumpUntil([&] {
                if (auto res = sendVStatus()) {
                    return res->Get()->Record.GetStatus() == NKikimrProto::OK;
                }
                return false;
            }, 60, TDuration::MilliSeconds(100));
        };

        UNIT_ASSERT_C(waitReady(), "target VDisk did not become ready");

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
                dispatchFor(TDuration::MilliSeconds(1));
            }
        };

        auto requestSyncLogCut = [&] {
            runtime.Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), NodeIndex);
            dispatchFor(TDuration::MilliSeconds(1));
        };

        writeTargetRecords(42, 50'000, "initial real-pdisk sync-log fill");

        UNIT_ASSERT_C(pumpUntil([&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, 300, TDuration::MilliSeconds(10)),
            "failed to discover SyncLog, SyncLogKeeper, PDisk owner, or data LSN");

        requestSyncLogCut();
        UNIT_ASSERT_C(pumpUntil([&] {
            return observedSyncLogDataCommit && syncLogCommitDoneEvents;
        }, 3000, TDuration::MilliSeconds(10)),
            "initial writes did not move SyncLog data to real PDisk chunks");

        const ui32 commitDoneBeforePostponedDataCommit = syncLogCommitDoneEvents;
        writeTargetRecords(42, 55'000, "pre-race real-pdisk sync-log fill");
        postponeNextSyncLogDataCommit = true;
        requestSyncLogCut();
        UNIT_ASSERT_C(pumpUntil([&] {
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
        UNIT_ASSERT_C(pumpUntil([&] {
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

} // Y_UNIT_TEST_SUITE(TBlobStorageSyncLogRealPDisk)

} // namespace NKikimr
