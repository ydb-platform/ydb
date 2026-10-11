#include <ydb/core/blobstorage/group_selector/group_selector.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_osiris.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeletonfront.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.h>

namespace {

enum class ERecoveryScenario {
    OfflineRealm,
    TwoWipedRealms,
    DelayedUnorderedSession,
};

void TestLostDataRecovery(bool emergencyMode, bool unorderedData = false,
        ERecoveryScenario scenario = ERecoveryScenario::OfflineRealm) {
    TEnvironmentSetup env({
        .NodeCount = 9,
        .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        .PrepareRuntime = [=](TTestActorSystem& runtime) {
            runtime.SetupNodeSubSystems = [=](ui32, TActorSystemSetup* setup) {
                auto selector = std::make_unique<TBlobStorageGroupSelector>("");
                TString error;
                UNIT_ASSERT(selector->AddRules(emergencyMode ? "ptest" : "pother", &error));
                setup->RegisterSubSystem(std::move(selector));
            };
        },
        .VDiskConfigPreprocessor = [=](TVDiskConfig& config) {
            config.EnableVDiskCooldownTimeout = false;
            config.RunAnubis = false;
            config.SyncJobTimeout = scenario == ERecoveryScenario::DelayedUnorderedSession
                ? TDuration::Max() : TDuration::Seconds(1);
            config.SyncerRLDRetryTimeout = TDuration::Seconds(1);
        },
        .EnablePhantomFlagStorage = unorderedData,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const auto groups = env.GetGroups();
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
    const auto info = env.GetGroupInfo(groups.front());

    ui32 healthyRealm = 0;
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        if (info->GetActorId(i).NodeId() == 1) {
            healthyRealm = info->GetVDiskId(i).FailRealm;
        }
    }
    const ui32 lostRealm = (healthyRealm + 1) % 3;
    const ui32 offlineRealm = (healthyRealm + 2) % 3;
    ui32 delayedSessionNode = 0;
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        if (info->GetVDiskId(i).FailRealm == lostRealm && info->GetVDiskId(i).FailDomain == 0) {
            delayedSessionNode = info->GetActorId(i).NodeId();
        }
        if (scenario != ERecoveryScenario::TwoWipedRealms && info->GetVDiskId(i).FailRealm == offlineRealm) {
            env.StopNode(info->GetActorId(i).NodeId());
        }
    }

    const TString data(128, 'X');
    const TLogoBlobID id(5000, 1, 1, 0, data.size(), 0);
    env.PutBlob(groups.front(), id, data);
    const ui64 blockedTablet = 6000;
    const ui32 blockedGeneration = 7;
    const auto blockEdge = env.Runtime->AllocateEdgeActor(1);
    env.Runtime->WrapInActorContext(blockEdge, [&] {
        SendToBSProxy(blockEdge, groups.front(), new TEvBlobStorage::TEvBlock(
            blockedTablet, blockedGeneration, TInstant::Max()));
    });
    const auto blockReply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(blockEdge);
    UNIT_ASSERT_VALUES_EQUAL(blockReply->Get()->Status, NKikimrProto::OK);
    env.Sim(TDuration::Seconds(30));

    const auto baseConfig = env.FetchBaseConfig();
    bool wiping = true;
    bool holdHealthyPeer = true;
    bool holdSession = scenario == ERecoveryScenario::DelayedUnorderedSession;
    using THeldEvents = std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>>;
    THeldEvents heldFullSyncRequests;
    THeldEvents heldSnapshotReplies;
    ui32 earlyFullSyncResponses = 0;
    std::set<ui32> recoveredNodes;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFull::EventType) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVSyncFull>()->Record;
            const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
            const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
            const bool recoveringSource = source.FailRealm == lostRealm ||
                (scenario == ERecoveryScenario::TwoWipedRealms && source.FailRealm == offlineRealm);
            const bool delayedHealthyPeer = target.FailRealm == healthyRealm &&
                (scenario == ERecoveryScenario::TwoWipedRealms || target.FailDomain == 0);
            if (recoveringSource && (wiping || (holdHealthyPeer && delayedHealthyPeer))) {
                if (scenario == ERecoveryScenario::DelayedUnorderedSession) {
                    // A disabled SyncJobTimeout cannot retry a dropped request. Delay it instead.
                    heldFullSyncRequests.emplace_back(nodeId, std::move(ev));
                }
                return false;
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFullResult::EventType) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVSyncFullResult>()->Record;
            if (!wiping && holdHealthyPeer && record.GetStatus() == NKikimrProto::OK &&
                    VDiskIDFromVDiskID(record.GetVDiskID()).FailRealm != healthyRealm) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetSyncState().GetSyncedLsn(), 0);
                ++earlyFullSyncResponses;
            }
        } else if (holdSession && !wiping &&
                ev->GetTypeRewrite() == NSyncLog::TEvPhantomFlagStorageGetSnapshotResult::EventType &&
                ev->Recipient.NodeId() == delayedSessionNode) {
            const auto* recipient = env.Runtime->GetActor(ev->GetRecipientRewrite());
            if (recipient && recipient->GetActivityType() ==
                    NKikimrServices::TActivity::BS_HULL_SYNC_FULL_UNORDERED_DATA_PROTOCOL) {
                heldSnapshotReplies.emplace_back(nodeId, std::move(ev));
                return false;
            }
        } else if (ev->GetTypeRewrite() == TEvSyncerLostDataRecovered::EventType) {
            recoveredNodes.insert(ev->Sender.NodeId());
        }
        return true;
    };
    auto releaseFullSyncRequests = [&] {
        THeldEvents events;
        events.swap(heldFullSyncRequests);
        for (auto& [nodeId, event] : events) {
            env.Runtime->Send(event.release(), nodeId);
        }
    };

    for (const auto& slot : baseConfig.GetVSlot()) {
        if (slot.GetGroupId() == groups.front() && (slot.GetFailRealmIdx() == lostRealm ||
                (scenario == ERecoveryScenario::TwoWipedRealms && slot.GetFailRealmIdx() == offlineRealm))) {
            const TVDiskID vdiskId(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                slot.GetFailDomainIdx(), slot.GetVDiskIdx());
            const auto& location = slot.GetVSlotId();
            env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), vdiskId);
        }
    }
    wiping = false;
    releaseFullSyncRequests();
    env.Sim(TDuration::Seconds(30));
    UNIT_ASSERT(recoveredNodes.empty()); // Even with early peer replies, every healthy-domain full sync is required.
    UNIT_ASSERT_VALUES_EQUAL(earlyFullSyncResponses != 0, emergencyMode);

    holdHealthyPeer = false;
    releaseFullSyncRequests();
    if (holdSession) {
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(recoveredNodes.size(), 1);
        UNIT_ASSERT(recoveredNodes.contains(delayedSessionNode));
        UNIT_ASSERT(!heldSnapshotReplies.empty());
        holdSession = false;
        for (auto& [nodeId, event] : heldSnapshotReplies) {
            env.Runtime->Send(event.release(), nodeId);
        }
    }
    env.Sim(TDuration::Minutes(2));
    UNIT_ASSERT_VALUES_EQUAL(recoveredNodes.size(), emergencyMode
        ? (scenario == ERecoveryScenario::TwoWipedRealms ? 6 : 3) : 0);

    if (emergencyMode) {
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const auto vdiskId = info->GetVDiskId(i);
            if (vdiskId.FailRealm != lostRealm &&
                    !(scenario == ERecoveryScenario::TwoWipedRealms && vdiskId.FailRealm == offlineRealm)) {
                continue;
            }
            env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
                const auto edge = env.Runtime->AllocateEdgeActor(queueId.NodeId());
                env.Runtime->Send(new IEventHandle(queueId, edge,
                    new TEvBlobStorage::TEvVGetBlock(blockedTablet, vdiskId, TInstant::Max())), edge.NodeId());
                const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetBlockResult>(edge);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetGeneration(), blockedGeneration);
            });
        }
        TBlobStorageGroupInfo::TVDiskIds vdiskIds;
        TBlobStorageGroupInfo::TServiceIds serviceIds;
        info->PickSubgroup(id.Hash(), &vdiskIds, &serviceIds);
        ui32 primaryIndex = Max<ui32>();
        for (ui32 i = 0; i < 3; ++i) {
            if (vdiskIds[i].FailRealm == lostRealm) {
                primaryIndex = i;
            }
        }
        UNIT_ASSERT(primaryIndex != Max<ui32>());
        env.CheckBlob(serviceIds[primaryIndex], vdiskIds[primaryIndex], TLogoBlobID(id, primaryIndex + 1), data);
    }
    env.Runtime->FilterFunction = {};
}

void TestAutomaticDoNotKeep(bool emergencyMode) {
    TEnvironmentSetup env({
        .NodeCount = 1,
        .Erasure = TBlobStorageGroupType::ErasureNone,
        .PrepareRuntime = [=](TTestActorSystem& runtime) {
            runtime.SetupNodeSubSystems = [=](ui32, TActorSystemSetup* setup) {
                auto selector = std::make_unique<TBlobStorageGroupSelector>("");
                TString error;
                UNIT_ASSERT(selector->AddRules(emergencyMode ? "*" : "", &error));
                setup->RegisterSubSystem(std::move(selector));
            };
        },
    });
    std::vector<std::unique_ptr<IEventHandle>> recoveryDone;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvSyncGuidRecoveryDone::EventType) {
            recoveryDone.push_back(std::move(ev));
            return false;
        }
        return true;
    };
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    UNIT_ASSERT(!recoveryDone.empty());
    const auto groups = env.GetGroups();
    const auto info = env.GetGroupInfo(groups.front());
    const auto actorId = info->GetActorId(0);
    const auto vdiskId = info->GetVDiskId(0);
    const auto edge = env.Runtime->AllocateEdgeActor(actorId.NodeId());
    const TString data(128, 'X');
    const TLogoBlobID anubisId(5000, 1, 1, 0, data.size(), 0);
    const TLogoBlobID phantomId(5000, 1, 2, 0, data.size(), 0);
    const TLogoBlobID gcId(5000, 1, 3, 0, data.size(), 0);
    auto forward = [&](IEventBase* message, ui64 cookie) {
        auto event = std::make_unique<IEventHandle>(actorId, edge, message, 0, cookie);
        event->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
        env.Runtime->Send(event.release(), edge.NodeId());
    };
    forward(new TEvAnubisOsirisPut(anubisId), 73);
    const auto anubisReply = env.WaitForEdgeActorEvent<TEvAnubisOsirisPutResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(anubisReply->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(anubisReply->Cookie, 73);

    env.Runtime->FilterFunction = {};
    for (auto& event : recoveryDone) {
        env.Runtime->Send(event.release(), actorId.NodeId());
    }
    env.Sim(TDuration::Seconds(5));
    for (const auto& id : {anubisId, phantomId, gcId}) {
        env.PutBlob(groups.front(), id, data);
    }

    forward(new TEvDetectedPhantomBlob(TDeque<TLogoBlobID>{phantomId}), 74);
    const auto phantomReply = env.Runtime->WaitForEdgeActorEvent({edge});
    UNIT_ASSERT_VALUES_EQUAL(phantomReply->GetTypeRewrite(), ui32(TEvBlobStorage::EvDetectedPhantomBlobCommitted));
    UNIT_ASSERT_VALUES_EQUAL(phantomReply->Cookie, 74);

    env.Runtime->WrapInActorContext(edge, [&] {
        SendToBSProxy(edge, groups.front(), new TEvBlobStorage::TEvCollectGarbage(gcId.TabletID(), 1, 1,
            gcId.Channel(), false, 0, 0, nullptr, new TVector<TLogoBlobID>{gcId}, TInstant::Max(), false));
    });
    const auto gcReply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
    UNIT_ASSERT_VALUES_EQUAL(gcReply->Get()->Status, NKikimrProto::OK);

    env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
        auto query = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(vdiskId, TInstant::Max(),
            NKikimrBlobStorage::FastRead);
        query->Record.SetSuppressBarrierCheck(true);
        for (const auto& id : {anubisId, phantomId, gcId}) {
            query->AddExtremeQuery(id, 0, 0);
        }
        env.Runtime->Send(new IEventHandle(queueId, edge, query.release()), edge.NodeId());
        const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
        const auto& record = reply->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 3);
        for (const auto& result : record.GetResult()) {
            const auto id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
            UNIT_ASSERT_VALUES_EQUAL(result.GetDoNotKeep(), id == gcId || !emergencyMode);
        }
    });
}

} // namespace

Y_UNIT_TEST_SUITE(EmergencyRecovery) {
    Y_UNIT_TEST(GroupSelectionAppliesOnlyAfterVDiskRestart) {
        TTempDir dir;
        const TString path = dir.Path().Child("rules.conf").GetPath();
        std::vector<TIntrusivePtr<TVDiskConfig>> configs;
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .PrepareRuntime = [&](TTestActorSystem& runtime) {
                runtime.SetupNodeSubSystems = [&](ui32, TActorSystemSetup* setup) {
                    setup->RegisterSubSystem(std::make_unique<TBlobStorageGroupSelector>(path));
                };
            },
            .VDiskConfigPreprocessor = [&](TVDiskConfig& config) {
                configs.emplace_back(&config);
            },
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(configs.size(), 1);
        UNIT_ASSERT(!configs.front()->EmergencyRecoveryMode);
        const auto groups = env.GetGroups();
        const auto info = env.GetGroupInfo(groups.front());
        auto* selector = env.Runtime->GetNode(1)->ActorSystem->GetSubSystem<TBlobStorageGroupSelector>();
        TString error;
        UNIT_ASSERT(selector->AddRules(TStringBuilder() << 'g' << groups.front(), &error));
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(!configs.front()->EmergencyRecoveryMode);

        const auto edge = env.Runtime->AllocateEdgeActor(1);
        auto restartVDisk = [&] {
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge,
                new TEvBlobStorage::TEvAskRestartVDisk(configs.back()->BaseInfo.PDiskId, info->GetVDiskId(0))), 1);
            env.Sim(TDuration::Seconds(30));
        };
        restartVDisk();
        UNIT_ASSERT_VALUES_EQUAL(configs.size(), 2);
        UNIT_ASSERT(configs.back()->EmergencyRecoveryMode);
        UNIT_ASSERT(!configs.front()->EmergencyRecoveryMode);
        UNIT_ASSERT_VALUES_EQUAL(selector,
            env.Runtime->GetNode(1)->ActorSystem->GetSubSystem<TBlobStorageGroupSelector>());
        {
            TFileOutput file(path);
            file << "pother\n";
        }
        UNIT_ASSERT(selector->ReloadRules(&error));
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(configs.back()->EmergencyRecoveryMode);
        restartVDisk();
        UNIT_ASSERT_VALUES_EQUAL(configs.size(), 3);
        UNIT_ASSERT(!configs.back()->EmergencyRecoveryMode);
        UNIT_ASSERT(configs[1]->EmergencyRecoveryMode);
        const TString data(128, 'R');
        const TLogoBlobID id(5000, 1, 1, 0, data.size(), 0);
        env.PutBlob(groups.front(), id, data);
        env.CheckBlob(info->GetActorId(0), info->GetVDiskId(0), TLogoBlobID(id, 1), data);
    }

    Y_UNIT_TEST(RecoverLostDataWithOfflineRealm) {
        TestLostDataRecovery(true);
    }

    Y_UNIT_TEST(UnselectedPoolKeepsRecoveryBlocked) {
        TestLostDataRecovery(false);
    }

    Y_UNIT_TEST(RecoverLostDataWithOfflineRealmUnorderedData) {
        TestLostDataRecovery(true, true);
    }

    Y_UNIT_TEST(UnselectedPoolKeepsRecoveryBlockedUnorderedData) {
        TestLostDataRecovery(false, true);
    }

    Y_UNIT_TEST(TwoWipedRealmsWaitForPreservedMetadata) {
        TestLostDataRecovery(true, false, ERecoveryScenario::TwoWipedRealms);
    }

    Y_UNIT_TEST(TwoWipedRealmsWaitForPreservedMetadataUnorderedData) {
        TestLostDataRecovery(true, true, ERecoveryScenario::TwoWipedRealms);
    }

    Y_UNIT_TEST(UnorderedRequestSurvivesRecoveryCompletionWithoutTimeout) {
        TestLostDataRecovery(true, true, ERecoveryScenario::DelayedUnorderedSession);
    }

    Y_UNIT_TEST(AutomaticDoNotKeepSuppressedAndGcPreserved) {
        TestAutomaticDoNotKeep(true);
    }

    Y_UNIT_TEST(UnselectedPoolKeepsAutomaticDoNotKeep) {
        TestAutomaticDoNotKeep(false);
    }
}
