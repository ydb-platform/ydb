#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_osiris.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_operation_broker.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeletonfront.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.h>

namespace {

void TestLostDataRecovery(bool emergencyMode, bool unorderedData = false) {
    TEnvironmentSetup env({
        .NodeCount = 9,
        .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        .VDiskConfigPreprocessor = [=](TVDiskConfig& config) {
            config.EmergencyRecoveryMode = emergencyMode;
            config.EnableVDiskCooldownTimeout = false;
            config.SyncJobTimeout = TDuration::Seconds(1);
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
    ui32 waitingNode = 0;
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        if (info->GetVDiskId(i).FailRealm == lostRealm && info->GetVDiskId(i).FailDomain == 0) {
            waitingNode = info->GetActorId(i).NodeId();
        }
        if (info->GetVDiskId(i).FailRealm == offlineRealm) {
            env.StopNode(info->GetActorId(i).NodeId());
        }
    }

    const TString data(128, 'X');
    const TLogoBlobID id(5000, 1, 1, 0, data.size(), 0);
    env.PutBlob(groups.front(), id, data);
    env.Sim(TDuration::Seconds(30));

    const auto baseConfig = env.FetchBaseConfig();
    bool wiping = true;
    bool holdHealthyPeer = true;
    bool holdStartupToken = true;
    bool waitingPeerReplied = false;
    std::vector<std::unique_ptr<IEventHandle>> heldStartupTokens;
    ui32 earlyFullSyncResponses = 0;
    std::set<ui32> recoveredNodes;
    env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (holdStartupToken && ev->GetTypeRewrite() == TEvAcquireVDiskOperationToken::EventType &&
                ev->Recipient == MakeBlobStorageStartupDataSyncBrokerID() &&
                ev->Get<TEvAcquireVDiskOperationToken>()->VDiskServiceId.NodeId() == waitingNode) {
            heldStartupTokens.push_back(std::move(ev));
            return false;
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFull::EventType) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVSyncFull>()->Record;
            const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
            const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
            if (!wiping && holdStartupToken && source.FailRealm == lostRealm) {
                UNIT_ASSERT(source.FailDomain != 0); // Outgoing recovery still needs its broker token.
            }
            if (source.FailRealm == lostRealm && (wiping ||
                    (holdHealthyPeer && target.FailRealm == healthyRealm && target.FailDomain == 0))) {
                return false;
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFullResult::EventType) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVSyncFullResult>()->Record;
            if (!wiping && holdHealthyPeer && record.GetStatus() == NKikimrProto::OK &&
                    VDiskIDFromVDiskID(record.GetVDiskID()).FailRealm == lostRealm) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetSyncState().GetSyncedLsn(), 0);
                ++earlyFullSyncResponses;
                waitingPeerReplied |= VDiskIDFromVDiskID(record.GetVDiskID()).FailDomain == 0;
            }
        } else if (ev->GetTypeRewrite() == TEvSyncerLostDataRecovered::EventType) {
            recoveredNodes.insert(ev->Sender.NodeId());
        }
        return true;
    };

    for (const auto& slot : baseConfig.GetVSlot()) {
        if (slot.GetGroupId() == groups.front() && slot.GetFailRealmIdx() == lostRealm) {
            const TVDiskID vdiskId(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                slot.GetFailDomainIdx(), slot.GetVDiskIdx());
            const auto& location = slot.GetVSlotId();
            env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), vdiskId);
        }
    }
    wiping = false;
    env.Sim(TDuration::Seconds(30));
    UNIT_ASSERT(recoveredNodes.empty()); // Even with early peer replies, every healthy-domain full sync is required.
    UNIT_ASSERT_VALUES_EQUAL(earlyFullSyncResponses != 0, emergencyMode);
    UNIT_ASSERT(!heldStartupTokens.empty());
    UNIT_ASSERT_VALUES_EQUAL(waitingPeerReplied, emergencyMode);

    holdHealthyPeer = false;
    holdStartupToken = false;
    for (auto& event : heldStartupTokens) {
        env.Runtime->Send(event.release(), waitingNode);
    }
    env.Sim(TDuration::Minutes(2));
    UNIT_ASSERT_VALUES_EQUAL(recoveredNodes.size(), emergencyMode ? 3 : 0);

    if (emergencyMode) {
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
        .VDiskConfigPreprocessor = [=](TVDiskConfig& config) {
            config.EmergencyRecoveryMode = emergencyMode;
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

    Y_UNIT_TEST(AutomaticDoNotKeepSuppressedAndGcPreserved) {
        TestAutomaticDoNotKeep(true);
    }

    Y_UNIT_TEST(UnselectedPoolKeepsAutomaticDoNotKeep) {
        TestAutomaticDoNotKeep(false);
    }
}
