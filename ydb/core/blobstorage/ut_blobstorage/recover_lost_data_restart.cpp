#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_data.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_proxyobtain.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_recovery.h>

namespace {

enum class ERestartPoint {
    FullSync,
    OsirisWriteReply,
    OsirisDone,
    RecoveryCommitted,
    GuidPublicationBeforeLost,
    GuidPublicationWithLost,
    GuidPublicationWithFinal,
};

void TestRestart(ERestartPoint point) {
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const ui32 groupId = env.GetGroups().front();
    const auto info = env.GetGroupInfo(groupId);
    const TString data(1024, 'R');
    const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
    TBlobStorageGroupInfo::TOrderNums subgroup;
    info->GetTopology().PickSubgroup(blob.Hash(), subgroup);
    const ui32 recoveringOrder = subgroup.front();
    const TVDiskID recoveringDisk = info->GetVDiskId(recoveringOrder);
    const TActorId recoveringActor = info->GetActorId(recoveringOrder);
    const ui32 recoveringNode = recoveringActor.NodeId();

    // Lose the part before this disk can publish its ingress to any peer.
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        return nodeId != recoveringNode || (ev->GetTypeRewrite() != TEvBlobStorage::EvVSync &&
            ev->GetTypeRewrite() != TEvBlobStorage::EvVSyncFull);
    };
    env.PutBlob(groupId, blob, data);
    env.Sim(TDuration::Seconds(10));
    env.Cleanup();
    for (auto& [key, state] : env.PDiskMockStates) {
        if (key.first == recoveringNode) {
            state.Reset();
        }
    }
    env.Initialize();

    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    const bool prepareLost = point == ERestartPoint::GuidPublicationWithLost;
    const bool prepareFinal = point == ERestartPoint::GuidPublicationWithFinal;
    ui64 dbBirthLsnBeforePublication = 0;
    if (prepareLost || prepareFinal) {
        bool prepared = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId != recoveringNode) {
                return true;
            }
            const ui32 type = ev->GetTypeRewrite();
            // Obtain a quorum consisting entirely of healthy peers. This goes straight to Lost,
            // leaving our own peer-GUID entry Empty for the next startup's publication repair.
            if (type == TEvBlobStorage::EvVDiskGuidObtained &&
                    ev->Get<TEvVDiskGuidObtained>()->VDiskId == recoveringDisk) {
                return false;
            }
            if (prepareLost && type == TEvBlobStorage::EvSyncerFullSyncedWithPeer) {
                prepared = true;
                return false;
            }
            if (prepareFinal && type == TEvBlobStorage::EvSyncerLostDataRecovered) {
                dbBirthLsnBeforePublication = ev->Get<TEvSyncerLostDataRecovered>()->LocalSyncerState.DbBirthLsn;
                prepared = true;
                return false;
            }
            return true;
        };
        waitUntil([&] { return prepared; }, "Recovery must commit the local state before GUID republication");
        if (prepareFinal) {
            UNIT_ASSERT_C(dbBirthLsnBeforePublication, "Completed lost-data recovery must have a nonzero DbBirthLsn");
        }
    }

    bool stopped = false;
    bool restarted = false;
    ui32 osirisCallsAfterRestart = 0;
    ui32 osirisWrites = 0;
    std::optional<ui64> dbBirthLsnAfterRestart;
    std::optional<NSyncer::EDecision> decisionAfterRestart;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != recoveringNode) {
            return true;
        }
        const ui32 type = ev->GetTypeRewrite();
        if (restarted) {
            if (type == TEvBlobStorage::EvVDiskGuidRecovered) {
                decisionAfterRestart = ev->Get<TEvVDiskGuidRecovered>()->Outcome.Decision;
            } else if (type == TEvBlobStorage::EvCallOsiris) {
                ++osirisCallsAfterRestart;
            } else if (type == TEvBlobStorage::EvSyncGuidRecoveryDone) {
                dbBirthLsnAfterRestart = ev->Get<TEvSyncGuidRecoveryDone>()->DbBirthLsn;
            }
            return true;
        }
        if (type == TEvBlobStorage::EvAnubisOsirisPut) {
            ++osirisWrites;
        }
        const bool stopAfterPublication = point == ERestartPoint::GuidPublicationBeforeLost || prepareLost || prepareFinal;
        if (stopAfterPublication && type == TEvBlobStorage::EvVDiskGuidObtained) {
            // Delay two peers' survey replies, so the first quorum includes the wiped disk's own Empty reply.
            // No reply contents are modified; all peers remain available for GUID publication and data recovery.
            const ui32 order = info->GetOrderNumber(ev->Get<TEvVDiskGuidObtained>()->VDiskId);
            if (order == (recoveringOrder + 1) % 8 || order == (recoveringOrder + 2) % 8) {
                return false;
            }
        }
        const bool stop =
            (point == ERestartPoint::FullSync && type == TEvBlobStorage::EvSyncerFullSyncedWithPeer) ||
            (point == ERestartPoint::OsirisWriteReply && type == TEvBlobStorage::EvAnubisOsirisPutResult) ||
            (point == ERestartPoint::OsirisDone && type == TEvBlobStorage::EvOsirisDone) ||
            (point == ERestartPoint::RecoveryCommitted && type == TEvBlobStorage::EvSyncerLostDataRecovered) ||
            (stopAfterPublication && type == TEvBlobStorage::EvSyncerGuidFirstRunDone);
        if (stop) {
            stopped = true;
            return false;
        }
        return true;
    };
    if (prepareLost || prepareFinal) {
        env.RestartNode(recoveringNode);
    }
    waitUntil([&] { return stopped; }, "The recovery must reach the requested restart point");
    if (point == ERestartPoint::OsirisWriteReply || point == ERestartPoint::OsirisDone ||
            point == ERestartPoint::RecoveryCommitted) {
        UNIT_ASSERT_C(osirisWrites, "Osiris must actually resurrect the lost part's ingress");
    }

    // This restart preserves all committed PDisk data from the interrupted recovery.
    restarted = true;
    env.RestartNode(recoveringNode);
    waitUntil([&] { return decisionAfterRestart.has_value(); }, "GUID recovery must finish after restart");
    const bool expectGood = point == ERestartPoint::RecoveryCommitted || prepareFinal;
    const auto expectedDecision = expectGood
        ? NSyncer::EDecision::Good : NSyncer::EDecision::LostData;
    Cerr << "decisionAfterRestart# " << NSyncer::EDecisionToStr(*decisionAfterRestart)
        << " expected# " << NSyncer::EDecisionToStr(expectedDecision) << Endl;
    env.WaitForVDiskRepl(recoveringActor, recoveringDisk);
    if (prepareFinal) {
        UNIT_ASSERT(dbBirthLsnAfterRestart);
        UNIT_ASSERT_VALUES_EQUAL(*dbBirthLsnAfterRestart, dbBirthLsnBeforePublication);
    }
    env.Runtime->FilterFunction = {};

    // Check the wiped disk itself, rather than allowing the group proxy to read surviving peers.
    const TActorId queue = env.CreateQueueActor(recoveringDisk, NKikimrBlobStorage::GetFastRead, 0);
    const TActorId edge = env.Runtime->AllocateEdgeActor(recoveringNode);
    auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(recoveringDisk, TInstant::Max(),
        NKikimrBlobStorage::FastRead);
    query->AddExtremeQuery(TLogoBlobID(blob, 1), 0, 0);
    env.Runtime->Send(new IEventHandle(queue, edge, query.release()), recoveringNode);
    auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.ResultSize(), 1);
    Cerr << "osirisCallsAfterRestart# " << osirisCallsAfterRestart << " recoveredPartStatus# "
        << NKikimrProto::EReplyStatus_Name(result->Get()->Record.GetResult(0).GetStatus()) << Endl;
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(*decisionAfterRestart), static_cast<int>(expectedDecision));
    UNIT_ASSERT_VALUES_EQUAL(osirisCallsAfterRestart, expectGood ? 0 : 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetResult(0).GetStatus(), NKikimrProto::OK);
}

void TestRollingWipe(bool delaySelf,
        std::optional<NKikimrBlobStorage::TSyncGuidInfo::EState> injectedSelfState = {}) {
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const ui32 groupId = env.GetGroups().front();
    const auto info = env.GetGroupInfo(groupId);
    ui32 unavailableOrder = 0;
    while (info->GetActorId(unavailableOrder).NodeId() == env.Settings.ControllerNodeId) {
        ++unavailableOrder;
    }
    const TVDiskID unavailableDisk = info->GetVDiskId(unavailableOrder);
    const TActorId unavailableActor = info->GetActorId(unavailableOrder);
    const ui32 unavailableNode = unavailableActor.NodeId();
    ui64 conflictingGuid = 0;
    if (injectedSelfState) {
        const auto edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
        env.Runtime->Send(new IEventHandle(unavailableActor, edge,
            new TEvBlobStorage::TEvVSyncGuid(unavailableDisk, unavailableDisk)), unavailableNode);
        const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVSyncGuidResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetReadInfo().GetState(), NKikimrBlobStorage::TSyncGuidInfo::Final);
        conflictingGuid = reply->Get()->Record.GetReadInfo().GetGuid() == 1 ? 2 : 1;
    }
    env.StopNode(unavailableNode);
    const ui64 tabletId = 1;
    const ui32 blockedGeneration = 42;
    const TActorId blockEdge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
    env.Runtime->WrapInActorContext(blockEdge, [&] {
        SendToBSProxy(blockEdge, groupId, new TEvBlobStorage::TEvBlock(tabletId, blockedGeneration, TInstant::Max()));
    });
    const auto block = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(blockEdge);
    UNIT_ASSERT_VALUES_EQUAL(block->Get()->Status, NKikimrProto::OK);
    env.Sim(TDuration::Seconds(30));

    std::set<ui32> recoveredNodes;
    std::optional<NSyncer::EDecision> decision;
    std::vector<std::unique_ptr<IEventHandle>> delayedReplies;
    ui32 surveyReplies = 0;
    bool holdingReplies = true;
    bool returning = false;
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        const ui32 type = ev->GetTypeRewrite();
        if (type == TEvBlobStorage::EvSyncGuidRecoveryDone) {
            if (nodeId != unavailableNode) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvSyncGuidRecoveryDone>()->Status, NKikimrProto::OK);
            }
            recoveredNodes.insert(nodeId);
        }
        if (returning && nodeId == unavailableNode) {
            if (injectedSelfState && type == TEvBlobStorage::EvVSyncGuidResult) {
                auto& record = ev->Get<TEvBlobStorage::TEvVSyncGuidResult>()->Record;
                if (record.HasReadInfo()) {
                    // A legacy responder can report a Final without GUID history.
                    record.ClearGuidHistory();
                    record.ClearRecoveredGuid();
                    auto* read = record.MutableReadInfo();
                    read->Clear();
                    if (info->GetOrderNumber(VDiskIDFromVDiskID(record.GetVDiskID())) == unavailableOrder) {
                        read->SetState(*injectedSelfState);
                        read->SetGuid(conflictingGuid);
                    }
                }
            }
            if (type == TEvBlobStorage::EvVDiskGuidObtained) {
                const ui32 order = info->GetOrderNumber(ev->Get<TEvVDiskGuidObtained>()->VDiskId);
                // Select the first survey quorum without changing any reply contents.
                if (holdingReplies && (delaySelf ? order == unavailableOrder :
                        order == (unavailableOrder + 1) % 8 || order == (unavailableOrder + 2) % 8)) {
                    delayedReplies.push_back(std::move(ev));
                    return false;
                }
                ++surveyReplies;
            } else if (type == TEvBlobStorage::EvVDiskGuidRecovered) {
                decision = ev->Get<TEvVDiskGuidRecovered>()->Outcome.Decision;
            }
        }
        return true;
    };
    auto waitUntil = [&](auto predicate, const char* phase) {
        for (ui32 i = 0; i != 600 && !predicate(); ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(predicate(), phase);
    };
    const auto config = env.FetchBaseConfig();
    for (const auto& slot : config.GetVSlot()) {
        if (slot.GetGroupId() != groupId || slot.GetVSlotId().GetNodeId() == unavailableNode) {
            continue;
        }
        const TVDiskID disk(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
            slot.GetFailDomainIdx(), slot.GetVDiskIdx());
        const auto& location = slot.GetVSlotId();
        recoveredNodes.erase(location.GetNodeId());
        env.Wipe(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), disk);
        // Do not mistake the old actor's Replicated status for completion of this wipe.
        waitUntil([&] { return recoveredNodes.contains(location.GetNodeId()); }, "Wiped peer must recover");
        env.WaitForVDiskRepl(info->GetActorId(info->GetOrderNumber(disk)), disk);
        env.Sim(TDuration::Seconds(30));
    }

    auto getBlock = [&](ui32 order) {
        const TVDiskID disk = info->GetVDiskId(order);
        const TActorId queue = env.CreateQueueActor(disk, NKikimrBlobStorage::GetFastRead, 0);
        return env.SyncQuery<TEvBlobStorage::TEvVGetBlockResult, TEvBlobStorage::TEvVGetBlock>(
            queue, tabletId, disk, TInstant::Max());
    };
    const auto peerBlock = getBlock((unavailableOrder + 1) % 8);
    UNIT_ASSERT_VALUES_EQUAL(peerBlock->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(peerBlock->Record.GetGeneration(), blockedGeneration);

    returning = true;
    env.StartNode(unavailableNode);
    waitUntil([&] { return surveyReplies >= 6; }, "Returning VDisk must receive a survey quorum");
    holdingReplies = false;
    for (auto& reply : delayedReplies) {
        env.Runtime->Send(reply.release(), unavailableNode);
    }
    waitUntil([&] { return decision.has_value(); }, "Returning VDisk must reach a GUID decision");
    if (injectedSelfState == NKikimrBlobStorage::TSyncGuidInfo::Final) {
        UNIT_ASSERT_C(*decision == NSyncer::EDecision::Inconsistency, NSyncer::EDecisionToStr(*decision));
        env.Runtime->FilterFunction = {};
        return;
    }
    UNIT_ASSERT_C(*decision == NSyncer::EDecision::Good, NSyncer::EDecisionToStr(*decision));
    waitUntil([&] { return recoveredNodes.contains(unavailableNode); }, "Returning VDisk must start");
    env.Sim(TDuration::Minutes(1));
    env.WaitForVDiskRepl(unavailableActor, unavailableDisk);
    env.Runtime->FilterFunction = {};

    // Query this VDisk directly: a group-proxy reply can hide its missing Block metadata.
    const auto result = getBlock(unavailableOrder);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetGeneration(), blockedGeneration);
}

void TestRestartWithTwoUnavailablePeers(ui32 legacyGuidPeers = 0) {
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = 8,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    TVector<ui32> stopped;
    for (ui32 order = 0; order < 8 && stopped.size() != 3; ++order) {
        if (info->GetActorId(order).NodeId() != env.Settings.ControllerNodeId) {
            stopped.push_back(order);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(stopped.size(), 3);
    for (ui32 order : stopped) {
        env.StopNode(info->GetActorId(order).NodeId());
    }
    const auto actor = info->GetActorId(stopped.front());
    const auto disk = info->GetVDiskId(stopped.front());
    std::set<ui32> legacyPeers;
    // Select self last. Four legacy replies leave self history-capable;
    // six cover every live responder, including self.
    for (ui32 offset = 1; offset <= 8 && legacyPeers.size() < legacyGuidPeers; ++offset) {
        const ui32 order = (stopped.front() + offset) % 8;
        if (order != stopped[1] && order != stopped[2]) {
            legacyPeers.insert(order);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(legacyPeers.size(), legacyGuidPeers);
    std::optional<NSyncer::EDecision> decision;
    bool replicated = false;
    bool durabilityReplicated = false;
    const auto edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
    env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->Recipient == edge && ev->GetTypeRewrite() == TEvBlobStorage::EvVStatusResult) {
            const auto& record = ev->Get<TEvBlobStorage::TEvVStatusResult>()->Record;
            replicated = record.GetReplicated();
            durabilityReplicated = record.GetDurabilityReplicated();
            return false;
        }
        if (legacyGuidPeers && nodeId == actor.NodeId()
                && ev->GetTypeRewrite() == TEvBlobStorage::EvVSyncGuidResult) {
            auto& record = ev->Get<TEvBlobStorage::TEvVSyncGuidResult>()->Record;
            const ui32 order = info->GetOrderNumber(VDiskIDFromVDiskID(record.GetVDiskID()));
            if (legacyPeers.contains(order)) {
                record.ClearGuidHistory();
                record.ClearRecoveredGuid();
            }
        }
        if (nodeId == actor.NodeId() && ev->GetTypeRewrite() == TEvBlobStorage::EvVDiskGuidRecovered) {
            decision = ev->Get<TEvVDiskGuidRecovered>()->Outcome.Decision;
        }
        return true;
    };
    env.StartNode(actor.NodeId());
    for (ui32 attempt = 0; attempt < 120 && !(replicated && durabilityReplicated); ++attempt) {
        env.Sim(TDuration::Seconds(1));
        env.Runtime->Send(new IEventHandle(actor, edge, new TEvBlobStorage::TEvVStatus(disk)), actor.NodeId());
    }
    env.Runtime->FilterFunction = {};
    UNIT_ASSERT_C(decision.has_value(), "TEvVDiskGuidRecovered must complete with two unavailable peers");
    UNIT_ASSERT_C(*decision == NSyncer::EDecision::Good, NSyncer::EDecisionToStr(*decision));
    UNIT_ASSERT_C(replicated, "Replicated must become true with two unavailable peers");
    UNIT_ASSERT_C(durabilityReplicated, "DurabilityReplicated must become true with two unavailable peers");
}

} // namespace

Y_UNIT_TEST_SUITE(RecoverLostDataRestart) {
    Y_UNIT_TEST(ReadOnlyRetainsReplicationStatus) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const auto info = env.GetGroupInfo(groupId);
        const auto config = env.FetchBaseConfig();
        for (const auto& slot : config.GetVSlot()) {
            if (slot.GetGroupId() != groupId || slot.GetVSlotId().GetNodeId() == env.Settings.ControllerNodeId) {
                continue;
            }
            const TVDiskID disk(slot.GetGroupId(), slot.GetGroupGeneration(), slot.GetFailRealmIdx(),
                slot.GetFailDomainIdx(), slot.GetVDiskIdx());
            const auto actor = info->GetActorId(info->GetOrderNumber(disk));
            env.WaitForVDiskRepl(actor, disk);
            const auto& location = slot.GetVSlotId();
            env.SetVDiskReadOnly(location.GetNodeId(), location.GetPDiskId(), location.GetVSlotId(), disk, true);
            env.StopNode(location.GetNodeId());
            env.StartNode(location.GetNodeId());
            env.Sim(TDuration::Seconds(30));
            const auto result = env.SyncQuery<TEvBlobStorage::TEvVStatusResult, TEvBlobStorage::TEvVStatus>(actor, disk);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT(result->Record.GetIsReadOnly());
            UNIT_ASSERT(result->Record.GetJoinedGroup());
            UNIT_ASSERT(result->Record.GetReplicated());
            UNIT_ASSERT(result->Record.GetDurabilityReplicated());
            return;
        }
        UNIT_FAIL("Group must have a VSlot");
    }

    Y_UNIT_TEST(RestartWithTwoUnavailablePeers) { TestRestartWithTwoUnavailablePeers(); }
    Y_UNIT_TEST(RestartWithLegacyGuidRepliesAndTwoUnavailablePeers) { TestRestartWithTwoUnavailablePeers(6); }
    Y_UNIT_TEST(RestartWithMixedGuidRepliesAndTwoUnavailablePeers) { TestRestartWithTwoUnavailablePeers(4); }
    Y_UNIT_TEST(DuringFullSync) { TestRestart(ERestartPoint::FullSync); }
    Y_UNIT_TEST(DuringOsiris) { TestRestart(ERestartPoint::OsirisWriteReply); }
    Y_UNIT_TEST(AfterOsirisBeforeFinalCommit) { TestRestart(ERestartPoint::OsirisDone); }
    Y_UNIT_TEST(AfterFinalCommitBeforeReplication) { TestRestart(ERestartPoint::RecoveryCommitted); }
    Y_UNIT_TEST(DuringGuidPublicationBeforeLostCommit) { TestRestart(ERestartPoint::GuidPublicationBeforeLost); }
    Y_UNIT_TEST(DuringGuidPublicationWithPersistedLost) { TestRestart(ERestartPoint::GuidPublicationWithLost); }
    Y_UNIT_TEST(GuidPublicationPreservesDbBirthLsn) { TestRestart(ERestartPoint::GuidPublicationWithFinal); }
    Y_UNIT_TEST(RollingWipeWithDelayedSelfGuid) { TestRollingWipe(true); }
    Y_UNIT_TEST(RollingWipeWithPreservedLocalState) { TestRollingWipe(false); }
    Y_UNIT_TEST(RollingWipeRejectsConflictingLegacyFinal) {
        TestRollingWipe(false, NKikimrBlobStorage::TSyncGuidInfo::Final);
    }
    Y_UNIT_TEST(RollingWipeRetainsIdentityOverTentativeGuid) {
        TestRollingWipe(false, NKikimrBlobStorage::TSyncGuidInfo::InProgress);
    }
}
