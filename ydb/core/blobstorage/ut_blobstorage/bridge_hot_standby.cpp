#include "bridge_pile_ops.h"

using namespace NBridgeTest;

namespace {

    TEnvironmentSetup::TSettings MakeSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    ui32 PrepareStandby(TEnvironmentSetup& env) {
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::HOT_STANDBY);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().PileSyncStateSize(), 1);
        for (const auto& group : config.GetStateStorageConfig().GetRingGroups()) {
            if (TBridgePileId::FromProto(&group, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId) ==
                    TBridgePileId::FromPileIndex(1)) {
                UNIT_ASSERT_EQUAL(group.GetPileState(), NKikimrConfig::TStateStorageConfig::DISCONNECTED);
            }
        }
        return env.GetGroups().front();
    }

    // Fail the standby request before the required pile gets its write. A standby failure must neither fail the
    // client nor restart its request, including a RACE that would normally wait for another group generation.
    void TestStandbyFailure(NKikimrProto::EReplyStatus status) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        std::unique_ptr<IEventHandle> requiredWrite;
        ui32 failures = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                if (ev->Recipient == MakeBlobStorageProxyID(piles[0])) {
                    UNIT_ASSERT(!requiredWrite);
                    requiredWrite = std::move(ev);
                    return false;
                } else if (ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                    ++failures;
                    auto result = ev->Get<TEvBlobStorage::TEvPut>()->MakeErrorResponse(status, "standby failure",
                        TGroupId::FromValue(piles[1]));
                    result->RacingGeneration = Max<ui32>();
                    env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie), nodeId);
                    return false;
                }
            }
            return true;
        };
        TDataChecker checker(env);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        const auto premature = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT(!premature);
        UNIT_ASSERT_VALUES_EQUAL(failures, 1);
        UNIT_ASSERT(requiredWrite);

        env.Runtime->FilterFunction = {};
        env.Runtime->Send(requiredWrite.release(), 1);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        checker.CheckAll();
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeHotStandby) {

    Y_UNIT_TEST(WritesCompleteBeforeStandbyAndReadsSkipIt) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        std::unique_ptr<IEventHandle> standbyWrite;
        TMessageRelevanceOwner relevance = std::make_shared<TMessageRelevanceTracker>();
        ui32 standbyReads = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == MakeBlobStorageProxyID(groupId)) {
                ev->Get<TEvBlobStorage::TEvPut>()->ExternalRelevanceWatcher = relevance;
            }
            if (ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    UNIT_ASSERT(!standbyWrite);
                    standbyWrite = std::move(ev);
                    return false;
                } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                    ++standbyReads;
                }
            }
            return true;
        };
        TDataChecker checker(env);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT(standbyWrite);
        checker.CheckAll();
        checker.CheckAll(9); // even a client in the standby reads from the synchronized pile
        UNIT_ASSERT_VALUES_EQUAL(standbyReads, 0);

        relevance.reset(); // the producer finishes while its background copy is still queued
        env.Runtime->FilterFunction = {};
        env.Runtime->Send(standbyWrite.release(), 1);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
            "background write reaches standby");
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
    }

    Y_UNIT_TEST(ErrorDoesNotAffectClient) { TestStandbyFailure(NKikimrProto::ERROR); }
    Y_UNIT_TEST(RaceDoesNotRestartClient) { TestStandbyFailure(NKikimrProto::RACE); }

    Y_UNIT_TEST(RejoinRecoversMissedWrites) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        ui32 dropped = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                ++dropped;
                return false;
            }
            return true;
        };
        TDataChecker checker(env);
        checker.Write(groupId);
        UNIT_ASSERT_VALUES_EQUAL(dropped, 1);
        env.Runtime->FilterFunction = {};
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::NODATA);
        env.Sim(TDuration::Seconds(60));
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
        ExpectError(TrySwitchPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED),
            "Can't switch to SYNCHRONIZED directly");
        ExpectError(TrySwitchPileState(env, 0, NKikimrBridge::TClusterState::HOT_STANDBY),
            "Incorrect primary pile state");

        RejoinPile(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::OK);
        checker.Write(groupId);
        CheckPileData(env, checker, 1, 9);
    }

    // A pile out of sync can't be suspended: the suspended pile would take part in configuration quorum and count its
    // state storage rings as synchronized ones.
    Y_UNIT_TEST(StandbyCannotBeSuspended) {
        TEnvironmentSetup env(MakeSettings());
        PrepareStandby(env);
        ExpectError(TrySwitchPileState(env, 1, NKikimrBridge::TClusterState::SUSPENDED),
            "only SYNCHRONIZED pile can be suspended");
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
    }

    Y_UNIT_TEST(CollectionForwardsOnlyKeepFlagsWithoutWaiting) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        TDataChecker checker(env);
        checker.Write(groupId);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        ui32 keeps = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage &&
                    ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                UNIT_ASSERT(!msg->Collect);
                UNIT_ASSERT(!msg->DoNotKeep);
                UNIT_ASSERT(msg->Keep);
                UNIT_ASSERT_VALUES_EQUAL(msg->Keep->size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Keep->front(), id);
                ++keeps;
                return false;
            }
            return true;
        };
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10,
                new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false));
        });
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(keeps, 1);
        env.Runtime->FilterFunction = {};
        RejoinPile(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::OK);
    }

}
