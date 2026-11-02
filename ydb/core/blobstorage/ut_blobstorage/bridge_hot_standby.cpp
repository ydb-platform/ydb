#include "bridge_pile_ops.h"

#include <ydb/core/base/counters.h>

using namespace NBridgeTest;

namespace {

    // the sum of the standby write counter of every node
    i64 GetStandbyCounter(TEnvironmentSetup& env, const TString& name, bool derivative = true) {
        i64 sum = 0;
        for (const ui32 nodeId : env.Runtime->GetNodes()) {
            sum += GetServiceCounters(env.Runtime->GetNode(nodeId)->AppData->Counters, "dsproxynode")
                ->GetSubgroup("subsystem", "bridge_standby")->GetCounter(name, derivative)->Val();
        }
        return sum;
    }

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

    // The DS proxy of the standby part lags behind the generation the write is forced to; the write is resent and
    // reaches the standby anyway.
    Y_UNIT_TEST(RaceWithLaggingProxyResendsWrite) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        const i64 restartedBefore = GetStandbyCounter(env, "WritesRestarted");
        ui32 races = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == MakeBlobStorageProxyID(piles[1]) &&
                    !races++) {
                auto result = ev->Get<TEvBlobStorage::TEvPut>()->MakeErrorResponse(NKikimrProto::RACE, "lagging proxy",
                    TGroupId::FromValue(piles[1]));
                result->RacingGeneration = 0;
                env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie), nodeId);
                return false;
            }
            return true;
        };
        TDataChecker checker(env);
        checker.Write(groupId);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
            "resent background write reaches standby");
        UNIT_ASSERT_VALUES_EQUAL(races, 2); // the RACE and the resent write
        UNIT_ASSERT_LT(restartedBefore, GetStandbyCounter(env, "WritesRestarted"));
        env.Runtime->FilterFunction = {};
    }

    // Writes beyond the node budget are dropped rather than held; the client doesn't notice, and the rejoin recovers them.
    Y_UNIT_TEST(OverBudgetWriteIsDropped) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        for (const ui32 nodeId : env.Runtime->GetNodes()) {
            TControlBoard::SetValue(0,
                env.Runtime->GetNode(nodeId)->AppData->Icb->DSProxyControls.BridgeStandbyMaxBytesInFlight);
        }
        const i64 droppedBefore = GetStandbyCounter(env, "WritesDropped");
        ui32 standbyWrites = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            standbyWrites += ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                ev->Recipient == MakeBlobStorageProxyID(piles[1]);
            return true;
        };
        TDataChecker checker(env);
        checker.Write(groupId);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        UNIT_ASSERT_VALUES_EQUAL(standbyWrites, 0);
        UNIT_ASSERT_LT(droppedBefore, GetStandbyCounter(env, "WritesDropped"));
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::NODATA);
        env.Runtime->FilterFunction = {};

        RejoinPile(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::OK);
    }

    // The write dropped over the budget gets to the standby by reconciliation, while the pile stays in HOT_STANDBY.
    Y_UNIT_TEST(ReconciliationCopiesDroppedWrite) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        for (const ui32 nodeId : env.Runtime->GetNodes()) {
            TControlBoard::SetValue(0,
                env.Runtime->GetNode(nodeId)->AppData->Icb->DSProxyControls.BridgeStandbyMaxBytesInFlight);
        }
        const i64 droppedBefore = GetStandbyCounter(env, "WritesDropped");
        TDataChecker checker(env);
        checker.Write(groupId);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        UNIT_ASSERT_LT(droppedBefore, GetStandbyCounter(env, "WritesDropped"));
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::NODATA);
        WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
            "reconciliation copies the dropped write");
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
    }

    // Reconciliation forwards the barrier held back by dsproxy-proxy only after the standby keeps what the source does:
    // the keep flag the standby has missed comes first, and the keep flag the source has released is released.
    Y_UNIT_TEST(ReconciliationCollectsGarbageAfterKeepFlags) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        TDataChecker checker(env);
        for (ui32 i = 0; i < 3; ++i) {
            checker.Write(groupId);
        }
        const auto& written = checker.GetWritten();
        const auto& [groupA, a, dataA] = written[0]; // kept, but its keep flag doesn't reach the standby
        const auto& [groupB, b, dataB] = written[1]; // not kept
        const auto& [groupC, c, dataC] = written[2]; // kept in the standby, and then released
        for (const auto& [group, id, data] : written) {
            UNIT_ASSERT_VALUES_EQUAL(group, groupId);
            WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
                "background write reaches standby");
        }

        ui32 keepsLost = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage &&
                    ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (msg->RecordGeneration == 1 && msg->Keep && std::ranges::find(*msg->Keep, a) != msg->Keep->end()) {
                    ++keepsLost; // the one of dsproxy-proxy, not of the reconciliation
                    return false;
                }
            }
            return true;
        };
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto collect = [&](ui32 counter, bool barrier, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(a.TabletID(), 1, counter, 0, barrier,
                    1, barrier ? 10 : 0, keep, doNotKeep, TInstant::Max(), false));
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        };
        collect(0, false, new TVector<TLogoBlobID>{c}, nullptr);
        collect(1, false, new TVector<TLogoBlobID>{a}, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(keepsLost, 1);
        collect(2, true, nullptr, new TVector<TLogoBlobID>{c});
        for (const auto& [group, id, data] : written) {
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::OK);
        }

        // the barrier takes effect when the VDisks of the group get it in sync
        WaitUntil(env, [&] {
            return ReadFromPileGroup(env, piles[0], 1, b, dataB) == NKikimrProto::NODATA &&
                ReadFromPileGroup(env, piles[0], 1, c, dataC) == NKikimrProto::NODATA;
        }, "source collects garbage", TDuration::Seconds(1), 60);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[0], 1, a, dataA), NKikimrProto::OK);

        WaitUntil(env, [&] {
            return ReadFromPileGroup(env, piles[1], 9, b, dataB) == NKikimrProto::NODATA &&
                ReadFromPileGroup(env, piles[1], 9, c, dataC) == NKikimrProto::NODATA;
        }, "reconciliation forwards the barrier");
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, a, dataA), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(keepsLost, 1);
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
        env.Runtime->FilterFunction = {};

        RejoinPile(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, a, dataA), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, b, dataB), NKikimrProto::NODATA);
    }

    // The standby having missed nothing since its last reconciliation is promoted without a scan of its parts, and the
    // rejoin doesn't make it skip writes in NOT_SYNCHRONIZED_1.
    Y_UNIT_TEST(CleanStandbyIsPromotedWithoutSync) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        TDataChecker checker(env);
        for (ui32 i = 0; i < 3; ++i) {
            checker.Write(groupId);
        }
        for (const auto& [group, id, data] : checker.GetWritten()) {
            WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
                "background write reaches standby");
        }
        env.Sim(TDuration::Minutes(2)); // the reconciliation that started with the standby is over

        ui32 scans = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            scans += ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate &&
                ev->Recipient == MakeBlobStorageProxyID(piles[1]);
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_UNEQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        checker.Write(groupId); // made while the part is being promoted
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT_VALUES_EQUAL(scans, 0);
        env.Runtime->FilterFunction = {};

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
