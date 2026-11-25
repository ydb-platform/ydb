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

    void SetStandbyBudget(TEnvironmentSetup& env, i64 bytes) {
        for (const ui32 nodeId : env.Runtime->GetNodes()) {
            TControlBoard::SetValue(bytes,
                env.Runtime->GetNode(nodeId)->AppData->Icb->DSProxyControls.BridgeStandbyMaxBytesInFlight);
        }
    }

    // the reports of Node Warden telling BSC that the part has missed a write
    ui32 CountStandbyMarks(IEventHandle& ev, ui32 targetGroupId) {
        ui32 num = 0;
        if (ev.GetTypeRewrite() == TEvBlobStorage::EvControllerUpdateSyncerState) {
            for (const auto& item : ev.Get<TEvBlobStorage::TEvControllerUpdateSyncerState>()->Record.GetStandbyDirty()) {
                num += item.GetTargetGroupId() == targetGroupId;
            }
        }
        return num;
    }

    TEnvironmentSetup::TSettings MakeSettings() {
        return MakeLocalPileFirstSettings(false);
    }

    using TGet = TEvBlobStorage::TEvGet;
    using TGetResult = TEvBlobStorage::TEvGetResult;

    std::unique_ptr<TGetResult> ReadFromStandbyNode(TEnvironmentSetup& env, ui32 groupId, std::unique_ptr<TGet> get) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(9, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] { SendToBSProxy(edge, groupId, get.release()); });
        auto res = env.WaitForEdgeActorEvent<TGetResult>(edge, true, env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT_C(res, "bridge read from standby node timed out");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->GroupId, groupId);
        return std::unique_ptr<TGetResult>(res->Release().Release());
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
        const auto writes = GetBridgeHandleCounters(GetBridgeRequestCounters(env, 1), "PutTabletLog", "256");
        const auto writtenBytes = writes->GetCounter("bridgeRequestBytes", true);
        const auto bytesBefore = writtenBytes->Val();
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
        UNIT_ASSERT_VALUES_EQUAL(writtenBytes->Val() - bytesBefore, data.size());
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
    }

    Y_UNIT_TEST(ErrorDoesNotAffectClient) { TestStandbyFailure(NKikimrProto::ERROR); }
    Y_UNIT_TEST(RaceDoesNotRestartClient) { TestStandbyFailure(NKikimrProto::RACE); }

    Y_UNIT_TEST(LocalStandbyReadsHitsAndFallsBackForMissingBlobs) {
        TEnvironmentSetup env(MakeLocalPileFirstSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        const TString localData = "standby";
        const TString remoteData = "required";
        const TLogoBlobID localId(100500, 1, 1, 0, localData.size(), 0);
        const TLogoBlobID remoteId(100500, 1, 2, 0, remoteData.size(), 0);
        const TLogoBlobID missingId(100500, 1, 3, 0, 1, 0);
        PutToPileGroup(env, piles[1], 9, localId, localData);
        PutToPileGroup(env, piles[0], 1, remoteId, remoteData, true);

        ui32 standbyReads = 0;
        bool delayedStandbyReply = false;
        TActorId standbyGetSender;
        std::vector<TLogoBlobID> requiredQueries;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent()) {
                const auto *get = ev->Get<TGet>();
                if (get->ForceGroupGeneration && get->Queries[0].Id.TabletID() == localId.TabletID()) {
                    if (ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                        UNIT_ASSERT_VALUES_EQUAL(get->Deadline, TInstant::Max());
                        ++standbyReads;
                        standbyGetSender = ev->Sender;
                    } else if (ev->Recipient == MakeBlobStorageProxyID(piles[0])) {
                        for (ui32 i = 0; i < get->QuerySize; ++i) {
                            requiredQueries.push_back(get->Queries[i].Id);
                        }
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent() &&
                    ev->GetRecipientRewrite() == standbyGetSender && ev->Get<TGetResult>()->GroupId == piles[1]) {
                auto *res = ev->Get<TGetResult>();
                for (ui32 i = 0; i < res->ResponseSz; ++i) {
                    auto& item = res->Responses[i];
                    if (item.Id == localId) {
                        item.Keep = res->ResponseSz == 1;
                        item.DoNotKeep = !item.Keep;
                    } else if (item.Id == remoteId) {
                        item.Status = NKikimrProto::ERROR;
                        item.DoNotKeep = true;
                    } else if (item.Id == missingId) {
                        item.Keep = true;
                    }
                }
                if (res->ResponseSz == 1 && !delayedStandbyReply) {
                    // The bridge must wait for this local hit even when the reply takes a few seconds.
                    delayedStandbyReply = true;
                    env.Runtime->Schedule(TDuration::Seconds(2), ev.release(), nullptr, nodeId);
                    return false;
                }
            }
            return true;
        };
        auto hit = ReadFromStandbyNode(env, groupId, std::make_unique<TGet>(localId, 0, 0, TInstant::Max(),
            NKikimrBlobStorage::FastRead));
        CheckGetResult(*hit, localData);
        UNIT_ASSERT(!hit->Responses[0].Keep);
        UNIT_ASSERT(!hit->Responses[0].DoNotKeep);
        UNIT_ASSERT(delayedStandbyReply);
        UNIT_ASSERT_VALUES_EQUAL(standbyReads, 1);
        UNIT_ASSERT(requiredQueries.empty());

        TArrayHolder<TGet::TQuery> queries(new TGet::TQuery[3]);
        queries[0].Set(remoteId, 1, 3);
        queries[1].Set(localId, 2, 4);
        queries[2].Set(missingId);
        auto res = ReadFromStandbyNode(env, groupId, std::make_unique<TGet>(queries, 3, TInstant::Max(),
            NKikimrBlobStorage::FastRead));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(standbyReads, 2);
        UNIT_ASSERT_VALUES_EQUAL(requiredQueries.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(requiredQueries[0], remoteId);
        UNIT_ASSERT_VALUES_EQUAL(requiredQueries[1], missingId);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 3);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Id, remoteId);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Buffer.ConvertToString(), remoteData.substr(1, 3));
        UNIT_ASSERT(res->Responses[0].Keep);
        UNIT_ASSERT(!res->Responses[0].DoNotKeep);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Id, localId);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Buffer.ConvertToString(), localData.substr(2, 4));
        UNIT_ASSERT(!res->Responses[1].Keep);
        UNIT_ASSERT(!res->Responses[1].DoNotKeep);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[2].Id, missingId);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[2].Status, NKikimrProto::NODATA);
        UNIT_ASSERT(!res->Responses[2].Keep);
        UNIT_ASSERT(!res->Responses[2].DoNotKeep);
    }

    Y_UNIT_TEST(LocalStandbyPartialHitCompletesWhenTrustedPileHasRemainingBlob) {
        TEnvironmentSetup env(MakeLocalPileFirstSettings(true, 3));
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        const TString localData = "standby";
        const TString remoteData = "required";
        const TLogoBlobID localId(100500, 1, 1, 0, localData.size(), 0);
        const TLogoBlobID remoteId(100500, 1, 2, 0, remoteData.size(), 0);
        PutToPileGroup(env, piles[1], 9, localId, localData);
        PutToPileGroup(env, piles[0], 1, remoteId, remoteData);

        std::unique_ptr<IEventHandle> held;
        ui32 heldNode = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->Recipient == MakeBlobStorageProxyID(piles[2])) {
                const auto *get = ev->Get<TGet>();
                UNIT_ASSERT_VALUES_EQUAL(get->QuerySize, 1);
                UNIT_ASSERT_VALUES_EQUAL(get->Queries[0].Id, remoteId);
                UNIT_ASSERT(!held);
                held = std::move(ev);
                heldNode = nodeId;
                return false;
            }
            return true;
        };
        TArrayHolder<TGet::TQuery> queries(new TGet::TQuery[2]);
        queries[0].Set(localId);
        queries[1].Set(remoteId);
        auto res = ReadFromStandbyNode(env, groupId, std::make_unique<TGet>(queries, 2, TInstant::Max(),
            NKikimrBlobStorage::FastRead));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(held);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 2);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Buffer.ConvertToString(), localData);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Buffer.ConvertToString(), remoteData);
        env.Runtime->Send(held.release(), heldNode);
        env.Sim(TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(LocalStandbyFallsBackOnFailure) {
        TEnvironmentSetup env(MakeLocalPileFirstSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        const TString data = "required";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, piles[0], 1, id, data);

        for (const auto failure : {NKikimrProto::ERROR, NKikimrProto::RACE, NKikimrProto::DEADLINE}) {
            ui32 standbyReads = 0;
            ui32 requiredReads = 0;
            env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent() && ev->Get<TGet>()->Queries[0].Id == id) {
                    if (ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                        ++standbyReads;
                        auto error = ev->Get<TGet>()->MakeErrorResponse(failure, "standby failure",
                            TGroupId::FromValue(piles[1]));
                        error->RacingGeneration = Max<ui32>();
                        env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, error.release(), 0, ev->Cookie), nodeId);
                        return false;
                    }
                    requiredReads += ev->Recipient == MakeBlobStorageProxyID(piles[0]) && ev->Get<TGet>()->ForceGroupGeneration;
                }
                return true;
            };
            auto res = ReadFromStandbyNode(env, groupId, std::make_unique<TGet>(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
            env.Runtime->FilterFunction = {};
            CheckGetResult(*res, data);
            UNIT_ASSERT_VALUES_EQUAL(standbyReads, 1);
            UNIT_ASSERT_VALUES_EQUAL(requiredReads, 1);
        }
    }

    Y_UNIT_TEST(LocalStandbySkipsMetadataAndFencedReads) {
        TEnvironmentSetup env(MakeLocalPileFirstSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        const TString data = "standby";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, piles[1], 9, id, data, true);

        ui32 standbyReads = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            standbyReads += ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent() &&
                ev->Recipient == MakeBlobStorageProxyID(piles[1]) && ev->Get<TGet>()->Queries[0].Id == id;
            return true;
        };
        auto check = [&](auto configure) {
            auto get = std::make_unique<TGet>(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
            configure(*get);
            auto res = ReadFromStandbyNode(env, groupId, std::move(get));
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[0].Status, NKikimrProto::NODATA, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(standbyReads, 0);
        };
        check([](TGet& get) { get.IsIndexOnly = true; });
        check([](TGet& get) { get.MustRestoreFirst = true; });
        check([](TGet& get) { get.Decommission = true; });
        check([&](TGet& get) { get.ReaderTabletData.emplace(id.TabletID(), 1); });
        check([&](TGet& get) { get.ForceBlockTabletData.emplace(id.TabletID(), 1); });
        check([&](TGet& get) { get.TabletId = id.TabletID(); get.AcquireBlockedGeneration = true; });
        env.Runtime->FilterFunction = {};
    }

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
        SetStandbyBudget(env, 0);
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
        SetStandbyBudget(env, 0);
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

    // An abandoned assignment must retain the missed write even with no new write or node registration to mark it again.
    Y_UNIT_TEST(ReconciliationRetriesAfterWorkerLoss) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2));

        ui32 lostNode = 0;
        ui64 lostAssignment = 0;
        ui32 replacementNode = 0;
        ui32 registrations = 0;
        ui32 newMarks = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (lostNode) {
                registrations += ev->GetTypeRewrite() == TEvBlobStorage::EvControllerRegisterNode;
                newMarks += CountStandbyMarks(*ev, piles[1]);
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                for (const auto& syncer : ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record.GetSyncers()) {
                    if (syncer.GetReconcile() && syncer.GetTargetGroupId() == piles[1]) {
                        if (!lostNode) {
                            lostNode = ev->Recipient.NodeId();
                            lostAssignment = syncer.GetAssignmentId();
                            UNIT_ASSERT(lostAssignment);
                            UNIT_ASSERT(syncer.GetThorough());
                            return false; // the worker dies before receiving its assignment
                        }
                        if (ev->Recipient.NodeId() == lostNode) {
                            return false; // don't deliver a repeated assignment before stopping the worker
                        }
                        UNIT_ASSERT_UNEQUAL(syncer.GetAssignmentId(), lostAssignment);
                        UNIT_ASSERT(syncer.GetThorough());
                        replacementNode = ev->Recipient.NodeId();
                    }
                }
            }
            return true;
        };
        SetStandbyBudget(env, 0);
        TDataChecker checker(env);
        checker.Write(groupId);
        SetStandbyBudget(env, 256 << 20);
        WaitUntil(env, [&] { return lostNode != 0; }, "reconciliation assigned", TDuration::Seconds(1), 90);
        UNIT_ASSERT(9 <= lostNode && lostNode <= 16);
        const ui32 readerNode = lostNode == 9 ? 10 : 9;
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], readerNode, id, data), NKikimrProto::NODATA);
        env.StopNode(lostNode);

        WaitUntil(env, [&] { return replacementNode != 0; }, "replacement reconciliation assigned",
            TDuration::Seconds(1), 30);
        WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], readerNode, id, data) == NKikimrProto::OK; },
            "replacement reconciler copies the missed write", TDuration::Seconds(1), 60);
        UNIT_ASSERT_VALUES_EQUAL(registrations, 0);
        UNIT_ASSERT_VALUES_EQUAL(newMarks, 0);
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::HOT_STANDBY);
        env.Runtime->FilterFunction = {};
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

    // The background write is dropped while the client write is still going to the synchronized pile. The part is
    // marked when the client write is over, as a reconciliation pass made before would not find the write in the source
    // either, and take the part for clean.
    Y_UNIT_TEST(LossIsReportedWhenClientWriteFinishes) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2)); // the reconciliation that started with the standby is over
        SetStandbyBudget(env, 0);
        std::unique_ptr<IEventHandle> requiredWrite;
        bool held = false;
        ui32 marks = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == MakeBlobStorageProxyID(piles[0]) &&
                    !std::exchange(held, true)) {
                requiredWrite = std::move(ev);
                return false;
            }
            marks += CountStandbyMarks(*ev, piles[1]);
            return true;
        };
        TDataChecker checker(env);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        env.Sim(TDuration::Minutes(2)); // a pass would have run by now
        UNIT_ASSERT(requiredWrite);
        UNIT_ASSERT_VALUES_EQUAL(marks, 0);

        // the DS proxy the write was held from may have been replaced meanwhile
        env.Runtime->Send(IEventHandle::Forward(std::move(requiredWrite), MakeBlobStorageProxyID(piles[0])).release(), 1);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        WaitUntil(env, [&] { return ReadFromPileGroup(env, piles[1], 9, id, data) == NKikimrProto::OK; },
            "reconciliation copies the dropped write");
        UNIT_ASSERT_LT(0, marks);
        env.Runtime->FilterFunction = {};

        RejoinPile(env, 1);
        CheckPileData(env, checker, 1, 9);
    }

    // The promotion of the part that is to be reconciled anyway waits for the seal too: the client write made before the
    // fence, whose background copy is lost, may still reach the source after the pass has scanned it.
    Y_UNIT_TEST(PromotionWaitsForClientWriteMissingStandby) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2));
        SetStandbyBudget(env, 0);
        TDataChecker checker(env);
        checker.Write(groupId); // the part is to be reconciled for this one
        std::unique_ptr<IEventHandle> requiredWrite;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->Recipient == MakeBlobStorageProxyID(piles[0]) &&
                    !requiredWrite) {
                requiredWrite = std::move(ev);
                return false;
            }
            return true;
        };
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(requiredWrite);
        SetStandbyBudget(env, 256 << 20);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        env.Sim(TDuration::Minutes(1)); // longer than the seal waits for the nodes it can't reach
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);

        env.Runtime->FilterFunction = {};
        // the DS proxy the write was held from may have been replaced meanwhile
        env.Runtime->Send(IEventHandle::Forward(std::move(requiredWrite), MakeBlobStorageProxyID(piles[0])).release(), 1);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        CheckPileData(env, checker, 1, 9);
    }

    // Once a flushed seal's client write finishes, it must progress without waiting for the background reply.
    Y_UNIT_TEST(FlushedSealProgressesAfterClientCompletion) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2));

        TActorId bridgeProxy;
        std::unique_ptr<IEventHandle> foregroundResult;
        std::unique_ptr<IEventHandle> backgroundResult;
        bool holdResults = true;
        bool flushed = false;
        bool sealed = false;
        ui64 sealCookie = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    ev->Recipient == MakeBlobStorageProxyID(piles[1])) {
                bridgeProxy = ev->Sender;
            }
            if (holdResults && ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->Recipient == bridgeProxy) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>();
                if (msg->GroupId == piles[0] || msg->GroupId == piles[1]) {
                    UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
                    auto& held = msg->GroupId == piles[0] ? foregroundResult : backgroundResult;
                    UNIT_ASSERT(!held);
                    held = std::move(ev);
                    return false;
                }
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenStandbySeal::EventType && ev->Recipient == bridgeProxy &&
                    ev->Get<NStorage::TEvNodeWardenStandbySeal>()->Flush) {
                flushed = true;
                sealCookie = ev->Cookie;
            }
            if (flushed && ev->GetTypeRewrite() == NStorage::TEvNodeWardenStandbySealed::EventType &&
                    ev->Sender == bridgeProxy && ev->Cookie == sealCookie) {
                sealed = true;
            }
            return true;
        };
        TDataChecker checker(env);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        WaitUntil(env, [&] { return foregroundResult && backgroundResult; }, "both write replies held",
            TDuration::Seconds(1), 10);
        holdResults = false;

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return flushed; }, "standby seal flushed", TDuration::Seconds(1), 40);
        UNIT_ASSERT(!sealed);
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);

        env.Runtime->Send(foregroundResult.release(), 1);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        WaitUntil(env, [&] { return sealed; }, "flushed seal completes with the client write",
            TDuration::MilliSeconds(100), 10);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        CheckPileData(env, checker, 1, 9);

        env.Runtime->FilterFunction = {};
        env.Runtime->Send(backgroundResult.release(), 1);
        WaitUntil(env, [&] { return GetStandbyCounter(env, "BytesInFlight", false) == 0; },
            "late background reply releases its budget", TDuration::Seconds(1), 10);
    }

    // The node BSC has lost may write to the standby without telling what it misses. The part reconciled since is not
    // taken for clean: the seal waits for the node, and then the part is reconciled after the fence.
    Y_UNIT_TEST(LostWriterIsReconciledAfterFence) {
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
        const ui32 lostNodeId = 8;
        env.StopNode(lostNodeId);
        env.Sim(TDuration::Minutes(2)); // the reconciliation passes made in standby are over

        ui32 scans = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            scans += ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate &&
                ev->Recipient == MakeBlobStorageProxyID(piles[1]);
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT_LT(0, scans);
        env.Runtime->FilterFunction = {};

        env.StartNode(lostNodeId);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId);
        CheckPileData(env, checker, 1, 9);
    }

    // An assignment list BSC has made before getting the result of a reconciliation pass doesn't start the pass again,
    // neither does the group configuration coming with it, as the result could be taken for the one of a later pass.
    Y_UNIT_TEST(FinishedPassIsNotRestarted) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2));

        enum { WaitAssignment, WaitFinish, HoldUpdate, Count } phase = WaitAssignment;
        ui32 nodeId = 0;
        NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate assignment; // the list starting the pass
        std::vector<NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate> held; // the ones made after its result
        ui32 scans = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvControllerNodeServiceSetUpdate: {
                    const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                    if (phase == WaitAssignment) {
                        for (const auto& syncer : record.GetSyncers()) {
                            if (syncer.GetReconcile() && syncer.GetTargetGroupId() == piles[1]) {
                                nodeId = ev->Recipient.NodeId();
                                assignment = record;
                                phase = WaitFinish;
                            }
                        }
                    } else if ((phase == HoldUpdate || phase == Count) && ev->Recipient.NodeId() == nodeId &&
                            ev->Sender) { // the lists the test delivers itself come with no sender
                        held.push_back(record);
                        phase = Count;
                        return false;
                    }
                    break;
                }
                case TEvBlobStorage::EvControllerUpdateSyncerState:
                    if (phase == WaitFinish && ev->Sender.NodeId() == nodeId) {
                        for (const auto& syncer : ev->Get<TEvBlobStorage::TEvControllerUpdateSyncerState>()->Record.GetSyncers()) {
                            if (syncer.GetFinished() && syncer.GetReconcile() && syncer.GetTargetGroupId() == piles[1]) {
                                phase = HoldUpdate;
                            }
                        }
                    }
                    break;

                case TEvBlobStorage::EvAssimilate:
                    // the scans of the parts of this group by a syncer of the node
                    scans += phase == Count && ev->Sender.NodeId() == nodeId &&
                        (ev->Recipient == MakeBlobStorageProxyID(piles[0]) ||
                            ev->Recipient == MakeBlobStorageProxyID(piles[1]));
                    break;
            }
            return true;
        };

        // the write dropped over the budget makes BSC start a pass
        SetStandbyBudget(env, 0);
        TDataChecker checker(env);
        checker.Write(groupId);
        SetStandbyBudget(env, 256 << 20);
        WaitUntil(env, [&] { return phase == Count; }, "reconciliation pass finishes");

        // the list made before BSC got the result comes after it; it carries the configuration of the group too, which
        // Node Warden applies before the list itself
        UNIT_ASSERT(std::ranges::any_of(assignment.GetServiceSet().GetGroups(), [&](const auto& group) {
            return group.GetGroupID() == groupId;
        }));
        auto deliver = [&](const NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate& record) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
            ev->Record = record;
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {}, ev.release()), nodeId);
        };
        deliver(assignment);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(scans, 0);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(!held.empty());
        for (const auto& record : held) {
            deliver(record);
        }

        const auto& [writtenGroup, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, id, data), NKikimrProto::OK);
        RejoinPile(env, 1);
        CheckPileData(env, checker, 1, 9);
    }

    // The bridge proxy started on the node before the seal comes is waited for until it registers with Node Warden, as
    // it may have been made with the group configuration older than the one of the seal.
    Y_UNIT_TEST(SealWaitsForProxyRegistration) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = PrepareStandby(env);
        const auto piles = GetPileGroups(env, groupId);
        env.Sim(TDuration::Minutes(2));

        // node 16 has served no request of the group yet, so its proxy starts with the first one
        const ui32 nodeId = 16;
        std::unique_ptr<IEventHandle> registration;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenRegisterBridgeProxy::EventType &&
                    ev->Sender.NodeId() == nodeId && !registration) {
                UNIT_ASSERT(ev->Get<NStorage::TEvNodeWardenRegisterBridgeProxy>()->Registered);
                registration = std::move(ev);
                return false;
            }
            return true;
        };
        TDataChecker checker(env);
        const auto edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT(registration);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);

        env.Runtime->FilterFunction = {};
        env.Runtime->Send(registration.release(), nodeId);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
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
