#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/bridge/proxy/bridge_proxy.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // holds events of the designated type sent to bridge proxies of one node, keeping the latest one of every proxy
    class TBridgeProxyEventHold {
        TEnvironmentSetup& Env;
        const ui32 NodeId;
        const ui32 EventType;
        bool Holding = false;
        std::map<TActorId, std::unique_ptr<IEventHandle>> Held;

    public:
        TBridgeProxyEventHold(TEnvironmentSetup& env, ui32 nodeId, ui32 eventType)
            : Env(env)
            , NodeId(nodeId)
            , EventType(eventType)
        {}

        void Start() {
            Holding = true;
        }

        size_t GetNumHeld() const {
            return Held.size();
        }

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (Holding && nodeId == NodeId && ev->GetTypeRewrite() == EventType) {
                if (IActor *actor = Env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                    Held[ev->GetRecipientRewrite()] = std::move(ev);
                    return false;
                }
            }
            return true;
        }

        void Release() {
            Holding = false;
            for (auto& [recipient, ev] : std::exchange(Held, {})) {
                Env.Runtime->Send(new IEventHandle(recipient, ev->Sender, ev->ReleaseBase().Release()), NodeId);
            }
        }
    };

    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    class TQueuedRoutingTest {
    public:
        TEnvironmentSetup Env{MakeTwoPileSettings()};
        NKikimrBlobStorage::TGroupInfo Group;
        TActorId Proxy;
        TActorId Edge;
        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> Config;
        TBridgeInfo::TPtr BridgeInfo;
        std::map<ui64, std::unique_ptr<TEvBlobStorage::TEvGetResult>> Results;
        bool RaceWithNewerPileGeneration = false;
        bool HoldResults = false;
        std::vector<std::unique_ptr<IEventHandle>> HeldResults;
        std::optional<TInstant> PileOutAt; // when the proxy got the first cluster state with a pile disconnected
        std::map<ui64, TInstant> ResultAt;

        TQueuedRoutingTest() {
            Env.CreatePool();
            Env.Sim(TDuration::Seconds(30));
            Group = GetGroupInfo(Env, Env.GetGroups().front());
            // Model group info arriving before the cluster state it depends on.
            Group.MutableBridgeGroupState()->SetClusterStateGeneration(
                QueryStorageConfig(Env).GetClusterState().GetGeneration() + 1);
            Edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetRecipientRewrite() == Proxy &&
                        ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                    Config = ev->Get<TEvNodeWardenStorageConfig>()->Config;
                    BridgeInfo = ev->Get<TEvNodeWardenStorageConfig>()->BridgeInfo;
                    if (!PileOutAt && BridgeInfo && std::ranges::any_of(BridgeInfo->Piles, [](const auto& pile) {
                            return pile.State == NKikimrBridge::TClusterState::DISCONNECTED;
                        })) {
                        PileOutAt = Env.Runtime->GetClock();
                    }
                } else if (HoldResults && ev->GetRecipientRewrite() == Proxy &&
                        ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    HeldResults.push_back(std::move(ev));
                    return false;
                } else if (RaceWithNewerPileGeneration && ev->GetRecipientRewrite() == Proxy &&
                        ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    MakeRace(*ev->Get<TEvBlobStorage::TEvGetResult>());
                } else if (ev->GetRecipientRewrite() == Edge && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    UNIT_ASSERT_C(!Results.contains(ev->Cookie), "duplicate terminal response");
                    ResultAt.emplace(ev->Cookie, Env.Runtime->GetClock());
                    Results.emplace(ev->Cookie, std::unique_ptr<TEvBlobStorage::TEvGetResult>(
                        ev->Release<TEvBlobStorage::TEvGetResult>().Release()));
                    return false;
                }
                return true;
            };
            Proxy = Env.Runtime->Register(CreateBridgeProxyActor(TBlobStorageGroupInfo::Parse(Group, nullptr, nullptr)), 1);
            WaitUntil(Env, [&] { return bool(Config); }, "proxy storage config", TDuration::MilliSeconds(10), 100);
        }

        ~TQueuedRoutingTest() {
            Env.Runtime->FilterFunction = {};
        }

        void MakeRace(TEvBlobStorage::TEvGetResult& msg) {
            msg.Status = NKikimrProto::RACE;
            for (const auto& pile : Group.GetBridgeGroupState().GetPile()) {
                if (pile.GetGroupId() == msg.GroupId) {
                    msg.RacingGeneration = pile.GetGroupGeneration() + 1;
                }
            }
        }

        void ReleaseResultsAsRace() {
            HoldResults = false;
            for (auto& ev : std::exchange(HeldResults, {})) {
                MakeRace(*ev->Get<TEvBlobStorage::TEvGetResult>());
                Env.Runtime->Send(ev.release(), 1);
            }
        }

        void SendGet(ui64 cookie, TInstant deadline = TInstant::Max()) {
            Env.Runtime->Send(new IEventHandle(Proxy, Edge, new TEvBlobStorage::TEvGet(
                TLogoBlobID(100500, 1, 1, 0, 10, 0), 0, 0, deadline, NKikimrBlobStorage::FastRead), 0, cookie), 1);
        }

        void RepeatConfig() {
            Env.Runtime->Send(new IEventHandle(Proxy, Edge, new TEvNodeWardenStorageConfig(Config, true, BridgeInfo)), 1);
        }

        void MakeRoutingAvailable() {
            Group.MutableBridgeGroupState()->SetClusterStateGeneration(Config->GetClusterState().GetGeneration());
            UpdateGroupInfo();
            RepeatConfig();
        }

        void UpdateGroupInfo() {
            Group.SetGroupGeneration(Group.GetGroupGeneration() + 1);
            Env.Runtime->Send(new IEventHandle(Proxy, Edge, new TEvBlobStorage::TEvConfigureProxy(
                TBlobStorageGroupInfo::Parse(Group, nullptr, nullptr), nullptr)), 1);
        }

        void CheckSuccess(ui64 cookie) {
            WaitUntil(Env, [&] { return Results.contains(cookie); }, "completed read", TDuration::MilliSeconds(10), 100);
            const auto& result = *Results.at(cookie);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result.ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Responses[0].Status, NKikimrProto::NODATA);
        }
    };

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRouting) {

    Y_UNIT_TEST(RestartLimitReturnsError) {
        TQueuedRoutingTest test;
        test.MakeRoutingAvailable();
        test.RaceWithNewerPileGeneration = true;
        auto request = std::make_unique<TEvBlobStorage::TEvGet>(TLogoBlobID(100500, 1, 1, 0, 10, 0),
            0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
        request->RestartCounter = 99; // the next RACE exhausts this request's retry budget
        test.Env.Runtime->Send(new IEventHandle(test.Proxy, test.Edge, request.release(), 0, 1), 1);
        WaitUntil(test.Env, [&] { return test.Results.contains(1); }, "restart limit error",
            TDuration::MilliSeconds(10), 100);
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(1)->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(test.Results.at(1)->ErrorReason, "too many restarts");
        test.RaceWithNewerPileGeneration = false;
        test.SendGet(2);
        test.CheckSuccess(2);
    }

    Y_UNIT_TEST(RoutingBudgetSurvivesConfigNotifications) {
        TQueuedRoutingTest test;
        test.SendGet(1);
        for (ui32 i = 0; i < 12; ++i) {
            test.Env.Sim(TDuration::MilliSeconds(250));
            test.RepeatConfig();
        }
        UNIT_ASSERT_C(test.Results.contains(1), "storage config notifications renewed the routing wait");
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(1)->Status, NKikimrProto::ERROR);
        test.MakeRoutingAvailable();
        test.SendGet(2);
        test.CheckSuccess(2);
        test.Env.Sim(TDuration::Seconds(3)); // expired work must not resume or reply twice
    }

    Y_UNIT_TEST(QueuedDeadlineOvertakesEarlierRequest) {
        TQueuedRoutingTest test;
        test.SendGet(1);
        test.Env.Sim(TDuration::MilliSeconds(10));
        test.SendGet(2, test.Env.Runtime->GetClock() + TDuration::MilliSeconds(100));
        test.Env.Sim(TDuration::MilliSeconds(200));
        UNIT_ASSERT_C(test.Results.contains(2), "queued read ignored its shorter caller deadline");
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(2)->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(!test.Results.contains(1));
        test.MakeRoutingAvailable();
        test.CheckSuccess(1);
        test.Env.Sim(TDuration::Seconds(3));
    }

    Y_UNIT_TEST(RoutingBudgetSurvivesGroupGenerationRetries) {
        TQueuedRoutingTest test;
        test.MakeRoutingAvailable();
        test.RaceWithNewerPileGeneration = true;
        test.SendGet(1);
        for (ui32 i = 0; i < 12; ++i) {
            test.Env.Sim(TDuration::MilliSeconds(250));
            test.UpdateGroupInfo(); // still behind the pile generation reported by RACE
        }
        UNIT_ASSERT_C(test.Results.contains(1), "group notifications renewed the routing wait");
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(1)->Status, NKikimrProto::ERROR);
        test.RaceWithNewerPileGeneration = false;
        test.SendGet(2);
        test.CheckSuccess(2);
        test.Env.Sim(TDuration::Seconds(3));
    }

    Y_UNIT_TEST(RoutingBudgetExcludesPileExecution) {
        TQueuedRoutingTest test;
        test.SendGet(1);
        test.Env.Sim(TDuration::MilliSeconds(1500));
        test.HoldResults = true;
        test.MakeRoutingAvailable();
        WaitUntil(test.Env, [&] { return !test.HeldResults.empty(); }, "dispatched read", TDuration::MilliSeconds(10), 100);
        test.Env.Sim(TDuration::Seconds(3)); // the piles take longer than the whole routing budget
        test.ReleaseResultsAsRace();
        test.Env.Sim(TDuration::MilliSeconds(300));
        UNIT_ASSERT_C(!test.Results.contains(1), "time spent in piles was charged to the routing wait");
        test.Env.Sim(TDuration::MilliSeconds(400));
        UNIT_ASSERT_C(test.Results.contains(1), "the routing wait was renewed by dispatch");
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(1)->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(test.Results.at(1)->ErrorReason, "bridge request timed out");
        test.SendGet(2);
        test.CheckSuccess(2);
        test.Env.Sim(TDuration::Seconds(3));
    }

    // the bridge proxy which knows the group state made for newer cluster state than it knows must not skip the pile
    // that takes no part in data quorum by its cluster state, as the pile may have got synced already
    Y_UNIT_TEST(StaleClusterStateWaits) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        constexpr ui32 nodeId = 2; // node of the pile staying connected; its bridge proxy keeps old cluster state
        TBridgeProxyEventHold hold(env, nodeId, TEvNodeWardenStorageConfig::EventType);
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            return hold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId); // start the bridge proxy on the node

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);

        hold.Start();
        RejoinPile(env, 1);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(hold.GetNumHeld()); // the proxy has not got the cluster state where the pile is synced

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "put completed with stale cluster state: " << res->Get()->ToString());

        hold.Release();
        res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());

        // the blob acknowledged after the pile got synced has to be in it
        const auto& [writtenGroupId, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroupId, groupId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[0], 1, id, data), NKikimrProto::OK);
        checker.CheckAll(nodeId);
    }

    // the bridge proxy which knows newer cluster state than the group state it has may find no pile to take the request;
    // it must wait for newer group state then
    Y_UNIT_TEST(NoEligiblePileWaits) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();

        constexpr ui32 nodeId = 2; // node of the pile staying connected; its bridge proxy keeps old group state
        TBridgeProxyEventHold hold(env, nodeId, TEvBlobStorage::TEvConfigureProxy::EventType);
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            return hold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId); // start the bridge proxy on the node

        // the proxy gets the group state where pile 1 is not synced
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);

        hold.Start();
        RejoinPile(env, 1);

        // pile 1 becomes primary, pile 0 (the only synced one in the group state of the proxy) stops taking part in data
        // quorum, while its nodes stay connected
        auto clusterState = QueryStorageConfig(env).GetClusterState();
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        clusterState.SetPerPileState(0, NKikimrBridge::TClusterState::SUSPENDED);
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        const auto switchResult = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
        });
        UNIT_ASSERT_EQUAL_C(switchResult.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            switchResult.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(hold.GetNumHeld()); // the proxy has not got the group state where pile 1 is synced

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "put completed with stale group state: " << res->Get()->ToString());

        hold.Release();
        res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        checker.CheckAll(nodeId);
    }

    // writers which don't know that the pile takes part in data quorum skip it; syncing the pile has to start with new
    // generations of its groups, so that such writers get refused by VDisks, both for dynamic and static groups
    Y_UNIT_TEST(SyncStartsAfterWritersFenced) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const ui32 dynamicPileGroupId = GetPileGroups(env, groupId).at(1);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));

        // generations of the parts in pile 1 as writers know them while the pile takes no part in data quorum
        THashMap<ui32, ui32> generations;
        const auto groupInfo = GetGroupInfo(env, groupId);
        for (const auto& pile : groupInfo.GetBridgeGroupState().GetPile()) {
            if (pile.GetGroupId() == dynamicPileGroupId) {
                generations[pile.GetGroupId()] = pile.GetGroupGeneration();
            }
        }
        const auto config = QueryStorageConfig(env);
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                const auto& pile = group.GetBridgeGroupState().GetPile(1);
                generations[pile.GetGroupId()] = pile.GetGroupGeneration();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(generations.size(), 2); // the dynamic group and the static one

        // the first assimilation request of the syncer of every part
        THashMap<ui32, ui32> syncerGenerations;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate) {
                for (const auto& [pileGroupId, generation] : generations) {
                    if (ev->Recipient == MakeBlobStorageProxyID(pileGroupId)) {
                        const auto *msg = ev->Get<TEvBlobStorage::TEvAssimilate>();
                        UNIT_ASSERT(msg->ForceGroupGeneration);
                        syncerGenerations.try_emplace(pileGroupId, *msg->ForceGroupGeneration);
                    }
                }
            }
            return true;
        };

        RejoinPile(env, 1);

        for (const auto& [pileGroupId, generation] : generations) {
            const auto it = syncerGenerations.find(pileGroupId);
            UNIT_ASSERT_C(it != syncerGenerations.end(), "no syncer for GroupId# " << pileGroupId);
            UNIT_ASSERT_C(generation < it->second, "syncer of GroupId# " << pileGroupId << " started with generation "
                << it->second << " writers skipping the pile know");
        }

        TDataChecker checker(env);
        checker.Write(groupId);
        checker.CheckAll();
    }


    // the bridge proxy started on the node doesn't get the storage config; requests to it are answered with error after
    // the configuration timeout, as the DS proxy does when it can't get configured, and served once the config comes
    Y_UNIT_TEST(NoStorageConfigFailsRequests) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const ui32 nodeId = 16;

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        std::vector<std::unique_ptr<IEventHandle>> held;
        bool hold = true;
        std::unique_ptr<IEventHandle> result;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            } else if (hold && recipientNodeId == nodeId && ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                    held.push_back(std::move(ev));
                    return false;
                }
            }
            return true;
        };
        auto get = [&] {
            result.reset();
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(TLogoBlobID(100500, 1, 1, 0, 1, 0), 0, 0,
                    TInstant::Max(), NKikimrBlobStorage::FastRead));
            });
            const TInstant start = env.Runtime->GetClock();
            WaitUntil(env, [&] { return !!result; }, "result", TDuration::MilliSeconds(100), 600);
            return std::make_tuple(result->Get<TEvBlobStorage::TEvGetResult>(), env.Runtime->GetClock() - start);
        };

        auto [res, duration] = get();
        UNIT_ASSERT(!held.empty()); // the proxy has been started without the config
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::ERROR, res->ToString());
        UNIT_ASSERT_C(duration <= ProxyConfigurationTimeout + TDuration::Seconds(2), duration);

        std::tie(res, duration) = get(); // the proxy doesn't wait any more
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::ERROR, res->ToString());
        UNIT_ASSERT_C(duration <= TDuration::Seconds(1), duration);

        hold = false;
        for (auto& ev : std::exchange(held, {})) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        std::tie(res, duration) = get();
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Status, NKikimrProto::NODATA);
    }


    // The request waiting for the part of the pile going out (its nodes are gone) completes when the proxy learns that
    // the pile takes no part in data quorum anymore, as the one admitted then would go to the primary pile only; the
    // error the part reports later doesn't fail it, nor makes another response.
    template<typename TResult>
    void CheckRequestInFlightCompletesWhenPileDisconnected(
            const std::function<void(TEnvironmentSetup&, TDataChecker&, ui32, TActorId)>& send,
            const std::function<void(TResult&)>& check) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);
        TDataChecker checker(env);
        checker.Write(groupId); // start the bridge proxy on node 1

        bool hold = true;
        std::vector<std::unique_ptr<IEventHandle>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (hold && nodeId == 1 && ev->GetTypeRewrite() == TResult::EventType &&
                    ev->Get<TResult>()->GroupId == pileGroups[1]) {
                held.push_back(std::move(ev));
                return false;
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        send(env, checker, groupId, edge);
        WaitUntil(env, [&] { return !held.empty(); }, "result of the second pile", TDuration::MilliSeconds(10), 100);
        auto res = env.WaitForEdgeActorEvent<TResult>(edge, false, env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "request completed without the second pile: " << res->Get()->ToString());

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        res = env.WaitForEdgeActorEvent<TResult>(edge, false, env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT_C(res, "request not completed after the pile has been disconnected");
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        check(*res->Get());

        hold = false;
        for (auto& ev : std::exchange(held, {})) { // the part gives up waiting for its VDisks
            auto *msg = ev->Get<TResult>();
            msg->Status = NKikimrProto::ERROR;
            msg->ErrorReason = "Timeout while establishing sessions";
            env.Runtime->Send(ev.release(), 1);
        }
        res = env.WaitForEdgeActorEvent<TResult>(edge, false, env.Runtime->GetClock() + TDuration::Seconds(10));
        UNIT_ASSERT_C(!res, "second response: " << res->Get()->ToString());
        env.Runtime->FilterFunction = {};
        checker.Write(groupId);
        checker.CheckAll();
    }

    Y_UNIT_TEST(PutInFlightCompletesWhenPileDisconnected) {
        CheckRequestInFlightCompletesWhenPileDisconnected<TEvBlobStorage::TEvPutResult>(
            [](TEnvironmentSetup&, TDataChecker& checker, ui32 groupId, TActorId edge) {
                checker.SendPut(groupId, edge);
            },
            [](TEvBlobStorage::TEvPutResult&) {});
    }

    // the request restarted as it waits for the pile disconnected is answered when the proxy stops right after that
    Y_UNIT_TEST(RestartedRequestAnsweredWhenProxyStops) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        TDataChecker checker(env);
        checker.Write(groupId); // start the bridge proxy on node 1

        std::optional<TActorId> proxyId;
        bool poisoned = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == 1 && ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult &&
                    ev->Get<TEvBlobStorage::TEvPutResult>()->GroupId == pileGroups[1]) {
                proxyId = ev->Recipient;
                return false; // the part of the second pile never answers
            } else if (proxyId && !poisoned && ev->GetRecipientRewrite() == *proxyId &&
                    ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                const auto& bridgeInfo = ev->Get<TEvNodeWardenStorageConfig>()->BridgeInfo;
                const auto *pile = bridgeInfo ? bridgeInfo->FindPile(TBridgePileId::FromPileIndex(1)) : nullptr;
                if (pile && pile->State == NKikimrBridge::TClusterState::DISCONNECTED) {
                    // the proxy is stopped right after it handles the cluster state making it restart the request
                    env.Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, *proxyId, {}, nullptr, 0), 1);
                    poisoned = true;
                }
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        WaitUntil(env, [&] { return proxyId.has_value(); }, "put of the second pile", TDuration::MilliSeconds(10), 100);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(poisoned);
        UNIT_ASSERT_C(res, "restarted request lost when the proxy stopped");
    }

    // the request restarted as it waits for the pile disconnected keeps the routing wait it has left
    Y_UNIT_TEST(RestartKeepsRoutingBudget) {
        TQueuedRoutingTest test;
        test.SendGet(1);
        test.Env.Sim(TDuration::MilliSeconds(1500)); // the read waits for the cluster state of its group state
        test.HoldResults = true;
        test.MakeRoutingAvailable();
        WaitUntil(test.Env, [&] { return test.HeldResults.size() == 2; }, "dispatched read",
            TDuration::MilliSeconds(10), 100);

        // the group state made for the cluster state newer than the one disconnecting the pile makes the restarted read
        // wait again
        const ui64 generation = test.Config->GetClusterState().GetGeneration();
        test.Group.MutableBridgeGroupState()->SetClusterStateGeneration(generation + 2);
        test.UpdateGroupInfo();
        test.Env.Sim(TDuration::MilliSeconds(10));
        UNIT_ASSERT(!test.Results.contains(1));
        SwitchPileState(test.Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitUntil(test.Env, [&] { return test.Results.contains(1); }, "read timed out", TDuration::MilliSeconds(10),
            300);

        // the read had waited for 1.5 s of 2 s when it was dispatched, so it is to wait for the remaining 0.5 s since
        // the proxy has restarted it: neither for the whole wait anew, nor for no time at all
        UNIT_ASSERT(test.PileOutAt);
        const TDuration waited = test.ResultAt.at(1) - *test.PileOutAt;
        UNIT_ASSERT_C(waited >= TDuration::MilliSeconds(300), "restarted read lost its routing wait: " << waited);
        UNIT_ASSERT_C(waited <= TDuration::MilliSeconds(700), "restarted read got a new routing wait: " << waited);
        UNIT_ASSERT_VALUES_EQUAL(test.Results.at(1)->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(test.Results.at(1)->ErrorReason, "bridge request timed out");
    }

    // a read of what the trusted pile has completes with its answer; the one proving the absence of a blob needs every
    // pile in data quorum
    Y_UNIT_TEST(GetInFlightCompletesWhenPileDisconnected) {
        const TLogoBlobID id(100500, 1, 1000000, 0, 10, 0); // never written
        CheckRequestInFlightCompletesWhenPileDisconnected<TEvBlobStorage::TEvGetResult>(
            [&](TEnvironmentSetup& env, TDataChecker&, ui32 groupId, TActorId edge) {
                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead));
                });
            },
            [&](TEvBlobStorage::TEvGetResult& res) {
                UNIT_ASSERT_VALUES_EQUAL(res.ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(res.Responses[0].Id, id);
                UNIT_ASSERT_VALUES_EQUAL_C(res.Responses[0].Status, NKikimrProto::NODATA, res.ToString());
            });
    }

    // The read with MustRestoreFirst has got the answer of the pile lacking the blob and waits for the pile having it,
    // when the former leaves data quorum. The blob is not restored into the pile out then (it gets synced when it is
    // back, and the write could only fail the read), and the read is answered with what the remaining pile has.
    Y_UNIT_TEST(RestoreSkipsPileDisconnected) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);
        TDataChecker checker(env);
        checker.Write(groupId); // start the bridge proxy on node 1

        // the blob only the first pile has
        const TString data = "restore me!";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroups[0], 1,
            new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL_C(put->Status, NKikimrProto::OK, put->ToString());

        bool hold = true;
        std::vector<std::unique_ptr<IEventHandle>> held;
        ui32 numGetsOfSecondPile = 0;
        ui32 numPutsToSecondPile = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId != 1) {
                return true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                const ui32 resultGroupId = ev->Get<TEvBlobStorage::TEvGetResult>()->GroupId;
                if (hold && resultGroupId == pileGroups[0]) {
                    held.push_back(std::move(ev));
                    return false;
                }
                numGetsOfSecondPile += resultGroupId == pileGroups[1];
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    ev->Get<TEvBlobStorage::TEvPut>()->Id == id) {
                ++numPutsToSecondPile; // the first pile has the blob, so any write of it is the restoring one
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        WaitUntil(env, [&] { return !held.empty() && numGetsOfSecondPile; }, "results of both piles",
            TDuration::MilliSeconds(10), 100);
        env.Sim(TDuration::Seconds(1)); // the proxy handles the answer of the second pile
        UNIT_ASSERT_VALUES_EQUAL(numPutsToSecondPile, 0);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(1));

        hold = false;
        for (auto& ev : std::exchange(held, {})) {
            env.Runtime->Send(ev.release(), 1);
        }
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(res, "read not completed");
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Responses[0].Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
        UNIT_ASSERT_VALUES_EQUAL_C(numPutsToSecondPile, 0, "the blob was restored into the pile out of data quorum");
    }

    // Discovery finds the blob in the pile which leaves data quorum before the last pile reports the blob missing. The
    // blob is not reread from the pile gone to be restored (it may never answer): the request starts over and finds the
    // blob in the pile still there.
    Y_UNIT_TEST(RestoreSourceDisconnected) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 3);
        TDataChecker checker(env);
        checker.Write(groupId); // start the bridge proxy on node 1

        // the blob the last pile lacks
        const ui64 tabletId = 200600; // not the one the checker writes blobs of
        const TString data = "restore me!";
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
        for (ui32 i : {0, 1}) {
            const auto put = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroups[i], 1,
                new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL_C(put->Status, NKikimrProto::OK, put->ToString());
        }

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto pileOf = [&](const IEventHandle& ev) -> std::optional<ui32> {
            for (ui32 i = 0; i < pileGroups.size(); ++i) {
                const TActorId proxyId = MakeBlobStorageProxyID(pileGroups[i]);
                if (ev.Recipient == proxyId || ev.GetRecipientRewrite() == proxyId) {
                    return i;
                }
            }
            return std::nullopt;
        };
        std::map<ui64, ui32> discoverPile; // subrequest cookie -> pile index
        std::map<ui32, std::vector<std::unique_ptr<IEventHandle>>> held; // pile index -> its results held
        std::set<ui32> holdPiles{0, 2}; // the second pile answers first and makes the source of restoration
        ui32 numResultsOfSecondPile = 0;
        bool disconnected = false;
        ui32 numGetsOfPileGone = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId != 1) {
                return true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvDiscover) {
                if (const auto pile = pileOf(*ev)) {
                    discoverPile[ev->Cookie] = *pile;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvDiscoverResult && ev->Recipient != edge) {
                if (const auto it = discoverPile.find(ev->Cookie); it != discoverPile.end()) {
                    if (holdPiles.contains(it->second)) {
                        held[it->second].push_back(std::move(ev));
                        return false;
                    }
                    numResultsOfSecondPile += it->second == 1;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && disconnected && pileOf(*ev) == 1u) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
                numGetsOfPileGone += msg->QuerySize == 1 && msg->Queries[0].Id == id;
            }
            return true;
        };
        auto release = [&](ui32 pile) {
            holdPiles.erase(pile);
            auto events = std::move(held[pile]);
            held.erase(pile);
            UNIT_ASSERT_C(!events.empty(), "no result held of pile " << pile);
            for (auto& ev : events) {
                env.Runtime->Send(ev.release(), 1);
            }
        };

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvDiscover(tabletId, 0, true, false, TInstant::Max(), 0,
                true));
        });
        WaitUntil(env, [&] { return numResultsOfSecondPile && held.contains(0) && held.contains(2); },
            "results of all piles", TDuration::MilliSeconds(10), 100);
        env.Sim(TDuration::Seconds(1)); // the proxy handles the answer of the second pile
        release(0);
        env.Sim(TDuration::Seconds(1));

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(1));
        disconnected = true;

        release(2);
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(10));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(res, "discovery not completed");
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Id, id);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Buffer, data);
        UNIT_ASSERT_VALUES_EQUAL_C(numGetsOfPileGone, 0, "the blob was reread from the pile out of data quorum");
        UNIT_ASSERT_EQUAL(ReadFromPileGroup(env, pileGroups[2], 1, id, data), NKikimrProto::OK); // restored
    }

}
