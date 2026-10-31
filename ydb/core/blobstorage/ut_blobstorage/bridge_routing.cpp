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
                } else if (RaceWithNewerPileGeneration && ev->GetRecipientRewrite() == Proxy &&
                        ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                    msg->Status = NKikimrProto::RACE;
                    for (const auto& pile : Group.GetBridgeGroupState().GetPile()) {
                        if (pile.GetGroupId() == msg->GroupId) {
                            msg->RacingGeneration = pile.GetGroupGeneration() + 1;
                        }
                    }
                } else if (ev->GetRecipientRewrite() == Edge && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    UNIT_ASSERT_C(!Results.contains(ev->Cookie), "duplicate terminal response");
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

}
