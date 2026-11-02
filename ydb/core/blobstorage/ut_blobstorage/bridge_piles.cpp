#include "bridge_config_yaml.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

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

    // remove the pile along with its nodes from the main config of the cluster
    NKikimrBlobStorage::TConfigResponse UpdateBridgeGroupInfo(TEnvironmentSetup& env,
            const NKikimrBlobStorage::TGroupInfo& group, const NKikimrBridge::TGroupState& state) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableUpdateBridgeGroupInfo();
        cmd->SetGroupId(group.GetGroupID());
        cmd->SetGroupGeneration(group.GetGroupGeneration());
        cmd->MutableBridgeGroupInfo()->MutableBridgeGroupState()->CopyFrom(state);
        return env.Invoke(request);
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgePiles) {

    // natively bridged cluster gets its pile registry persisted from the start
    Y_UNIT_TEST(PileRegistryPersisted) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.Sim(TDuration::Seconds(30));
        const auto config = QueryStorageConfig(env);
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT_VALUES_EQUAL(registry.PilesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(0).GetName(), "pile_1");
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(1).GetName(), "pile_2");
        UNIT_ASSERT(!registry.GetImplicitPileId());
    }

    // pile ids are bound to piles, wherever they are listed in BridgeConfig; piles being part of the cluster can't be
    // removed from the config
    Y_UNIT_TEST(PileListChangeRejected) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.Sim(TDuration::Seconds(30));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        const auto before = QueryStorageConfig(env);
        auto res = ReplaceMainConfig(env, [](YAML::Node& config) {
            YAML::Node piles = config["bridge_config"]["piles"];
            YAML::Node reordered(YAML::NodeType::Sequence);
            reordered.push_back(piles[1]);
            reordered.push_back(piles[0]);
            config["bridge_config"]["piles"] = reordered;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        const auto after = QueryStorageConfig(env);
        UNIT_ASSERT_LT(before.GetGeneration(), after.GetGeneration());
        // pile ids stay the same, but the change of their order makes the config not suitable for older versions
        auto registry = after.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT(registry.GetPileListChanged());
        registry.ClearPileListChanged();
        UNIT_ASSERT_VALUES_EQUAL(registry.DebugString(), before.GetClusterStateDetails().GetPileRegistry().DebugString());
        UNIT_ASSERT_VALUES_EQUAL(after.GetClusterState().GetGeneration(), before.GetClusterState().GetGeneration());

        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            for (YAML::Node pile : config["bridge_config"]["piles"]) {
                if (pile["name"].as<TString>() == "pile_2") {
                    pile["name"] = "pile_3";
                }
            }
            for (YAML::Node host : config["hosts"]) {
                if (host["location"]["bridge_pile_name"].as<TString>() == "pile_2") {
                    host["location"]["bridge_pile_name"] = "pile_3";
                }
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "pile 'pile_2' in state SYNCHRONIZED can't be removed");

        // bridged cluster can't lose its bridge config, its nodes would run without one after restart
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config.remove("bridge_config");
            for (YAML::Node host : config["hosts"]) {
                host["location"].remove("bridge_pile_name");
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "bridged cluster can't run without bridge config");

        // node of unknown pile would make every node fail to build its bridge info
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["hosts"][15]["location"]["bridge_pile_name"] = "pile_3";
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "unknown pile name 'pile_3' for node");
    }

    // pile added to BridgeConfig is INACTIVE: it has no resources and takes no part in the cluster, which keeps
    // working, even being restarted with the new config; such pile can be removed from the config, and the next one
    // added takes its slot, so adding and removing the same pile many times doesn't grow any structures
    Y_UNIT_TEST(InactivePileRegistration) {
        TEnvironmentSetup env(MakeExpandableSettings());
        TGroupInfoTracker tracker;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return tracker.Filter(nodeId, ev);
        };
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        auto waitForWorkingRoot = [&] {
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            for (;;) {
                const auto response = InvokeDistconf(env, [](auto& record) {
                    record.MutableQueryConfig();
                }, 1, deadline);
                if (response.GetStatus() == TResult::OK && response.HasScepter()) {
                    return;
                }
                UNIT_ASSERT_C(env.Runtime->GetClock() < deadline, response.DebugString());
                env.Sim(TDuration::Seconds(1));
            }
        };

        // INACTIVE pile has no part in static groups and state storage, and the nodes of active piles keep all VDisks
        auto checkInactivePile = [&] {
            const auto config = QueryStorageConfig(env);
            for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.HasBridgeGroupState()) {
                    UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeGroupState().PileSize(), 2);
                }
            }
            for (const auto *ss : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                    &config.GetSchemeBoardConfig()}) {
                for (const auto& ringGroup : ss->GetRingGroups()) {
                    UNIT_ASSERT_LT(TBridgePileId::FromProto(&ringGroup,
                        &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId).GetPileIndex(), 2);
                }
            }
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto& vslot : baseConfig.GetVSlot()) {
                UNIT_ASSERT_LT(vslot.GetVSlotId().GetNodeId(), FirstThirdPileNodeId);
            }
        };

        for (ui32 cycle = 0; cycle < 3; ++cycle) {
            Cerr << "*** cycle " << cycle << Endl;

            // the pile is added to the config of running cluster
            waitForWorkingRoot();
            auto res = AddThirdPileToConfig(env);
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            env.Sim(TDuration::Seconds(10));
            const auto added = QueryStorageConfig(env);
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(added), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");
            checkInactivePile();
            checker.Write(groupId);
            checker.CheckAll();

            // its state can't be changed, the pile has to be added to the cluster first
            res = InvokeDistconf(env, [&](auto& record) {
                auto *next = record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
                next->CopyFrom(added.GetClusterState());
                next->SetPerPileState(2, NKikimrBridge::TClusterState::DISCONNECTED);
                next->SetGeneration(next->GetGeneration() + 1);
            });
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is not added to the cluster");

            // nodes of the pile start with the new config, the nodes of other piles get restarted with it
            env.Runtime->GetAppDataBridgeConfig().AddPiles()->SetName("pile_3");
            env.ConfiguredNodeCount = LastThirdPileNodeId;
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                env.StartNode(nodeId);
            }
            env.Sim(TDuration::Seconds(30));
            for (ui32 nodeId : {1u, 2u, 9u, 10u}) {
                Cerr << "*** restarting node " << nodeId << " with three-pile config" << Endl;
                env.RestartNode(nodeId);
                env.Sim(TDuration::Seconds(30));
                checker.Write(groupId);
                checker.CheckAll();
            }

            // new groups are created in active piles only
            const ui64 storagePoolId = 2 + cycle;
            CreateAnotherPool(env, storagePoolId);
            env.Sim(TDuration::Seconds(30));
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto *group : GetPoolGroups(baseConfig, storagePoolId)) {
                if (group->GetIsProxyGroup()) {
                    checker.Write(group->GetGroupId()); // nodes get the info of bridge proxy group when using it
                    const auto& info = tracker.Get(group->GetGroupId());
                    UNIT_ASSERT_VALUES_EQUAL(info.GetBridgeGroupState().PileSize(), 2);
                }
            }
            checkInactivePile();
            checker.CheckAll();

            // INACTIVE pile is removed from the config along with its nodes; active piles can't be removed
            waitForWorkingRoot();
            res = DropPileFromConfig(env, "pile_2");
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "pile 'pile_2' in state SYNCHRONIZED can't be removed");
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                env.StopNode(nodeId);
            }
            waitForWorkingRoot();
            res = DropPileFromConfig(env, "pile_3");
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            env.Sim(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED");
            env.Runtime->GetAppDataBridgeConfig().MutablePiles()->RemoveLast();
            env.ConfiguredNodeCount = FirstThirdPileNodeId - 1;
            for (ui32 nodeId : {1u, 9u}) {
                env.RestartNode(nodeId);
                env.Sim(TDuration::Seconds(30));
            }
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    // nodes running older versions take pile ids from positions of piles in their BridgeConfig, so the list of piles
    // can be changed only when every node reports it supports that
    Y_UNIT_TEST(PileListChangeRequiresEveryNode) {
        TEnvironmentSetup env(MakeExpandableSettings());
        env.Sim(TDuration::Seconds(30));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        env.StopNode(16);
        env.Sim(TDuration::Seconds(10));
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "Some static nodes did not report their BridgeConfig");
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "NodeIds# 16");
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED");

        env.StartNode(16);
        env.Sim(TDuration::Seconds(30));
        res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // replacement of the config not touching the list of piles doesn't require that, even when positions of piles
        // in BridgeConfig don't match their ids anymore
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            YAML::Node piles = config["bridge_config"]["piles"];
            YAML::Node reordered(YAML::NodeType::Sequence);
            for (size_t i = piles.size(); i--; ) {
                reordered.push_back(piles[i]);
            }
            config["bridge_config"]["piles"] = reordered;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        env.StopNode(16);
        env.Sim(TDuration::Seconds(10));
        res = ReplaceMainConfig(env, [](YAML::Node& /*config*/) {});
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = DropPileFromConfig(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "NodeIds# 16");
    }

    // UpdateBridgeGroupInfo command must validate the new state against the current one and distribute the change
    Y_UNIT_TEST(UpdateBridgeGroupInfoValidation) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        TGroupInfoTracker tracker;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return tracker.Filter(nodeId, ev);
        };
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        const auto group = tracker.Get(groupId);
        const auto& state = group.GetBridgeGroupState();
        UNIT_ASSERT_VALUES_EQUAL(state.PileSize(), 2);

        // pile group can't be replaced
        auto wrong = state;
        wrong.MutablePile(0)->SetGroupId(state.GetPile(1).GetGroupId());
        wrong.MutablePile(0)->SetGroupGeneration(state.GetPile(1).GetGroupGeneration());
        auto response = UpdateBridgeGroupInfo(env, group, wrong);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "can't change group id");

        // stage change reaches nodes
        auto updated = state;
        updated.MutablePile(0)->SetStage(NKikimrBridge::TGroupState::WRITE_KEEP);
        response = UpdateBridgeGroupInfo(env, group, updated);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Sim(TDuration::Seconds(10));
        const auto changed = tracker.Get(groupId);
        UNIT_ASSERT_LT(group.GetGroupGeneration(), changed.GetGroupGeneration());
        UNIT_ASSERT_EQUAL(changed.GetBridgeGroupState().GetPile(0).GetStage(), NKikimrBridge::TGroupState::WRITE_KEEP);

        // return it back, the group keeps working
        auto restored = changed.GetBridgeGroupState();
        restored.MutablePile(0)->SetStage(NKikimrBridge::TGroupState::SYNCED);
        response = UpdateBridgeGroupInfo(env, changed, restored);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_EQUAL(tracker.Get(groupId).GetBridgeGroupState().GetPile(0).GetStage(),
            NKikimrBridge::TGroupState::SYNCED);
        checker.Write(groupId);
        checker.CheckAll();
    }


    // the DS proxy started before the node knew the group was bridged forwards requests to the bridge proxy it has put
    // in front of itself, and the bunch of puts delivered to it directly (as restarted puts are) goes there too
    Y_UNIT_TEST(ForwardingProxyPassesBunchOfEvents) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        // catch the DS proxy getting the bridged group info on the node which hasn't served the group yet
        const ui32 nodeId = 16;
        TActorId forwarderId;
        env.Runtime->FilterFunction = [&](ui32 recipientNodeId, std::unique_ptr<IEventHandle>& ev) {
            if (recipientNodeId == nodeId && ev->GetTypeRewrite() == TEvBlobStorage::EvConfigureProxy && ev->HasEvent()) {
                const auto& info = ev->Get<TEvBlobStorage::TEvConfigureProxy>()->Info;
                IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
                if (info && info->IsBridged() && info->GroupID == TGroupId::FromValue(groupId) && actor &&
                        TypeName(*actor).Contains("TBlobStorageGroupProxy")) {
                    forwarderId = ev->GetRecipientRewrite();
                }
            }
            return true;
        };
        TDataChecker checker(env);
        checker.Write(groupId, nodeId);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(forwarderId, "the DS proxy wasn't started for the bridged group");

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const TString data = "put of the bunch";
        auto bunch = std::make_unique<TEvBlobStorage::TEvBunchOfEvents>();
        std::vector<TLogoBlobID> ids;
        for (ui32 step = 1; step <= 2; ++step) {
            const TLogoBlobID id(100700, 1, step, 0, data.size(), 0);
            ids.push_back(id);
            bunch->Bunch.emplace_back(new IEventHandle(forwarderId, edge, new TEvBlobStorage::TEvPut(id, data,
                TInstant::Max()), 0, step));
        }
        std::vector<NKikimrProto::EReplyStatus> statuses;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge && ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult) {
                statuses.push_back(ev->Get<TEvBlobStorage::TEvPutResult>()->Status);
                return false;
            }
            return true;
        };
        env.Runtime->Send(new IEventHandle(forwarderId, edge, bunch.release()), nodeId);
        WaitUntil(env, [&] { return statuses.size() == ids.size(); }, "results of puts of the bunch",
            TDuration::Seconds(1), 60);
        env.Runtime->FilterFunction = {};
        for (const auto status : statuses) {
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
        }
        for (const auto& id : ids) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Responses[0].Status, NKikimrProto::OK, res->Get()->ToString());
        }
    }

}
