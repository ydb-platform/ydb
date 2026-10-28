#include "bridge_config_yaml.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

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

    // pile ids are bound to the list of piles of the bridged cluster, so the list can't be changed arbitrarily
    Y_UNIT_TEST(PileListChangeRejected) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.Sim(TDuration::Seconds(30));
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        auto res = ReplaceMainConfig(env, [](YAML::Node& config) {
            YAML::Node piles = config["bridge_config"]["piles"];
            YAML::Node reordered(YAML::NodeType::Sequence);
            reordered.push_back(piles[1]);
            reordered.push_back(piles[0]);
            config["bridge_config"]["piles"] = reordered;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "order of bridge piles can't be changed");

        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["bridge_config"]["piles"][1]["name"] = "pile_3";
            for (YAML::Node host : config["hosts"]) {
                if (host["location"]["bridge_pile_name"].as<TString>() == "pile_2") {
                    host["location"]["bridge_pile_name"] = "pile_3";
                }
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "list of bridge piles can't be changed");

        // bridged cluster can't lose its bridge config, its nodes would run without one after restart
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config.remove("bridge_config");
            for (YAML::Node host : config["hosts"]) {
                host["location"].remove("bridge_pile_name");
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "list of bridge piles can't be changed");

        // node of unknown pile would make every node fail to build its bridge info
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["hosts"][15]["location"]["bridge_pile_name"] = "pile_3";
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "unknown pile name 'pile_3' for node");
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

}
