#include "bridge_pile_ops.h"

using namespace NBridgeTest;

namespace {

    void AddOverride(YAML::Node& config, const TString& pileName, bool staticGroup = true) {
        YAML::Node rule;
        rule["pile_name"] = pileName;
        rule["kind"].push_back("test");
        rule["static_group_override"] = staticGroup;
        rule["erasure_species"] = "block-4-2";
        rule["geometry"] = YAML::Node(YAML::NodeType::Map);
        config["blob_storage_config"]["storage_pool_pile_overrides"].push_back(rule);
    }

    void CheckErasure(TEnvironmentSetup& env, ui32 groupId, ui32 pileIndex,
            TBlobStorageGroupType::EErasureSpecies erasure) {
        const auto info = GetGroupInfo(env, groupId);
        const auto *part = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
        UNIT_ASSERT_C(part, info.DebugString());
        const auto physical = GetGroupInfo(env, part->GetGroupId());
        UNIT_ASSERT_VALUES_EQUAL_C(physical.GetErasureSpecies(), erasure, physical.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(physical.RingsSize(), erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1);
    }

} // namespace

Y_UNIT_TEST_SUITE(BridgePileOverrides) {
    Y_UNIT_TEST(ConvertAddGrowAndRepair) {
        const auto named = std::make_shared<bool>(false);
        auto settings = MakeConvertedClusterSettings(named);
        settings.Erasure = TBlobStorageGroupType::ErasureMirror3dc;
        settings.NodeCount = 18;
        settings.LocationGenerator = [named](ui32 nodeId) {
            if (nodeId > 9) {
                return GetConvertedClusterLocation(nodeId, true);
            }
            NActorsInterconnect::TNodeLocation location;
            GetUnbridgedLocation(nodeId).Serialize(&location, false);
            location.SetDataCenter(TStringBuilder() << "dc_" << (nodeId - 1) / 3);
            if (*named) {
                location.SetBridgePileName("pile_1");
            }
            return TNodeLocation(location);
        };
        TEnvironmentSetup env(settings);
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        checker.Write(0); // static data must also cross the erasure boundary
        NameConvertedClusterNodes(env, *named);
        auto result = ReplaceMainConfig(env, [&](YAML::Node& config) {
            YAML::Node pile;
            pile["name"] = "pile_2";
            config["bridge_config"]["piles"].push_back(pile);
            AddPileHosts(config, "pile_2", 10, 18);
            AddOverride(config, "pile_2");
        });
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 18);
        AddPileAndWait(env, "pile_2");
        RejoinPile(env, 1);
        for (ui32 logical : {0u, groupId}) {
            CheckErasure(env, logical, 0, TBlobStorageGroupType::ErasureMirror3dc);
            CheckErasure(env, logical, 1, TBlobStorageGroupType::Erasure4Plus2Block);
        }
        checker.CheckAll();
        CheckPileData(env, checker, 1, 10);

        // Resend the original base definition, as Console does when growing a pool.
        auto pool = ReadStoragePool(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetErasureSpecies(), "mirror-3-dc");
        pool.SetNumGroups(pool.GetNumGroups() + 1);
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetStoragePoolId() == 1 && GetGroupInfo(env, group.GetGroupId()).HasBridgeGroupState()) {
                CheckErasure(env, group.GetGroupId(), 0, TBlobStorageGroupType::ErasureMirror3dc);
                CheckErasure(env, group.GetGroupId(), 1, TBlobStorageGroupType::Erasure4Plus2Block);
                checker.Write(group.GetGroupId());
            }
        }
        const auto info = GetGroupInfo(env, groupId);
        ReassignOneDisk(env, info.GetBridgeGroupState().GetPile(1).GetGroupId());
        const auto staticConfig = QueryStorageConfig(env);
        const auto staticInfo = GetGroupInfo(env, 0);
        const ui32 staticPartId = staticInfo.GetBridgeGroupState().GetPile(1).GetGroupId();
        bool reassigned = false;
        for (const auto& vdisk : staticConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
            if (vdisk.GetVDiskID().GetGroupID() == staticPartId) {
                const auto result = InvokeDistconf(env, [&](auto& record) {
                    record.MutableReassignGroupDisk()->MutableVDiskId()->CopyFrom(vdisk.GetVDiskID());
                });
                UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                    result.GetErrorReason());
                reassigned = true;
                break;
            }
        }
        UNIT_ASSERT(reassigned);
        env.RestartNode(1);
        env.Sim(TDuration::Seconds(60));
        checker.CheckAll();
        CheckPileData(env, checker, 1, 10);

        // Removing the rules must not silently reinterpret live block-4-2 groups as mirror-3-dc.
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"].remove("storage_pool_pile_overrides");
        }), "existing static group");
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"]["storage_pool_pile_overrides"][0]["kind"] = YAML::Node(YAML::NodeType::Sequence);
        }), "existing GroupId");

        // Replace the original inline mirror-3-dc parts, retaining the logical group ids and data.
        auto state = QueryStorageConfig(env).GetClusterState();
        state.SetGeneration(state.GetGeneration() + 1);
        TBridgePileId::FromPileIndex(1).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPromotedPile);
        state.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        auto invoke = [&](const auto& fill) {
            const auto result = InvokeDistconf(env, fill, 10);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                result.GetErrorReason());
        };
        invoke([&](auto& record) { record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(state); });
        invoke([](auto& record) { record.MutableRemoveBridgePile()->SetPileName("pile_1"); });
        WaitPileOperationFinished(env, 10);
        checker.CheckAll(10);
        result = ReplaceMainConfig(env, [](YAML::Node& config) { AddOverride(config, "pile_1"); }, 10);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        invoke([](auto& record) { record.MutableAddBridgePile()->SetPileName("pile_1"); });
        WaitPileOperationFinished(env, 10);
        state = QueryStorageConfig(env, 10).GetClusterState();
        state.SetGeneration(state.GetGeneration() + 1);
        state.SetPerPileState(0, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        invoke([&](auto& record) { record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(state); });
        WaitUntil(env, [&] {
            return QueryStorageConfig(env, 10).GetClusterState().GetPerPileState(0) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        }, "recreated original pile synchronized");
        for (ui32 logical : {0u, groupId}) {
            CheckErasure(env, logical, 0, TBlobStorageGroupType::Erasure4Plus2Block);
            CheckErasure(env, logical, 1, TBlobStorageGroupType::Erasure4Plus2Block);
        }
        checker.CheckAll();
        CheckPileData(env, checker, 0, 1);
    }
}
