#include "bridge_pile_ops.h"

using namespace NBridgeTest;

namespace {

    void AddOverride(YAML::Node& config, std::initializer_list<TString> pileNames, bool staticGroup = true) {
        YAML::Node rule;
        for (const auto& pileName : pileNames) {
            rule["pile_name"].push_back(pileName);
        }
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
        TEnvironmentSetup env({
            .NodeCount = 27,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .LocationGenerator = [named](ui32 nodeId) {
                if (nodeId > 9) {
                    NActorsInterconnect::TNodeLocation location;
                    GetConvertedClusterLocation(nodeId, true).Serialize(&location, false);
                    location.SetBridgePileName(nodeId <= 18 ? "pile_2" : "pile_3");
                    return TNodeLocation(location);
                }
                NActorsInterconnect::TNodeLocation location;
                GetUnbridgedLocation(nodeId).Serialize(&location, false);
                location.SetDataCenter(TStringBuilder() << "dc_" << (nodeId - 1) / 3);
                if (*named) {
                    location.SetBridgePileName("pile_1");
                }
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 9,
        });
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        checker.Write(0); // static data must also cross the erasure boundary
        NameConvertedClusterNodes(env, *named);
        auto result = ReplaceMainConfig(env, [&](YAML::Node& config) {
            for (const auto *name : {"pile_2", "pile_3"}) {
                YAML::Node pile;
                pile["name"] = name;
                config["bridge_config"]["piles"].push_back(pile);
            }
            AddPileHosts(config, "pile_2", 10, 18);
            AddPileHosts(config, "pile_3", 19, 27);
            AddOverride(config, {"pile_2", "pile_3"});
        });
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"]["storage_pool_pile_overrides"][0]["pile_name"].push_back("unknown_pile");
        }), "unknown pile in storage pool pile override: unknown_pile");
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 18);
        AddPileAndWait(env, "pile_2");
        RejoinPile(env, 1);
        StartPileNodes(env, "pile_3", 19, 27);
        AddPileAndWait(env, "pile_3");
        RejoinPile(env, 2);
        for (ui32 logical : {0u, groupId}) {
            CheckErasure(env, logical, 0, TBlobStorageGroupType::ErasureMirror3dc);
            CheckErasure(env, logical, 1, TBlobStorageGroupType::Erasure4Plus2Block);
            CheckErasure(env, logical, 2, TBlobStorageGroupType::Erasure4Plus2Block);
        }
        checker.CheckAll();
        CheckPileData(env, checker, 1, 10);
        CheckPileData(env, checker, 2, 19);

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
                CheckErasure(env, group.GetGroupId(), 2, TBlobStorageGroupType::Erasure4Plus2Block);
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
                // A cleanup report can reach a root whose config still lists this generation as live.
                // It must be retried without changing the configuration or crashing the root.
                const auto report = InvokeDistconf(env, [&](auto& record) {
                    auto *cmd = record.MutableStaticVDiskSlain();
                    cmd->MutableVDiskId()->CopyFrom(vdisk.GetVDiskID());
                    const auto& location = vdisk.GetVDiskLocation();
                    auto *slot = cmd->MutableVSlotId();
                    slot->SetNodeId(location.GetNodeID());
                    slot->SetPDiskId(location.GetPDiskID());
                    slot->SetVSlotId(location.GetVDiskSlotID());
                });
                UNIT_ASSERT_EQUAL_C(report.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE,
                    report.GetErrorReason());
                UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetGeneration(), staticConfig.GetGeneration());
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
        CheckPileData(env, checker, 2, 19);

        // Removing the rules must not silently reinterpret live block-4-2 groups as mirror-3-dc.
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"].remove("storage_pool_pile_overrides");
        }), "existing GroupId");
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"]["storage_pool_pile_overrides"][0]["static_group_override"] = false;
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
        result = ReplaceMainConfig(env, [](YAML::Node& config) {
            AddOverride(config, {"pile_1"});
            // The original pile still spans three DCs, with only three nodes in each. For its replacement,
            // use racks as fail domains without constraining the single fail realm to one datacenter.
            auto geometry = config["blob_storage_config"]["storage_pool_pile_overrides"][1]["geometry"];
            geometry["domain_level_begin"] = 10;
            geometry["domain_level_end"] = 40;
        }, 10);
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
            CheckErasure(env, logical, 2, TBlobStorageGroupType::Erasure4Plus2Block);
        }
        checker.CheckAll();
        CheckPileData(env, checker, 0, 1);
    }
}
