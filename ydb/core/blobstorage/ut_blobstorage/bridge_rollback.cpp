#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/utility.h>

#include <algorithm>
#include <map>
#include <set>
#include <ranges>

using namespace NBridgeTest;

namespace {

    // pile of every node known to Hive (raw pile id, 0 when the node has none)
    std::map<ui32, ui32> GetHiveNodePiles(TEnvironmentSetup& env) {
        const ui32 nodeId = env.Settings.ControllerNodeId;
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const TActorId clientId = env.Runtime->Register(NTabletPipe::CreateClient(edge, MakeDefaultHiveID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()), nodeId);
        {
            auto res = env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
        env.Runtime->WrapInActorContext(edge, [&] {
            NTabletPipe::SendData(edge, clientId, new TEvHive::TEvRequestHiveNodeStats);
        });
        auto res = env.WaitForEdgeActorEvent<TEvHive::TEvResponseHiveNodeStats>(edge, false);
        env.Runtime->WrapInActorContext(edge, [&] {
            NTabletPipe::CloseClient(TActivationContext::AsActorContext(), clientId);
        });
        env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientDestroyed>(edge, false); // edge actors abort on unexpected ones
        std::map<ui32, ui32> piles;
        for (const auto& node : res->Get()->Record.GetNodeStats()) {
            piles.emplace(node.GetNodeId(), node.GetBridgePileId());
        }
        return piles;
    }

    void CheckHiveNodePiles(TEnvironmentSetup& env, ui32 expectedPileId) {
        const auto piles = GetHiveNodePiles(env);
        UNIT_ASSERT(!piles.empty());
        for (const auto& [nodeId, pileId] : piles) {
            UNIT_ASSERT_VALUES_EQUAL_C(pileId, expectedPileId, "NodeId# " << nodeId);
        }
    }

    struct TRollbackStatus {
        bool HasClusterState = false;
        std::set<ui32> BridgeConfigNodeIds;
        std::set<ui32> UnreportedStaticNodeIds;
    };

    // rollback status as distconf reports it (what the GetRollbackStatus RPC replies with)
    TRollbackStatus QueryRollbackStatus(TEnvironmentSetup& env) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            const auto res = InvokeDistconf(env, [](auto& record) { record.MutableQueryBridgeRollbackStatus(); });
            if (res.GetStatus() == TResult::OK) {
                UNIT_ASSERT_C(res.HasBridgeRollbackStatus(), res.DebugString());
                const auto& status = res.GetBridgeRollbackStatus();
                return {
                    .HasClusterState = status.GetHasClusterState(),
                    .BridgeConfigNodeIds = std::set<ui32>(status.GetBridgeConfigNodeIds().begin(),
                        status.GetBridgeConfigNodeIds().end()),
                    .UnreportedStaticNodeIds = std::set<ui32>(status.GetUnreportedStaticNodeIds().begin(),
                        status.GetUnreportedStaticNodeIds().end()),
                };
            }
            UNIT_ASSERT_C(res.GetStatus() == TResult::RACE && env.Runtime->GetClock() < deadline, res.DebugString());
            env.Sim(TDuration::MilliSeconds(10));
        }
    }

    void CheckRollbackStatus(TEnvironmentSetup& env, const std::set<ui32>& bridgeConfigNodeIds,
            const std::set<ui32>& unreportedStaticNodeIds = {}, bool hasClusterState = false) {
        const auto status = QueryRollbackStatus(env);
        UNIT_ASSERT_VALUES_EQUAL(status.HasClusterState, hasClusterState);
        UNIT_ASSERT_EQUAL_C(status.BridgeConfigNodeIds, bridgeConfigNodeIds,
            "BridgeConfigNodeIds# " << FormatList(status.BridgeConfigNodeIds));
        UNIT_ASSERT_EQUAL_C(status.UnreportedStaticNodeIds, unreportedStaticNodeIds,
            "UnreportedStaticNodeIds# " << FormatList(status.UnreportedStaticNodeIds));
    }

    std::set<ui32> GetAllNodeIds(TEnvironmentSetup& env) {
        std::set<ui32> res;
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            if (nodeId <= env.ConfiguredNodeCount) { // the rest are not started
                res.insert(nodeId);
            }
        }
        return res;
    }

    // restart nodes without bridge config one by one, the node hosting Hive first
    void RollingRestartWithoutBridgeConfig(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId,
            const std::function<void(ui32)>& afterRestart = {}) {
        env.Runtime->GetAppDataBridgeConfig().ClearPiles();
        std::vector<ui32> nodeIds{env.Settings.ControllerNodeId};
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            if (nodeId != env.Settings.ControllerNodeId) {
                nodeIds.push_back(nodeId);
            }
        }
        for (ui32 nodeId : nodeIds) {
            Cerr << "*** restarting node " << nodeId << " without bridge config" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
            if (afterRestart) {
                afterRestart(nodeId);
            }
        }
    }

    // prepare the rolled back cluster for bridge mode again and convert it
    void ConvertClusterAgain(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId) {
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);
        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT(config.HasClusterState());
        UNIT_ASSERT(!config.HasBridgeRollback());
        UNIT_ASSERT(!config.GetClusterStateDetails().GetBscConversionPending());
        UNIT_ASSERT_VALUES_UNEQUAL(config.GetClusterStateDetails().GetNextStaticGroupId(), 0);
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRollback) {

    // the cluster prepared for bridge mode (every node runs with single-pile bridge config, the cluster is not converted)
    // gets back to ordinary mode by restarting nodes without bridge config; Hive forgets the piles of nodes, both when
    // it loads (it is started on the node without bridge config first) and when nodes still running with bridge config
    // register with it, so that they don't get into a balancing segment other than the one of the nodes registered later
    Y_UNIT_TEST(PreparedClusterBackToOrdinary) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        CheckHiveNodePiles(env, 0);
        CheckRollbackStatus(env, {});

        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);
        const ui32 implicitPileId = TBridgePileId::FromPileIndex(0).GetLocalDb(); // the value reported in protobufs
        CheckHiveNodePiles(env, implicitPileId);
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());
        auto bridgeConfigNodeIds = GetAllNodeIds(env);
        CheckRollbackStatus(env, bridgeConfigNodeIds);

        RollingRestartWithoutBridgeConfig(env, checker, groupId, [&](ui32 nodeId) {
            CheckHiveNodePiles(env, 0); // the node hosting Hive has been restarted first
            bridgeConfigNodeIds.erase(nodeId);
            CheckRollbackStatus(env, bridgeConfigNodeIds);
        });
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());
        CheckRollbackStatus(env, {});

        // Hive loads in ordinary mode again
        env.RestartNode(env.Settings.ControllerNodeId);
        env.Sim(TDuration::Seconds(30));
        CheckHiveNodePiles(env, 0);
        checker.Write(groupId);
        checker.CheckAll();

        // the cluster can be prepared for bridge mode again, and converted
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);
        CheckHiveNodePiles(env, implicitPileId);
        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(QueryStorageConfig(env).HasClusterState());
        CheckRollbackStatus(env, GetAllNodeIds(env), {}, true);
        checker.Write(groupId);
        checker.CheckAll();
    }

    // only some nodes have been restarted with bridge config when the preparation is rolled back
    Y_UNIT_TEST(PartiallyPreparedClusterBackToOrdinary) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId, std::vector<ui32>{
            env.Settings.ControllerNodeId, 2, 3});
        const auto piles = GetHiveNodePiles(env);
        UNIT_ASSERT(std::ranges::all_of(piles | std::views::values, [](ui32 pileId) { return pileId != 0; }));
        CheckRollbackStatus(env, {env.Settings.ControllerNodeId, 2, 3});

        RollingRestartWithoutBridgeConfig(env, checker, groupId);
        CheckHiveNodePiles(env, 0);
        CheckRollbackStatus(env, {});

        // static node not running can't be confirmed to run without bridge config
        constexpr ui32 stoppedNodeId = 5;
        env.StopNode(stoppedNodeId);
        env.Sim(TDuration::Seconds(30));
        CheckRollbackStatus(env, {}, {stoppedNodeId});
        env.StartNode(stoppedNodeId);
        env.Sim(TDuration::Seconds(30));
        CheckRollbackStatus(env, {});
    }

    // the converted cluster is rolled back: static groups become ordinary ones, BSC makes its pools and groups such, the
    // cluster state goes away; running nodes keep serving the groups (their bridge proxies pass requests through), and
    // then they are restarted without bridge config; the cluster can be converted again afterwards
    Y_UNIT_TEST(ConvertedClusterBackToOrdinary) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 2);
        CreateAnotherPool(env, 2); // created bridged, inline in the original pile
        env.Sim(TDuration::Seconds(30));
        std::vector<ui32> groupIds;
        for (ui32 id : env.GetGroups()) {
            groupIds.push_back(id);
        }
        {
            const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
            for (const auto *group : GetPoolGroups(baseConfig, 2)) {
                groupIds.push_back(group->GetGroupId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groupIds.size(), 3);
        for (ui32 id : groupIds) {
            CheckInlinePileGroup(env, id);
        }
        const ui32 staticGroupId = 0;
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, staticGroupId).GetBridgeProxyGroupId(), staticGroupId);
        const std::vector<ui32> dynamicGroupIds = groupIds;
        groupIds.push_back(staticGroupId);
        for (ui32 id : groupIds) {
            checker.Write(id);
        }
        checker.CheckAll();

        // static groups are ordinary ones as soon as the rollback is accepted
        auto res = InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto accepted = QueryStorageConfig(env); // a temporary would be gone before the loop body (pre-C++23)
        for (const auto& group : accepted.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            UNIT_ASSERT_C(!group.HasBridgeGroupState() && !group.HasBridgeProxyGroupId() && !group.HasBridgePileId(),
                group.ShortDebugString());
        }
        WaitUntil(env, [&] { return !QueryStorageConfig(env).HasClusterState(); }, "cluster state removed",
            [&] { return QueryStorageConfig(env).GetClusterStateDetails().ShortDebugString(); });

        // nothing bridged is left in the storage config and in BSC
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT(!config.HasClusterStateDetails());
        UNIT_ASSERT_C(config.HasBridgeRollback(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_UNEQUAL(config.GetBridgeRollback().GetNextStaticGroupId(), 0);
        for (const auto *pb : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                &config.GetSchemeBoardConfig()}) {
            for (const auto& ringGroup : pb->GetRingGroups()) {
                UNIT_ASSERT_C(!ringGroup.HasBridgePileId(), ringGroup.ShortDebugString());
            }
        }
        for (ui32 id : groupIds) {
            const auto info = GetGroupInfo(env, id);
            UNIT_ASSERT_C(!info.HasBridgeGroupState() && !info.HasBridgeProxyGroupId() && !info.HasBridgePileId(),
                info.ShortDebugString());
            UNIT_ASSERT_C(info.RingsSize(), info.ShortDebugString());
        }
        for (ui64 storagePoolId : {1, 2}) {
            UNIT_ASSERT(!ReadStoragePool(env, storagePoolId).GetBridgeMode());
        }

        // running nodes serve the groups as ordinary ones, the data is in place
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            for (ui32 id : groupIds) {
                checker.Write(id, nodeId);
            }
            checker.CheckAll(nodeId);
        }
        ReassignOneDisk(env, groupId); // self-heal works for the group made ordinary
        env.Sim(TDuration::Minutes(1));
        checker.CheckAll();

        // the cluster is prepared for bridge mode now, the rest of the rollback is restarting nodes without bridge config
        CheckRollbackStatus(env, GetAllNodeIds(env));
        RollingRestartWithoutBridgeConfig(env, checker, groupId);
        CheckRollbackStatus(env, {});
        CheckHiveNodePiles(env, 0);
        for (ui32 id : groupIds) {
            checker.Write(id);
        }
        checker.CheckAll();

        // the cluster can be converted again, static group ids are not reused
        ConvertClusterAgain(env, checker, groupId);
        for (ui32 id : dynamicGroupIds) {
            CheckInlinePileGroup(env, id);
        }
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, staticGroupId).GetBridgeProxyGroupId(), staticGroupId);
        for (ui32 id : groupIds) {
            checker.Write(id);
        }
        checker.CheckAll();
    }

    // the cluster bridged with several piles from the very beginning has no original pile to get back to
    Y_UNIT_TEST(NativeBridgedClusterRefused) {
        TEnvironmentSetup env({
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        });
        const auto res = InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            res.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "no original pile");
        UNIT_ASSERT(QueryStorageConfig(env).HasClusterState());
    }

    // the ordinary cluster has nothing to roll back
    Y_UNIT_TEST(OrdinaryClusterRefused) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        RollingRestartWithSinglePileBridgeConfig(env, checker, env.GetGroups().front());
        const auto res = InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            res.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "not converted to bridged one");
    }

}
