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

}
