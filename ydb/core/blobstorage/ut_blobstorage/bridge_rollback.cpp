#include "bridge_pile_ops.h"

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
            const auto res = InvokeDistconf(env, [](auto& record) { record.MutableQueryBridgeRollbackStatus(); }, 1,
                deadline);
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
        for (ui32 nodeId : GetAllNodeIds(env)) {
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

    // the notification of BSC about the finished conversion delivered after the cluster has been rolled back and
    // converted again doesn't finish the new conversion; BSC of older version reports no identity of the conversion
    void CheckStaleConversionNotificationIgnored(bool olderBsc) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        // the notification of the first conversion stays undelivered, then the reply to it is held
        bool holdNotification = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> notifications;
        std::optional<TActorId> bscId;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> replies;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (holdNotification && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType &&
                    ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBscConversionFinished()) {
                notifications.emplace_back(nodeId, std::move(ev));
                return false;
            } else if (bscId && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType &&
                    ev->Recipient == *bscId) {
                replies.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        };
        auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitUntil(env, [&] { return !notifications.empty(); }, "notification of BSC");
        UNIT_ASSERT_VALUES_EQUAL(notifications.size(), 1);
        CheckInlinePileGroup(env, groupId);
        UNIT_ASSERT(QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        holdNotification = false;
        const ui64 clusterStateGeneration = QueryStorageConfig(env).GetClusterState().GetGeneration();

        // the conversion is rolled back, and the cluster is converted again
        res = InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitUntil(env, [&] { return !QueryStorageConfig(env).HasClusterState(); }, "cluster state removed");
        res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        // generations of the cluster state go on from the ones of the cluster rolled back
        UNIT_ASSERT_GT(QueryStorageConfig(env).GetClusterState().GetGeneration(), clusterStateGeneration);

        // the late notification of the first conversion leaves the second one pending
        bscId.emplace(notifications.front().second->Sender);
        for (auto& [nodeId, ev] : notifications) {
            auto& record = *ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.MutableNotifyBscConversionFinished();
            UNIT_ASSERT(record.GetBscConversionId());
            if (olderBsc) {
                record.ClearBscConversionId();
            }
        }
        for (auto& [nodeId, ev] : std::exchange(notifications, {})) {
            ResendEvent(env, nodeId, std::move(ev));
        }
        WaitUntil(env, [&] { return !replies.empty(); }, "reply to the notification");
        UNIT_ASSERT(QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        // BSC performs the second conversion then
        bscId.reset();
        for (auto& [nodeId, ev] : std::exchange(replies, {})) {
            ResendEvent(env, nodeId, std::move(ev));
        }
        WaitUntil(env, [&] { return !QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending(); },
            "second conversion finished");
        CheckInlinePileGroup(env, groupId);
        checker.Write(groupId);
        checker.CheckAll();
        env.Runtime->FilterFunction = {};
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
        const ui64 clusterStateGeneration = QueryStorageConfig(env).GetClusterState().GetGeneration();
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
        UNIT_ASSERT_VALUES_EQUAL(config.GetBridgeRollback().GetClusterStateGeneration(), clusterStateGeneration);
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

        // the cluster can be converted again, static group ids are not reused, generations of the cluster state go on
        ConvertClusterAgain(env, checker, groupId);
        UNIT_ASSERT_GT(QueryStorageConfig(env).GetClusterState().GetGeneration(), clusterStateGeneration);
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

    // the rollback status is collected from every node, which may take long: it doesn't hold the queue of distconf
    // operations meanwhile, and the requests coming while it is collected get the result of that collection
    Y_UNIT_TEST(StatusCollectionOutsideOperationQueue) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        TEnvironmentSetup env(MakeUnbridgedSettings());
        CreatePoolAndGetGroup(env);

        // the reports of nodes don't get to the root until released
        THashSet<ui64> collectionTaskIds;
        bool hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigScatter::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigScatter>()->Record;
                if (record.HasCollectBridgeCapability()) {
                    collectionTaskIds.insert(record.GetTaskId());
                }
            } else if (hold && ev->GetTypeRewrite() == NStorage::TEvNodeConfigGather::EventType &&
                    ev->Get<NStorage::TEvNodeConfigGather>()->Record.HasCollectBridgeCapability()) {
                held.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        };

        std::vector<TActorId> edges;
        for (ui32 i = 0; i < 3; ++i) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableQueryBridgeRollbackStatus();
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge, request.release()), 1);
            edges.push_back(edge);
            env.Sim(TDuration::MilliSeconds(100));
        }
        WaitUntil(env, [&] { return !held.empty(); }, "reports of nodes", TDuration::MilliSeconds(100), 100);

        // the queue of operations is not blocked by the collection
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());
        UNIT_ASSERT_VALUES_EQUAL(collectionTaskIds.size(), 1);

        hold = false;
        for (auto& [nodeId, ev] : std::exchange(held, {})) {
            ResendEvent(env, nodeId, std::move(ev));
        }
        for (const TActorId& edge : edges) {
            const auto res = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(res);
            const auto& record = res->Get()->Record;
            UNIT_ASSERT_EQUAL_C(record.GetStatus(), TResult::OK, record.ShortDebugString());
            UNIT_ASSERT_C(record.HasBridgeRollbackStatus(), record.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(record.GetBridgeRollbackStatus().BridgeConfigNodeIdsSize(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(collectionTaskIds.size(), 1);
        env.Runtime->FilterFunction = {};
    }

    // the ordinary cluster has nothing to roll back
    Y_UNIT_TEST(OrdinaryClusterRefused) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        CreatePoolAndGetGroup(env);
        ExpectError(InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); }),
            "Bridge mode is not enabled");
    }

    // the cluster prepared for bridge mode is rolled back by restarting nodes without bridge config, there is no
    // operation for it
    Y_UNIT_TEST(PreparedClusterRefused) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        RollingRestartWithSinglePileBridgeConfig(env, checker, env.GetGroups().front());
        ExpectError(InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); }),
            "not converted to bridged one");
    }

    // the cluster has had another pile added and removed, and its nodes still run with the bridge config listing both
    // piles when it is rolled back: they keep running with the original pile only, until restarted without bridge config
    Y_UNIT_TEST(RollbackAfterPileAddedAndRemoved) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named);
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            env.RestartNode(nodeId); // nodes of the original pile get the bridge config listing both piles too
            env.Sim(TDuration::Seconds(30));
        }
        AddPileAndWait(env, "pile_2");
        RejoinPile(env, 1);
        checker.Write(groupId);
        checker.CheckAll();

        // the pile is removed along with its nodes
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        RemovePileAndWait(env, "pile_2");
        for (ui32 nodeId = 10; nodeId <= 17; ++nodeId) {
            env.StopNode(nodeId);
        }
        env.ConfiguredNodeCount = 9;
        res = DropPileFromConfig(env, "pile_2");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->GetAppDataBridgeConfig().PilesSize(), 2);

        // nodes bound to the ones stopped may take a while to report again
        WaitUntil(env, [&] {
            res = InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
            UNIT_ASSERT_C(res.GetStatus() == TResult::OK || res.GetErrorReason().Contains("did not report"),
                res.GetErrorReason());
            return res.GetStatus() == TResult::OK;
        }, "rollback accepted", [&] { return res.GetErrorReason(); });
        WaitUntil(env, [&] { return !QueryStorageConfig(env).HasClusterState(); }, "cluster state removed",
            [&] { return QueryStorageConfig(env).GetClusterStateDetails().ShortDebugString(); });
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetBridgeRollback().GetPileName(), "pile_1");
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            checker.Write(groupId, nodeId);
            checker.CheckAll(nodeId);
        }
        CheckRollbackStatus(env, GetAllNodeIds(env));

        // the node restarted with the same bridge config works as well
        env.RestartNode(2);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, 2);
        checker.CheckAll(2);
        CheckRollbackStatus(env, GetAllNodeIds(env));
    }

    // while the cluster is being rolled back, bridged groups stay the way they are until BSC makes them ordinary ones:
    // they can't be decommitted, the original pile stays the only one, and new pools are ordinary ones; BSC retries its
    // part until what blocks it is gone
    Y_UNIT_TEST(OperationsFencedWhileRollingBack) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named); // other piles can be listed in bridge config then
        const TString overrideKind = "unused"; // no pools of this kind
        auto rollback = [&] {
            return InvokeDistconf(env, [](auto& record) { record.MutableRollbackBridge(); });
        };

        // pile overrides of the original pile are to be removed first
        auto res = ReplaceMainConfig(env, [&](YAML::Node& config) {
            YAML::Node rule;
            rule["pile_name"].push_back("pile_1");
            rule["kind"].push_back(overrideKind);
            rule["erasure_species"] = "block-4-2";
            rule["geometry"] = YAML::Node(YAML::NodeType::Map);
            config["blob_storage_config"]["storage_pool_pile_overrides"].push_back(rule);
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        ExpectError(rollback(), "Storage pool pile overrides for the original pile 'pile_1'");
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["blob_storage_config"].remove("storage_pool_pile_overrides");
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // BSC is shown an override of the pile once the rollback is accepted, so it can't make its groups ordinary ones;
        // only the configs node warden sends to its subscribers are changed, not the one distconf reports to node warden
        // (addressed to its service id)
        bool spoof = true;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (spoof && ev->GetTypeRewrite() == TEvBlobStorage::EvNodeWardenStorageConfig && !ev->Recipient.IsService()) {
                auto *msg = ev->Get<TEvNodeWardenStorageConfig>();
                if (msg->Config && NBridge::IsRollbackPending(msg->Config->GetClusterStateDetails())) {
                    auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>(*msg->Config);
                    auto *rule = config->MutableBlobStorageConfig()->AddStoragePoolPileOverrides();
                    rule->AddPileName("pile_1");
                    rule->AddKind(overrideKind);
                    rule->SetErasureSpecies("block-4-2");
                    rule->MutableGeometry();
                    msg->Config = std::move(config);
                }
            }
            return true;
        };
        res = rollback();
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitUntil(env, [&] {
            return QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        }, "error of BSC recorded");
        UNIT_ASSERT_STRING_CONTAINS(QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation()
            .GetErrorReason(), "storage pool pile overrides for the pile 'pile_1' have to be removed");
        CheckInlinePileGroup(env, groupId);

        // the bridged group can't be decommitted
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableDecommitGroups();
            cmd->AddGroupIds(groupId);
            cmd->SetDatabase(TStringBuilder() << '/' << env.Runtime->GetDomainsInfo()->GetDomain()->Name);
            const auto response = env.Invoke(request);
            UNIT_ASSERT(!response.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(),
                "bridged group can't be decommitted while the cluster is being rolled back");
        }

        // no pile can be added
        ExpectError(ReplaceMainConfig(env, [](YAML::Node& config) {
            YAML::Node pile;
            pile["name"] = "pile_2";
            config["bridge_config"]["piles"].push_back(pile);
        }), "list of bridge piles can't be changed while the cluster is being rolled back");

        // new pools are ordinary ones, they can't be bridged ones
        {
            NKikimrBlobStorage::TDefineStoragePool pool = ReadStoragePool(env, 1);
            UNIT_ASSERT(pool.GetBridgeMode());
            pool.SetStoragePoolId(2);
            pool.SetName("pool_2");
            pool.SetKind("pool_2");
            pool.SetNumGroups(1);
            pool.ClearItemConfigGeneration();
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
            auto response = env.Invoke(request);
            UNIT_ASSERT(!response.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(),
                "new storage pool can't be a bridged one while the cluster is being rolled back");
            request.MutableCommand(0)->MutableDefineStoragePool()->ClearBridgeMode();
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(!ReadStoragePool(env, 2).GetBridgeMode());
        std::vector<ui32> newGroupIds;
        {
            const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
            for (const auto *group : GetPoolGroups(baseConfig, 2)) {
                newGroupIds.push_back(group->GetGroupId());
            }
        }
        UNIT_ASSERT(!newGroupIds.empty());
        for (ui32 id : newGroupIds) {
            const auto info = GetGroupInfo(env, id);
            UNIT_ASSERT_C(!info.HasBridgeGroupState() && !info.HasBridgeProxyGroupId() && !info.HasBridgePileId(),
                info.ShortDebugString());
            checker.Write(id);
        }
        checker.Write(groupId);
        checker.CheckAll();

        // the override is gone for BSC, the rollback finishes
        spoof = false;
        env.RebootTablet(env.TabletId);
        WaitUntil(env, [&] { return !QueryStorageConfig(env).HasClusterState(); }, "cluster state removed",
            [&] { return QueryStorageConfig(env).GetClusterStateDetails().ShortDebugString(); });
        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_C(!info.HasBridgeGroupState() && !info.HasBridgeProxyGroupId() && !info.HasBridgePileId(),
            info.ShortDebugString());
        checker.Write(groupId);
        checker.CheckAll();
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(StaleConversionNotificationIgnored) {
        CheckStaleConversionNotificationIgnored(false);
    }

    Y_UNIT_TEST(StaleConversionNotificationOfOlderBscIgnored) {
        CheckStaleConversionNotificationIgnored(true);
    }

}
