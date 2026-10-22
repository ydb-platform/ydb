#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

using namespace NBridgeTest;

namespace {

    using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    constexpr ui32 PileOpsLevel = NStorage::BridgeFeatureLevelPileOps;

    size_t NumStaticVDisks(TEnvironmentSetup& env) {
        return QueryStorageConfig(env).GetBlobStorageConfig().GetServiceSet().VDisksSize();
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRemovePile) {
    // the pile is added and removed repeatedly: data stays available, nothing of the removed pile remains in the
    // cluster and the lists of piles don't grow; then the removed pile is dropped from the config along with its hosts
    Y_UNIT_TEST(AddRemoveCycles) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        std::optional<size_t> numStaticVDisks;
        THashSet<ui32> staticPartIds; // nodes know the static groups removed with the pile, so their ids are not reused
        ui64 prevAddedGeneration = 0;
        for (ui32 cycle = 0; cycle < 3; ++cycle) {
            AddPileAndWait(env, "pile_3");
            const auto config = QueryStorageConfig(env);
            // the generation of the addition tells the nodes of the previous incarnation of the pile from the current ones
            const ui64 addedGeneration = config.GetClusterStateDetails().GetPileRegistry().GetPiles(2).GetAddedGeneration();
            UNIT_ASSERT_LT(prevAddedGeneration, addedGeneration);
            UNIT_ASSERT_LE(addedGeneration, config.GetClusterState().GetGeneration());
            prevAddedGeneration = addedGeneration;
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
            // nodes of older versions can't join the cluster after pile operations
            UNIT_ASSERT(NStorage::IsPileListChanged(config));
            UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config), PileOpsLevel);
            for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.HasBridgeGroupState()) {
                    const auto *part = NBridge::GetGroupPile(group.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2));
                    UNIT_ASSERT(part);
                    UNIT_ASSERT(staticPartIds.insert(part->GetGroupId()).second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
            RejoinPile(env, 2);
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 2, FirstThirdPileNodeId);

            // static VDisks of the previous incarnation of the pile get destroyed by its nodes once they rejoin
            if (!numStaticVDisks) {
                numStaticVDisks = NumStaticVDisks(env);
            } else {
                WaitUntil(env, [&] { return NumStaticVDisks(env) == *numStaticVDisks; }, "destroyed VDisks slain");
            }

            SwitchPileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
            RemovePileAndWait(env, "pile_3");
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
                "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");
            CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
            checker.Write(groupId);
            checker.CheckAll();
        }

        // the hosts of the pile are decommissioned, the slot of the pile gets free and trimmed
        for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
            env.StopNode(nodeId);
        }
        res = DropPileFromConfig(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config), PileOpsLevel); // kept by config changes
        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        for (const auto& vdisk : ss.GetVDisks()) {
            UNIT_ASSERT_LT_C(vdisk.GetVDiskLocation().GetNodeID(), FirstThirdPileNodeId, vdisk.DebugString());
        }
        for (const auto& pdisk : ss.GetPDisks()) {
            UNIT_ASSERT_LT_C(pdisk.GetNodeID(), FirstThirdPileNodeId, pdisk.DebugString());
        }
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            UNIT_ASSERT_LT_C(pdisk.GetNodeId(), FirstThirdPileNodeId, pdisk.DebugString());
        }
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the pile added to the converted cluster is removed and added again; inline piles stay the only parts of their
    // groups meanwhile
    Y_UNIT_TEST(ConvertedAddRemove) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named);
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);

        for (ui32 cycle = 0; cycle < 2; ++cycle) {
            AddPileAndWait(env, "pile_2");
            UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 1);
            RejoinPile(env, 1);
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 1, 10);

            SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
            RemovePileAndWait(env, "pile_2");
            CheckPileGone(env, 1, 10, 17);
            const auto config = QueryStorageConfig(env);
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:INACTIVE");
            UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 1);

            // the converted group is the inline pile only again
            const auto info = GetGroupInfo(env, groupId);
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 1, info.DebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().GetPile(0).GetGroupId(), groupId, info.DebugString());
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 0, 1);
        }

        // nodes keep working after restarts
        env.RestartNode(2);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the original pile of the converted cluster is removed when the added one has taken over: groups of the cluster lose
    // their inline piles and keep serving as bridge proxy groups only, the implicit pile goes away
    Y_UNIT_TEST(ConvertedRemoveOriginalPile) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named);
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);
        AddPileAndWait(env, "pile_2");
        RejoinPile(env, 1);
        checker.Write(groupId);
        checker.CheckAll();

        // the added pile becomes the primary one, the original pile is disconnected; distconf is asked through the
        // nodes of the added pile from now on
        constexpr ui32 nodeId = 10;

        // the read of the blob present in the added pile only gets NODATA from the inline pile, while the response of
        // the added pile is held until the inline pile is removed; then the blob is restored into the inline pile gone
        const TLogoBlobID blobId(100500, 1, 1, 0, 6, 0);
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const ui32 addedPartId = NBridge::GetGroupPile(GetGroupInfo(env, groupId).GetBridgeGroupState(),
            TBridgePileId::FromPileIndex(1))->GetGroupId();
        const ui32 addedPartGeneration = GetGroupInfo(env, addedPartId).GetGroupGeneration();
        env.Runtime->WrapInActorContext(edge, [&] {
            auto *put = new TEvBlobStorage::TEvPut(blobId, TString("hello!"), TInstant::Max());
            put->ForceGroupGeneration = addedPartGeneration;
            SendToBSProxy(edge, addedPartId, put);
        });
        {
            const auto putResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ToString());
        }
        std::unique_ptr<IEventHandle> heldResult;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && !heldResult) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->GroupId == addedPartId && msg->ResponseSz == 1 && msg->Responses[0].Id == blobId) {
                    heldResult = std::move(ev);
                    return false;
                }
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, true /*mustRestoreFirst*/));
        });
        WaitUntil(env, [&] { return heldResult != nullptr; }, "response of the added pile");
        env.Runtime->FilterFunction = {};

        auto clusterState = QueryStorageConfig(env).GetClusterState();
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        clusterState.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        res = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
        }, nodeId);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));

        res = RemovePile(env, "pile_1", nodeId);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:INACTIVE pile_2:SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 0);
        CheckPileGone(env, 0, 1, 9, nodeId);

        // the restore write into the inline pile gone gets error as the subrequest of the inline pile, the read is
        // answered and the node keeps running
        env.Runtime->Send(heldResult.release(), nodeId);
        env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);

        // the groups of the converted cluster are pure bridge proxy groups now
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                UNIT_ASSERT_C(!group.RingsSize() && !group.HasBridgeProxyGroupId(), group.DebugString());
            }
        }
        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_C(!info.RingsSize() && !info.HasBridgeProxyGroupId(), info.DebugString());
        UNIT_ASSERT_C(NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1)),
            info.DebugString());
        checker.Write(groupId, nodeId);
        checker.CheckAll(nodeId);
        CheckPileData(env, checker, 1, nodeId);

        // nodes of the remaining pile keep working after restarts
        env.RestartNode(11);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);
        checker.CheckAll(nodeId);
    }

    // notification of BSC delivered late (e.g. retried after BSC restart) can't finish another operation on the same pile
    Y_UNIT_TEST(StaleNotificationRefused) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot> notification;
        bool hold = false;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (const auto *record = GetBridgePileOperationNotification(*ev)) {
                if (!notification) {
                    notification.emplace(*record);
                }
                return !hold;
            }
            return true;
        };
        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT(notification);
        RemovePileAndWait(env, "pile_3");

        // the same pile is being added again, while the notification of the first addition comes once more
        hold = true;
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        hold = false; // BSC doesn't repeat the dropped notification until restarted
        ExpectError(InvokeDistconf(env, [&](auto& record) { record.CopyFrom(*notification); }),
            "Pending pile operation is another one");
        const auto config = QueryStorageConfig(env);
        const auto& details = config.GetClusterStateDetails();
        UNIT_ASSERT(details.HasPendingPileOperation());
        UNIT_ASSERT_UNEQUAL(details.GetPendingPileOperation().GetOperationId(),
            notification->GetNotifyBridgePileOperation().GetOperationId());

        // BSC (residing on node 1) performs its part again after restart, as the notification has been dropped
        env.RestartNode(1);
        WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
    }

    // two piles of three fail at once and the remaining one removes them one by one, though the nodes of the failed
    // piles can't report anything (they wouldn't be let in by the remaining pile even if they were running)
    Y_UNIT_TEST(RemoveTwoFailedPiles) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto clusterState = QueryStorageConfig(env).GetClusterState();
        UNIT_ASSERT_VALUES_EQUAL(TBridgePileId::FromProto(&clusterState,
            &NKikimrBridge::TClusterState::GetPrimaryPile).GetPileIndex(), 0);
        for (ui32 nodeId = 9; nodeId <= 24; ++nodeId) {
            env.StopNode(nodeId);
        }
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        clusterState.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        clusterState.SetPerPileState(2, NKikimrBridge::TClusterState::DISCONNECTED);
        // the nodes of the remaining pile may be still rebinding after losing their parents
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(2);
        for (;;) {
            const auto res = InvokeDistconf(env, [&](auto& record) {
                record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
            });
            if (res.GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C((res.GetStatus() == TResult::NO_QUORUM || res.GetStatus() == TResult::RACE) &&
                env.Runtime->GetClock() < deadline, res.DebugString());
            env.Sim(TDuration::Seconds(1));
        }
        env.Sim(TDuration::Seconds(30));

        RemovePileAndWait(env, "pile_2");
        CheckPileGone(env, 1, 9, 16);
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, 17, 24);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
            "pile_1:SYNCHRONIZED pile_2:INACTIVE pile_3:INACTIVE");
        checker.Write(groupId);
        checker.CheckAll();
    }

    // only DISCONNECTED pile without pending operation can be removed
    Y_UNIT_TEST(Refusals) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        ExpectError(RemovePile(env, "pile_4"), "is not listed in BridgeConfig");
        ExpectError(RemovePile(env, "pile_1"), "has to be DISCONNECTED to be removed from the cluster, its state is"
            " SYNCHRONIZED");
        ExpectError(RemovePile(env, "pile_3"), "its state is INACTIVE");

        // other piles can't be removed until BSC finishes adding the pile (its own removal cancels the addition, see
        // BridgeAddPile::StuckAdditionCancelled)
        bool hold = true;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            return !hold || !GetBridgePileOperationNotification(*ev);
        };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        ExpectError(RemovePile(env, "pile_2"), "Operation on pile 'pile_3' is in progress");
        // the notification has been dropped, BSC (residing on node 1) performs its part again after restart
        hold = false;
        env.RestartNode(1);
        WaitPileOperationFinished(env);

        // nothing has been removed by the refused commands
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
            "pile_1:SYNCHRONIZED pile_2:DISCONNECTED pile_3:DISCONNECTED");
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 9);
    }

    // distconf finishes removal only when BSC has deleted parts of its groups; the pile can't change its state meanwhile,
    // new groups get no parts in it, and BSC repeats its part after restart
    Y_UNIT_TEST(RemovalWaitsForBsc) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        AddPileAndWait(env, "pile_3");

        bool hold = true;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
                return !hold;
            }
            return true;
        };
        res = RemovePile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(numNotifications);
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_EQUAL(config.GetClusterStateDetails().GetPendingPileOperation().GetKind(),
            NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE);
        // nodes of older versions can't work with the cluster removing piles
        UNIT_ASSERT(config.GetClusterStateDetails().GetPileRegistry().GetPileListChanged());
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetRequiredFeatureLevel(),
            static_cast<ui32>(NStorage::BridgeFeatureLevelPileOps));
        // BSC has already deleted its parts, while static ones are still there
        const auto pileId = TBridgePileId::FromPileIndex(2);
        UNIT_ASSERT(!NBridge::GetGroupPile(GetGroupInfo(env, groupId).GetBridgeGroupState(), pileId));
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                UNIT_ASSERT(NBridge::GetGroupPile(group.GetBridgeGroupState(), pileId));
            }
        }

        ExpectError(TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1),
            "is being removed from the cluster");
        ExpectError(AddPile(env, "pile_3"), "Operation on pile 'pile_3' is in progress");

        // new groups get no parts in the pile being removed
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        const auto poolGroups = GetPoolGroups(baseConfig, 2);
        UNIT_ASSERT(!poolGroups.empty());
        for (const auto *group : poolGroups) {
            if (group->GetIsProxyGroup()) {
                const auto info = GetGroupInfo(env, group->GetGroupId());
                UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 2, info.DebugString());
                checker.Write(group->GetGroupId());
            }
        }
        checker.Write(groupId);
        checker.CheckAll();

        // BSC (residing on node 1) is restarted, it performs its part again finding everything done already
        hold = false;
        env.RestartNode(1);
        WaitPileOperationFinished(env);
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        checker.Write(groupId);
        checker.CheckAll();
    }

    // drives of the removed pile get ACTIVE status back, as nobody can tell how they are until the pile is added again;
    // INACTIVE ones (CMS sentinel makes them so when their nodes go away) would keep the pile from being added
    Y_UNIT_TEST(RemovedPileDrivesActiveAgain) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        AddPileAndWait(env, "pile_3");
        RejoinPile(env, 2);

        auto forEachPileDrive = [&](const std::function<void(const NKikimrBlobStorage::TBaseConfig::TPDisk&)>& callback) {
            size_t numDrives = 0;
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto& pdisk : baseConfig.GetPDisk()) {
                if (FirstThirdPileNodeId <= pdisk.GetNodeId() && pdisk.GetNodeId() <= LastThirdPileNodeId) {
                    callback(pdisk);
                    ++numDrives;
                }
            }
            UNIT_ASSERT(numDrives);
        };

        SwitchPileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        forEachPileDrive([&](const auto& pdisk) {
            env.UpdateDriveStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(), NKikimrBlobStorage::EDriveStatus::INACTIVE, {},
                true);
        });
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        forEachPileDrive([&](const auto& pdisk) {
            UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::ACTIVE, pdisk.DebugString());
        });

        // status change as CMS sentinel makes it, judging the drive by what it sees of it
        auto updateObservedStatus = [&](ui32 nodeId, ui32 pdiskId, NKikimrBlobStorage::EDriveStatus status) {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupFailModelChecks(true);
            request.SetIgnoreDegradedGroupsChecks(true);
            request.SetIgnoreDisintegratedGroupsChecks(true);
            auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
            cmd->MutableHostKey()->SetNodeId(nodeId);
            cmd->SetPDiskId(pdiskId);
            cmd->SetStatus(status);
            cmd->SetSkipIfPileCutOff(true);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        };

        // the stale change comes after the removal (e.g. the sentinel retries the one judged before the pile went
        // away): the drives of the pile cut off from the cluster keep their status
        forEachPileDrive([&](const auto& pdisk) {
            updateObservedStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(), NKikimrBlobStorage::EDriveStatus::INACTIVE);
        });
        forEachPileDrive([&](const auto& pdisk) {
            UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::ACTIVE, pdisk.DebugString());
        });

        // while the drives of connected piles get it
        std::optional<std::tuple<ui32, ui32>> connectedDrive;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == 1) {
                connectedDrive.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
            }
        }
        UNIT_ASSERT(connectedDrive);
        const auto& [connectedNodeId, connectedPDiskId] = *connectedDrive;
        updateObservedStatus(connectedNodeId, connectedPDiskId, NKikimrBlobStorage::EDriveStatus::INACTIVE);
        bool found = false;
        const auto updatedBaseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : updatedBaseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == connectedNodeId && pdisk.GetPDiskId() == connectedPDiskId) {
                UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::INACTIVE, pdisk.DebugString());
                found = true;
            }
        }
        UNIT_ASSERT(found);
        env.UpdateDriveStatus(connectedNodeId, connectedPDiskId, NKikimrBlobStorage::EDriveStatus::ACTIVE, {}, true);

        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }
}
