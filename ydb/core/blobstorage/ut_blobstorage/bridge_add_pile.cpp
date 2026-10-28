#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

using namespace NBridgeTest;

namespace {

    using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    // make explicit host config for existing hosts and add another one, which has drives with one slot only
    TResult AddSingleSlotHostConfig(TEnvironmentSetup& env) {
        return ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][0]["host_config_id"] = 1;
            for (YAML::Node host : config["hosts"]) {
                host["host_config_id"] = 1;
            }
            YAML::Node drive;
            drive["path"] = "SectorMap:X:1000";
            drive["type"] = "NVME";
            drive["expected_slot_count"] = 1;
            YAML::Node hostConfig;
            hostConfig["host_config_id"] = 2;
            hostConfig["drive"].push_back(drive);
            config["host_configs"].push_back(hostConfig);
        });
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeAddPile) {
    Y_UNIT_TEST(NativeTwoToThree) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        res = AddPile(env, "pile_4");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is not listed in BridgeConfig");
        res = AddPile(env, "pile_2");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(),
            "has to be INACTIVE to be added to the cluster, its state is SYNCHRONIZED");

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        checker.Write(groupId);
        checker.CheckAll();

        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);

        // new groups get their parts in every pile
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        for (const auto *group : GetPoolGroups(baseConfig, 2)) {
            if (group->GetIsProxyGroup()) {
                checker.Write(group->GetGroupId());
            }
        }
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
        CheckPileData(env, checker, 0, 1);
    }

    Y_UNIT_TEST(ConvertedOneToTwo) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        // other piles can't be listed in the config while nodes of the converted cluster have no pile names
        ExpectError(ReplaceMainConfig(env, [&](YAML::Node& config) {
            for (const char *name : {"pile_1", "pile_2"}) {
                YAML::Node pile;
                pile["name"] = name;
                config["bridge_config"]["piles"].push_back(pile);
            }
        }), "mandatory pile name is missing for node 1");

        // nodes of the converted cluster get the name of their pile (implicit one), restarted with it one by one
        NameConvertedClusterNodes(env, *named, [&] {
            checker.Write(groupId);
            checker.CheckAll();
        });
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED");

        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);

        res = AddPile(env, "pile_2");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:DISCONNECTED");
        // groups without pile id stay in the implicit pile
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 1);
        checker.Write(groupId);
        checker.CheckAll();

        // the group relies on its original part until the part in the new pile is synced, and the original pile can't
        // go out meanwhile
        auto getAuthorityPileId = [&] {
            const auto info = GetGroupInfo(env, groupId);
            return TBridgePileId::FromProto(&info.GetBridgeGroupState(),
                &NKikimrBridge::TGroupState::GetAuthorityPileId);
        };
        UNIT_ASSERT(getAuthorityPileId() == TBridgePileId::FromPileIndex(0));
        ExpectError(TrySwitchPileState(env, 0, NKikimrBridge::TClusterState::DISCONNECTED),
            "Incorrect primary pile state");

        RejoinPile(env, 1);
        WaitUntil(env, [&] { return !getAuthorityPileId(); }, "group not relying on its original part");
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 10);
        CheckPileData(env, checker, 0, 1);

        // nodes of both piles keep working after restarts
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    // groups of the pools not converted along with the cluster get no parts in the added pile, while static groups do
    Y_UNIT_TEST(ConvertedOneToTwoSkippingStoragePools) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 1, true);
        const ui32 staticGroupId = 0;
        checker.Write(staticGroupId);

        NameConvertedClusterNodes(env, *named, [&] {
            checker.Write(groupId);
            checker.CheckAll();
        });
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);
        AddPileAndWait(env, "pile_2");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 0); // no dynamic bridged groups
        RejoinPile(env, 1);

        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_C(!info.HasBridgeGroupState(), info.ShortDebugString());
        for (ui32 nodeId : {1u, 10u}) {
            checker.Write(groupId, nodeId);
            checker.Write(staticGroupId, nodeId);
            checker.CheckAll(nodeId);
        }
    }

    // nothing gets committed when there are not enough resources in the pile, neither for static groups, nor for the
    // groups of BSC; nodes of the pile don't have to be running to add it
    Y_UNIT_TEST(NotEnoughResources) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env, 2);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, FirstThirdPileNodeId + 3, 2); // not enough hosts
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        auto checkNothingCommitted = [&](const NKikimrBlobStorage::TStorageConfig& before) {
            const auto config = QueryStorageConfig(env);
            UNIT_ASSERT_VALUES_EQUAL(config.GetGeneration(), before.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");
            UNIT_ASSERT(!config.GetClusterStateDetails().HasPendingPileOperation());
        };

        auto before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "failed to allocate part of static group");
        checkNothingCommitted(before);

        // static group fits now, but the groups of BSC need two slots on each drive
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            AddPileHosts(config, "pile_3", FirstThirdPileNodeId + 4, LastThirdPileNodeId, 2);
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "BSC can't add parts of its groups to the pile");
        checkNothingCommitted(before);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolGroups(env.FetchBaseConfig(), 1).size(), 3 * 2); // proxy and two parts for each

        auto setSlotCount = [&](ui32 slotCount) {
            const auto res = ReplaceMainConfig(env, [&](YAML::Node& config) {
                config["host_configs"][1]["drive"][0]["expected_slot_count"] = slotCount;
            });
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            env.Sim(TDuration::Seconds(10));
        };

        // two slots would fit the groups of BSC, but static group takes one of them
        setSlotCount(2);
        before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "BSC can't add parts of its groups to the pile");
        checkNothingCommitted(before);

        // there are enough slots now
        setSlotCount(4);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);

        // BSC knows static VDisks placed on its PDisks in the pile without being restarted
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (FirstThirdPileNodeId <= pdisk.GetNodeId()) {
                UNIT_ASSERT_VALUES_EQUAL_C(pdisk.GetNumStaticSlots(), 1, pdisk.DebugString());
            }
        }

        // nodes of the pile are started only now, the pile gets synced
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // static groups are placed on PDisks known to BSC, so the pile can't be added while BSC doesn't provide them
    Y_UNIT_TEST(BaseConfigUnavailable) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // pipe of NodeWarden to BSC breaks while the base config is being queried
        bool breakPipe = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (breakPipe && ev->GetTypeRewrite() == TEvTabletPipe::EvSend &&
                    ev->Type == TEvBlobStorage::EvControllerConfigRequest) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                if (request.CommandSize() == 1 && request.GetCommand(0).HasQueryBaseConfig()) {
                    breakPipe = false;
                    env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, ev->GetRecipientRewrite(), {},
                        nullptr, 0), nodeId);
                    return false;
                }
            }
            return true;
        };
        const auto before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT(!breakPipe);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "Can't get base config: pipe to BSC disconnected");
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(config.GetGeneration(), before.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");

        // pipe gets reestablished, the pile is added on PDisks of BSC
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
    }

    // BSC fails to perform its part after the pile has been added (as resources has gone after validation); the error is
    // recorded and the operation is retried, so that it finishes when there are enough resources
    Y_UNIT_TEST(BscFailureRetried) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId, 2);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // validation by BSC fails, but distconf is told it succeeded
        bool spoofValidation = true;
        ui32 numSpoofed = 0;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerDistconfResponse && spoofValidation) {
                auto& record = ev->Get<TEvBlobStorage::TEvControllerDistconfResponse>()->Record;
                if (record.GetStatus() == NKikimrBlobStorage::TEvControllerDistconfResponse::Error) {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                    record.ClearErrorReason();
                    ++numSpoofed;
                }
            } else if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
            }
            return true;
        };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(numSpoofed, 1);
        spoofValidation = false;

        WaitUntil(env, [&] {
            return QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        }, "error of BSC recorded");
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_STRING_CONTAINS(config.GetClusterStateDetails().GetPendingPileOperation().GetErrorReason(),
            "Group fit error");

        // BSC keeps retrying, the same error doesn't change the config
        const ui32 notificationsBefore = numNotifications;
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_GT(numNotifications, notificationsBefore);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetGeneration(), config.GetGeneration());

        res = TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is being added to the cluster");
        checker.Write(groupId);
        checker.CheckAll();

        // resources appear, BSC finishes the operation
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][1]["drive"][0]["expected_slot_count"] = 4;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);

        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // groups created while the addition of the pile is stuck get no part in the pile (the addition gives it to them), so
    // they neither fail for lack of resources there nor take the ones the addition counts on; the stuck addition is
    // cancelled by removing the pile, which can be added again then
    Y_UNIT_TEST(StuckAdditionCancelled) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId, 2);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // validation by BSC fails, but distconf is told it succeeded, so BSC can't place the parts afterwards
        bool spoofValidation = true;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerDistconfResponse && spoofValidation) {
                auto& record = ev->Get<TEvBlobStorage::TEvControllerDistconfResponse>()->Record;
                if (record.GetStatus() == NKikimrBlobStorage::TEvControllerDistconfResponse::Error) {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                    record.ClearErrorReason();
                }
            }
            return true;
        };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        spoofValidation = false;
        WaitUntil(env, [&] {
            return QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        }, "error of BSC recorded");

        // the new pool gets its group with no part in the pile
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        std::optional<ui32> newGroupId;
        {
            const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
            for (const auto *group : GetPoolGroups(baseConfig, 2)) {
                if (group->GetIsProxyGroup()) {
                    newGroupId = group->GetGroupId();
                }
            }
        }
        UNIT_ASSERT(newGroupId);
        const auto info = GetGroupInfo(env, *newGroupId);
        UNIT_ASSERT_C(!NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2)),
            info.DebugString());
        checker.Write(*newGroupId);

        // the stuck addition is cancelled by the removal
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        checker.CheckAll();

        // resources appear, the pile is added with parts of both groups
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][1]["drive"][0]["expected_slot_count"] = 4;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // the pile can't be rejoined until BSC adds its parts to every group; BSC finishes the operation after restart
    Y_UNIT_TEST(RejoinWaitsForBsc) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        // BSC has created parts of its groups, but distconf doesn't know it yet
        bool hold = true;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
                return !hold;
            }
            return true;
        };

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(numNotifications);
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT(config.GetClusterStateDetails().HasPendingPileOperation());

        res = TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is being added to the cluster");
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        checker.Write(groupId);
        checker.CheckAll();

        // BSC (residing on node 1) is restarted, it performs the operation again finding everything done already
        hold = false;
        env.RestartNode(1);
        config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolGroups(env.FetchBaseConfig(), 1).size(), 4); // proxy and three parts

        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // restarts of the nodes of the new pile and of the cluster while the pile is being added and synced
    Y_UNIT_TEST(RestartsWhileAdding) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        for (ui32 nodeId : {FirstThirdPileNodeId, 2u, 9u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(10));
        }
        WaitPileOperationFinished(env);
        checker.Write(groupId);
        checker.CheckAll();

        SwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        for (ui32 nodeId : {FirstThirdPileNodeId + 1, 1u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(10));
        }
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }
}
