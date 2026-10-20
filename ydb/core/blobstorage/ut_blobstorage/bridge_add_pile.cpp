#include "bridge_config_yaml.h"

#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

using namespace NBridgeTest;

namespace {

    using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    TResult AddPile(TEnvironmentSetup& env, const TString& name) {
        return InvokeDistconf(env, [&](auto& record) { record.MutableAddBridgePile()->SetPileName(name); });
    }

    // start nodes of the pile with BridgeConfig listing it
    void StartPileNodes(TEnvironmentSetup& env, const TString& name, ui32 firstNodeId, ui32 lastNodeId) {
        env.Runtime->GetAppDataBridgeConfig().AddPiles()->SetName(name);
        env.ConfiguredNodeCount = lastNodeId;
        for (ui32 nodeId = firstNodeId; nodeId <= lastNodeId; ++nodeId) {
            env.StartNode(nodeId);
        }
        env.Sim(TDuration::Seconds(30));
    }

    // wait until BSC performs its part of the pending pile operation
    NKikimrBlobStorage::TStorageConfig WaitPileOperationFinished(TEnvironmentSetup& env) {
        WaitUntil(env, [&] { return !QueryStorageConfig(env).GetClusterStateDetails().HasPendingPileOperation(); },
            "pile operation finish");
        return QueryStorageConfig(env);
    }

    void RejoinPile(TEnvironmentSetup& env, ui32 pileIndex) {
        SwitchPileState(env, pileIndex, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, pileIndex, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetClusterStateDetails().PileSyncStateSize(), 0);
    }

    // the pile has its part in every bridged group, static and dynamic ones, placed on its nodes, and its state
    // storage replicas; returns the number of dynamic bridged groups
    size_t CheckPileParts(TEnvironmentSetup& env, ui32 pileIndex, ui32 firstNodeId, ui32 lastNodeId) {
        const auto pileId = TBridgePileId::FromPileIndex(pileIndex);
        auto inPile = [&](ui32 nodeId) { return firstNodeId <= nodeId && nodeId <= lastNodeId; };
        const auto config = QueryStorageConfig(env);

        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        THashMap<ui32, const NKikimrBlobStorage::TGroupInfo*> staticGroups;
        for (const auto& group : ss.GetGroups()) {
            staticGroups.emplace(group.GetGroupID(), &group);
        }
        THashSet<ui32> pileGroupIds; // parts of bridged groups, static and dynamic ones, in the pile
        for (const auto& group : ss.GetGroups()) {
            if (!group.HasBridgeGroupState()) {
                continue;
            }
            const auto *pile = NBridge::GetGroupPile(group.GetBridgeGroupState(), pileId);
            UNIT_ASSERT_C(pile, group.DebugString());
            const auto it = staticGroups.find(pile->GetGroupId());
            UNIT_ASSERT(it != staticGroups.end());
            for (const auto& realm : it->second->GetRings()) {
                for (const auto& domain : realm.GetFailDomains()) {
                    for (const auto& location : domain.GetVDiskLocations()) {
                        UNIT_ASSERT_C(inPile(location.GetNodeID()), it->second->DebugString());
                    }
                }
            }
            UNIT_ASSERT(pileGroupIds.insert(pile->GetGroupId()).second);
        }
        UNIT_ASSERT(!pileGroupIds.empty());

        for (const auto *pb : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                &config.GetSchemeBoardConfig()}) {
            size_t numRingGroups = 0;
            for (const auto& ringGroup : pb->GetRingGroups()) {
                if (TBridgePileId::FromProto(&ringGroup, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId) ==
                        pileId) {
                    ++numRingGroups;
                    for (const auto& ring : ringGroup.GetRing()) {
                        for (const ui32 nodeId : ring.GetNode()) {
                            UNIT_ASSERT(inPile(nodeId));
                        }
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(numRingGroups, 1, pb->DebugString());
        }

        // every dynamic bridged group has exactly one part in the pile, and only parts in the pile reside on its nodes
        const auto baseConfig = env.FetchBaseConfig();

        // static groups reside on PDisks of BSC, as there can be only one PDisk on the drive
        THashMap<std::tuple<ui32, ui32>, const NKikimrBlobStorage::TBaseConfig::TPDisk*> bscPDisks;
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            bscPDisks.emplace(std::make_tuple(pdisk.GetNodeId(), pdisk.GetPDiskId()), &pdisk);
        }
        for (const auto& pdisk : ss.GetPDisks()) {
            if (inPile(pdisk.GetNodeID())) {
                const auto it = bscPDisks.find(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPDiskID()));
                UNIT_ASSERT_C(it != bscPDisks.end(), pdisk.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(it->second->GetPath(), pdisk.GetPath());
                UNIT_ASSERT_VALUES_EQUAL(it->second->GetGuid(), pdisk.GetPDiskGuid());
            }
        }

        THashMap<ui32, ui32> vslotNodes;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            vslotNodes.emplace(vslot.GetGroupId(), vslot.GetVSlotId().GetNodeId());
        }
        size_t numBridged = 0;
        for (const auto& group : baseConfig.GetGroup()) {
            if (staticGroups.contains(group.GetGroupId())) {
                continue; // checked above
            }
            const auto info = GetGroupInfo(env, group.GetGroupId());
            if (!info.HasBridgeGroupState()) {
                continue;
            }
            ++numBridged;
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), pileId);
            UNIT_ASSERT_C(pile, info.DebugString());
            UNIT_ASSERT(pileGroupIds.insert(pile->GetGroupId()).second);
        }
        for (const auto& [groupId, nodeId] : vslotNodes) {
            UNIT_ASSERT_VALUES_EQUAL_C(pileGroupIds.contains(groupId), inPile(nodeId), "GroupId# " << groupId);
        }
        return numBridged;
    }

    // read data written by the checker from the parts of groups in the pile, not through the bridge proxy
    void CheckPileData(TEnvironmentSetup& env, const TDataChecker& checker, ui32 pileIndex, ui32 nodeId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        for (const auto& [groupId, id, data] : checker.GetWritten()) {
            const auto info = GetGroupInfo(env, groupId);
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
            UNIT_ASSERT(pile);
            const ui32 pileGroupId = pile->GetGroupId();
            const ui32 generation = GetGroupInfo(env, pileGroupId).GetGroupGeneration();
            env.Runtime->WrapInActorContext(edge, [&] {
                auto *request = new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
                request->ForceGroupGeneration = generation;
                SendToBSProxy(edge, pileGroupId, request);
            });
            CheckGetResult(*env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false)->Get(), data);
        }
    }

    // nodes 1-9 make the cluster to be converted (as in MakeUnbridgedSettings), they get the name of their pile when
    // named is set; nodes 10-17 make the pile added later
    TNodeLocation GetConvertedClusterLocation(ui32 nodeId, bool named) {
        if (nodeId <= 9 && !named) {
            return GetUnbridgedLocation(nodeId);
        }
        NActorsInterconnect::TNodeLocation location;
        location.SetBridgePileName(nodeId <= 9 ? "pile_1" : "pile_2");
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

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
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is already added to the cluster");

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
        for (const auto *group : GetPoolGroups(env.FetchBaseConfig(), 2)) {
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
        TEnvironmentSetup env{{
            .NodeCount = 17,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = [named](ui32 nodeId) { return GetConvertedClusterLocation(nodeId, *named); },
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 9,
        }};
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        // other piles can't be listed in the config while nodes of the converted cluster have no pile names
        auto listPiles = [](YAML::Node& config, std::vector<TString> names) {
            for (const auto& name : names) {
                YAML::Node pile;
                pile["name"] = name;
                config["bridge_config"]["piles"].push_back(pile);
            }
        };
        auto res = ReplaceMainConfig(env, [&](YAML::Node& config) { listPiles(config, {"pile_1", "pile_2"}); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "mandatory pile name is missing for node 1");

        // nodes of the converted cluster get the name of their pile (implicit one), restarted with it one by one
        res = ReplaceMainConfig(env, [&](YAML::Node& config) {
            listPiles(config, {"pile_1"});
            for (YAML::Node host : config["hosts"]) {
                host["location"]["bridge_pile_name"] = "pile_1";
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        *named = true;
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED");

        res = AddPileToConfig(env, "pile_2", 10, 17);
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

        RejoinPile(env, 1);
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
            } else if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType &&
                    ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBridgePileOperation()) {
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
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType &&
                    ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBridgePileOperation()) {
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
