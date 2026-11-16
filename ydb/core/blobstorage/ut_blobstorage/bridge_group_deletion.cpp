#include "bridge_common.h"

using namespace NBridgeTest;

namespace {

    TEnvironmentSetup::TSettings MakeSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    bool HasGroup(const NKikimrBlobStorage::TBaseConfig& baseConfig, ui32 groupId) {
        const auto& groups = baseConfig.GetGroup();
        return std::any_of(groups.begin(), groups.end(), [&](const auto& group) { return group.GetGroupId() == groupId; });
    }

    // bridge proxy groups of the pool; the base config lists their parts under the pool too
    std::vector<ui32> GetProxyGroups(TEnvironmentSetup& env, ui64 storagePoolId) {
        std::vector<ui32> res;
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        for (const auto *group : GetPoolGroups(baseConfig, storagePoolId)) {
            if (group->GetIsProxyGroup()) {
                res.push_back(group->GetGroupId());
            }
        }
        return res;
    }

    NKikimrBlobStorage::TConfigResponse DeleteGroups(TEnvironmentSetup& env, const std::vector<ui32>& groupIds) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableDeleteSpecificGroups();
        for (const ui32 groupId : groupIds) {
            cmd->AddGroupIds(groupId);
        }
        return env.Invoke(request);
    }

    NKikimrBlobStorage::TConfigResponse DeletePool(TEnvironmentSetup& env, ui64 storagePoolId) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableDeleteStoragePool();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(storagePoolId);
        cmd->SetItemConfigGeneration(ReadStoragePool(env, storagePoolId).GetItemConfigGeneration());
        return env.Invoke(request);
    }

    NKikimrBlobStorage::TConfigResponse MoveGroups(TEnvironmentSetup& env, ui64 origin, ui64 target,
            const std::vector<ui32>& groupIds) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *move = request.AddCommand()->MutableMoveGroups();
        move->SetBoxId(1);
        move->SetOriginStoragePoolId(origin);
        move->SetOriginStoragePoolGeneration(ReadStoragePool(env, origin).GetItemConfigGeneration());
        move->SetTargetStoragePoolId(target);
        move->SetTargetStoragePoolGeneration(ReadStoragePool(env, target).GetItemConfigGeneration());
        for (const ui32 groupId : groupIds) {
            move->AddExplicitGroupId(groupId);
        }
        return env.Invoke(request);
    }

    // restart the node running BS_CONTROLLER, so that it loads its state again, and check the data written before
    void RestartControllerAndCheck(TEnvironmentSetup& env, TDataChecker& checker) {
        env.RestartNode(env.Settings.ControllerNodeId);
        env.Sim(TDuration::Seconds(30));
        env.FetchBaseConfig(); // BS_CONTROLLER answers again
        checker.CheckAll();
    }

} // namespace

Y_UNIT_TEST_SUITE(BridgeGroupDeletion) {

    // Console deletes the groups Hive gives up when a pool shrinks, and Hive knows bridge proxy groups only
    Y_UNIT_TEST(DeletingProxyDeletesItsParts) {
        TEnvironmentSetup env(MakeSettings());
        CreatePoolAndGetGroup(env, 2);
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 deleted = groups[0];
        const ui32 kept = groups[1];
        const auto parts = GetPileGroups(env, deleted);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);

        TDataChecker checker(env);
        checker.Write(kept);

        const auto response = DeleteGroups(env, {deleted});
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const auto config = env.FetchBaseConfig();
        UNIT_ASSERT(!HasGroup(config, deleted));
        for (const ui32 part : parts) {
            UNIT_ASSERT_C(!HasGroup(config, part), "part of the deleted bridged group is left GroupId# " << part);
        }
        UNIT_ASSERT(HasGroup(config, kept));
        for (const ui32 part : GetPileGroups(env, kept)) {
            UNIT_ASSERT(HasGroup(config, part));
        }

        // the remaining bridged group still works, before and after BS_CONTROLLER restarts
        checker.Write(kept);
        RestartControllerAndCheck(env, checker);
    }

    // deleting a part leaves its bridge proxy group referring to a group that doesn't exist
    Y_UNIT_TEST(DeletingPartAloneIsRefused) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const auto parts = GetPileGroups(env, proxy);
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 2);

        TDataChecker checker(env);
        checker.Write(proxy);

        const auto response = DeleteGroups(env, {parts.back()});
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "part of bridged GroupId#");
        const auto config = env.FetchBaseConfig();
        for (const ui32 part : parts) {
            UNIT_ASSERT(HasGroup(config, part));
        }
        checker.CheckAll(); // the data is still there

        // deleting the part along with its bridge proxy group is fine
        const auto both = DeleteGroups(env, {parts.back(), proxy});
        UNIT_ASSERT_C(both.GetSuccess(), both.GetErrorDescription());
        const auto after = env.FetchBaseConfig();
        UNIT_ASSERT(!HasGroup(after, proxy));
        for (const ui32 part : parts) {
            UNIT_ASSERT(!HasGroup(after, part));
        }

        env.RestartNode(env.Settings.ControllerNodeId);
        env.Sim(TDuration::Seconds(30));
        env.FetchBaseConfig();
    }

    // the listed bridge proxy group moves with its parts, and the other one stays in place with its own
    Y_UNIT_TEST(MovingProxyMovesItsParts) {
        TEnvironmentSetup env(MakeSettings());
        CreatePoolAndGetGroup(env, 2);
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 proxy = groups[0];
        const ui32 other = groups[1];
        const auto parts = GetPileGroups(env, proxy);
        const auto otherParts = GetPileGroups(env, other);
        CreateAnotherPool(env, 2);

        TDataChecker checker(env);
        checker.Write(proxy);

        const auto response = MoveGroups(env, 1, 2, {proxy});
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const auto config = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(FindGroup(config, proxy).GetStoragePoolId(), 2);
        for (const ui32 part : parts) {
            UNIT_ASSERT_VALUES_EQUAL_C(FindGroup(config, part).GetStoragePoolId(), 2, "GroupId# " << part);
        }
        UNIT_ASSERT_VALUES_EQUAL(FindGroup(config, other).GetStoragePoolId(), 1);
        for (const ui32 part : otherParts) {
            UNIT_ASSERT_VALUES_EQUAL_C(FindGroup(config, part).GetStoragePoolId(), 1, "GroupId# " << part);
        }
        // NumGroups counts bridge proxy groups, not their parts
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 1).GetNumGroups(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).GetNumGroups(), 2);

        // deleting the origin pool deletes the group left there and keeps the moved one intact
        const auto deletion = DeletePool(env, 1);
        UNIT_ASSERT_C(deletion.GetSuccess(), deletion.GetErrorDescription());
        const auto after = env.FetchBaseConfig();
        UNIT_ASSERT(HasGroup(after, proxy));
        for (const ui32 part : parts) {
            UNIT_ASSERT(HasGroup(after, part));
        }
        UNIT_ASSERT(!HasGroup(after, other));
        for (const ui32 part : otherParts) {
            UNIT_ASSERT(!HasGroup(after, part));
        }
        checker.Write(proxy);
        RestartControllerAndCheck(env, checker);
    }

    Y_UNIT_TEST(MovingPartAloneIsRefused) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const auto parts = GetPileGroups(env, proxy);
        CreateAnotherPool(env, 2);

        const auto response = MoveGroups(env, 1, 2, {parts.front()});
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "part of bridged GroupId#");
        const auto config = env.FetchBaseConfig();
        for (const ui32 part : parts) {
            UNIT_ASSERT_VALUES_EQUAL(FindGroup(config, part).GetStoragePoolId(), 1);
        }
    }

    // the moved groups get a new generation, and their VDisks keep their state with it: a read-only VDisk stays such on
    // its node
    Y_UNIT_TEST(MovingKeepsReadOnlyVDisk) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const ui32 part = GetPileGroups(env, proxy).front();
        CreateAnotherPool(env, 2);

        const auto info = env.GetGroupInfo(part);
        const TVDiskID vdiskId = info->GetVDiskId(0);
        ui32 nodeId, pdiskId, vslotId;
        std::tie(nodeId, pdiskId, vslotId) = DecomposeVDiskServiceId(info->GetActorId(0));
        env.SetVDiskReadOnly(nodeId, pdiskId, vslotId, vdiskId, true);
        env.Sim(TDuration::Seconds(10));

        // the VDisk as BS_CONTROLLER publishes it to its node
        std::optional<NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk> published;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (const auto& vdisk : record.GetServiceSet().GetVDisks()) {
                    const auto& location = vdisk.GetVDiskLocation();
                    if (location.GetNodeID() == nodeId && location.GetPDiskID() == pdiskId &&
                            location.GetVDiskSlotID() == vslotId) {
                        published = vdisk;
                    }
                }
            }
            return true;
        };
        const auto response = MoveGroups(env, 1, 2, {proxy});
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(10));
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(published);
        UNIT_ASSERT_GT(VDiskIDFromVDiskID(published->GetVDiskID()).GroupGeneration, vdiskId.GroupGeneration);
        UNIT_ASSERT_C(published->GetReadOnly(), published->DebugString());

        // the VDisk with the new generation refuses writes
        const TVDiskID movedVDiskId = env.GetGroupInfo(part)->GetVDiskId(0);
        UNIT_ASSERT_GT(movedVDiskId.GroupGeneration, vdiskId.GroupGeneration);
        env.WithQueueId(movedVDiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            const TString data = "data";
            env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVPut(TLogoBlobID(1, 1, 1, 0,
                data.size(), 0, 1), TRope(data), movedVDiskId, false, nullptr, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false)), queueId.NodeId());
            const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge);
            const auto& record = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrProto::ERROR, record.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(record.GetErrorReason(), "read-only");
        });
    }

    // the VDisks of the moved group are laid out by the geometry of its new pool, so it must be the same as the one of
    // the group; otherwise BS_CONTROLLER would place the VDisks beyond the bounds of the geometry when loading its state
    Y_UNIT_TEST(MovingToOtherGeometryIsRefused) {
        TEnvironmentSetup env({
            .NodeCount = 18, // nine nodes in each pile
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                location.SetBridgePileName(TStringBuilder() << "pile_" << (nodeId - 1) / 9 + 1);
                location.SetDataCenter("my_dc");
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        });
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const auto parts = GetPileGroups(env, proxy);

        // the pool with nine fail domains, one more than the erasure needs, and the empty one of another erasure, which
        // has the same geometry by default
        CreateAnotherPool(env, 2, [](auto& pool) {
            auto *geometry = pool.MutableGeometry();
            geometry->SetNumFailRealms(1);
            geometry->SetNumFailDomainsPerFailRealm(9);
            geometry->SetNumVDisksPerFailDomain(1);
        });
        CreateAnotherPool(env, 3, [](auto& pool) {
            pool.SetErasureSpecies("mirror-3-dc");
            pool.SetNumGroups(0);
        });
        env.Sim(TDuration::Seconds(30));
        const auto wideProxies = GetProxyGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(wideProxies.size(), 1);
        const ui32 wide = wideProxies.front();
        const auto wideParts = GetPileGroups(env, wide);

        TDataChecker checker(env);
        checker.Write(proxy);
        checker.Write(wide);

        // neither the wide group goes to the pool of eight fail domains, nor the other one in the opposite direction or
        // to the pool of mirror-3-dc
        for (const auto& [origin, target, groupId] : {std::make_tuple(2, 1, wide), std::make_tuple(1, 2, proxy),
                std::make_tuple(1, 3, proxy)}) {
            const auto response = MoveGroups(env, origin, target, {groupId});
            UNIT_ASSERT(!response.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "doesn't match the erasure and geometry");
        }
        const auto config = env.FetchBaseConfig();
        for (const ui32 groupId : {proxy, parts[0], parts[1]}) {
            UNIT_ASSERT_VALUES_EQUAL_C(FindGroup(config, groupId).GetStoragePoolId(), 1, "GroupId# " << groupId);
        }
        for (const ui32 groupId : {wide, wideParts[0], wideParts[1]}) {
            UNIT_ASSERT_VALUES_EQUAL_C(FindGroup(config, groupId).GetStoragePoolId(), 2, "GroupId# " << groupId);
        }
        RestartControllerAndCheck(env, checker);
    }

    // without explicit groups MoveGroups moves the parts too, and they must not change NumGroups of the pools
    Y_UNIT_TEST(MovingAllGroupsKeepsNumGroups) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const auto parts = GetPileGroups(env, proxy);
        CreateAnotherPool(env, 2);

        TDataChecker checker(env);
        checker.Write(proxy);

        const auto response = MoveGroups(env, 1, 2, {});
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const auto config = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(FindGroup(config, proxy).GetStoragePoolId(), 2);
        for (const ui32 part : parts) {
            UNIT_ASSERT_VALUES_EQUAL_C(FindGroup(config, part).GetStoragePoolId(), 2, "GroupId# " << part);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 1).GetNumGroups(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).GetNumGroups(), 2);

        const auto deletion = DeletePool(env, 1);
        UNIT_ASSERT_C(deletion.GetSuccess(), deletion.GetErrorDescription());
        checker.Write(proxy);
        RestartControllerAndCheck(env, checker);
    }

    Y_UNIT_TEST(DeletingPoolDeletesBridgedGroups) {
        TEnvironmentSetup env(MakeSettings());
        CreatePoolAndGetGroup(env, 2);
        const auto groups = env.GetGroups();
        std::vector<ui32> all(groups.begin(), groups.end());
        for (const ui32 groupId : groups) {
            const auto parts = GetPileGroups(env, groupId);
            all.insert(all.end(), parts.begin(), parts.end());
        }
        CreateAnotherPool(env, 2);
        const auto keptGroups = GetProxyGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(keptGroups.size(), 1);
        const ui32 kept = keptGroups.front();
        TDataChecker checker(env);
        checker.Write(kept);

        const auto response = DeletePool(env, 1);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const auto config = env.FetchBaseConfig();
        for (const ui32 groupId : all) {
            UNIT_ASSERT_C(!HasGroup(config, groupId), "GroupId# " << groupId);
        }
        RestartControllerAndCheck(env, checker);
    }
}
