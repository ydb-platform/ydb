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
        for (const auto *group : GetPoolGroups(env.FetchBaseConfig(), storagePoolId)) {
            if (GetGroupInfo(env, group->GetGroupId()).HasBridgeGroupState()) {
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

    Y_UNIT_TEST(MovingProxyMovesItsParts) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 proxy = CreatePoolAndGetGroup(env);
        const auto parts = GetPileGroups(env, proxy);
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
        UNIT_ASSERT(GetPoolGroups(config, 1).empty());
        // NumGroups counts bridge proxy groups, not their parts
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 1).GetNumGroups(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).GetNumGroups(), 2);

        // the origin pool is empty now, so deleting it leaves the moved group intact
        const auto deletion = DeletePool(env, 1);
        UNIT_ASSERT_C(deletion.GetSuccess(), deletion.GetErrorDescription());
        const auto after = env.FetchBaseConfig();
        UNIT_ASSERT(HasGroup(after, proxy));
        for (const ui32 part : parts) {
            UNIT_ASSERT(HasGroup(after, part));
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
