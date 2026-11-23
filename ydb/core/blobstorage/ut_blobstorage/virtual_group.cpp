#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(VirtualGroup) {

    // Regression test for a deleted virtual group left bound to its storage pool. A virtual group whose setup failed
    // and which was then cancelled is deleted by its setup machine. That deletion did not unbind the group from the
    // pool, so the next DefineStoragePool of that pool aborted BS_CONTROLLER on the missing group, and the pool kept
    // counting the group in NumGroups (which then made the fitter create a physical group in its place).
    Y_UNIT_TEST(CancelledGroupLeavesStoragePool) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{}); // without Hive, so setting up a virtual group fails
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));

        auto readPool = [&] {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableReadStoragePool();
            cmd->SetBoxId(1);
            cmd->AddName(env.StoragePoolName);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);
            return response.GetStatus(0).GetStoragePool(0);
        };

        auto findGroup = [&](ui32 groupId) -> std::optional<NKikimrBlobStorage::TBaseConfig::TGroup> {
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto& group : baseConfig.GetGroup()) {
                if (group.GetGroupId() == groupId) {
                    return group;
                }
            }
            return std::nullopt;
        };

        ui32 groupId;
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableAllocateVirtualGroup();
            cmd->SetName("vg");
            cmd->SetDatabase("/" + env.DomainName);
            cmd->SetStoragePoolName(env.StoragePoolName);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            groupId = response.GetStatus(0).GetGroupId(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(readPool().GetNumGroups(), 2);

        env.Sim(TDuration::Seconds(5));
        {
            const auto group = findGroup(groupId);
            UNIT_ASSERT(group);
            UNIT_ASSERT_EQUAL(group->GetVirtualGroupInfo().GetState(), NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED);
        }

        {
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableCancelVirtualGroup()->SetGroupId(groupId);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT(!findGroup(groupId));

        const auto pool = readPool();
        UNIT_ASSERT_VALUES_EQUAL(pool.GetNumGroups(), 1);

        // read-modify-write of the pool, as done by dstool pool set
        {
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        // check the persisted state too
        env.RestartNode(env.Settings.ControllerNodeId);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(readPool().GetNumGroups(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.FetchBaseConfig().GroupSize(), 1);
    }

}
