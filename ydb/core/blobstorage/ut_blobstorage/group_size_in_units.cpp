#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(GroupSizeInUnits) {

    using namespace NKikimrBlobStorage;

    Y_UNIT_TEST(SimplestErasureNone) {
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });

        for (ui32 nodeId : env.Runtime->GetNodes()) {
            TActorId whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
            TActorId whiteboardActor = env.Runtime->Register(NNodeWhiteboard::CreateNodeWhiteboardService(), nodeId);
            env.Runtime->RegisterService(whiteboardId, whiteboardActor);
        }

        // Init: 1 group, 1 vdisk, 1 pdisk
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(env.PDiskMockStates.size(), 1);
        const TPDiskMockState& pdiskMockState = *env.PDiskMockStates.begin()->second;
        UNIT_ASSERT_VALUES_EQUAL(pdiskMockState.GetNumActiveSlots(), 1);

        // Fetch BaseConfig
        TBaseConfig baseConfig_rev1 = env.FetchBaseConfig();

        // Validate BaseConfig.PDisk.Config.SlotSizeInUnits
        UNIT_ASSERT_VALUES_EQUAL(baseConfig_rev1.PDiskSize(), 1);
        const TBaseConfig_TPDisk& pdiskBaseConfig = baseConfig_rev1.GetPDisk(0);
        const ui32 nodeId = pdiskBaseConfig.GetNodeId();
        const ui32 pdiskId = pdiskBaseConfig.GetPDiskId();
        UNIT_ASSERT_VALUES_EQUAL(pdiskBaseConfig.GetPDiskConfig().GetSlotSizeInUnits(), 0u);

        {
            // Validate BaseConfig.Group.GroupSizeInUnits
            auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            auto groupInfo = env.GetGroupInfo(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(groupInfo->GroupGeneration, 1);
            UNIT_ASSERT_VALUES_EQUAL(groupInfo->GroupSizeInUnits, 0u);

            // Validate BaseConfig.VSlot.GroupSizeInUnits
            UNIT_ASSERT_VALUES_EQUAL(baseConfig_rev1.VSlotSize(), 1);
            const TBaseConfig_TVSlot& vslotBaseConfig = baseConfig_rev1.GetVSlot(0);
            const NKikimrBlobStorage::TVSlotId vslotId = vslotBaseConfig.GetVSlotId();
            UNIT_ASSERT_VALUES_EQUAL(vslotId.GetNodeId(), nodeId);
            UNIT_ASSERT_VALUES_EQUAL(vslotId.GetPDiskId(), pdiskId);
            UNIT_ASSERT_VALUES_EQUAL(vslotBaseConfig.GetGroupId(), groupInfo->GroupID.GetRawId());
            UNIT_ASSERT_VALUES_EQUAL(vslotBaseConfig.GetGroupGeneration(), groupInfo->GroupGeneration);
        }

        {
            // Reqest ReadStoragePool
            NKikimrBlobStorage::TConfigRequest readStoragePoolRequest;
            auto* cmd1 = readStoragePoolRequest.AddCommand()->MutableReadStoragePool();
            cmd1->SetBoxId(1);
            NKikimrBlobStorage::TConfigResponse readStoragePoolResponse = env.Invoke(readStoragePoolRequest);
            UNIT_ASSERT_C(readStoragePoolResponse.GetSuccess(), readStoragePoolResponse.GetErrorDescription());
            UNIT_ASSERT_C(readStoragePoolResponse.GetStatus(0).GetSuccess(), readStoragePoolResponse.GetStatus(0).GetErrorDescription());

            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.StatusSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.GetStatus(0).StoragePoolSize(), 1);
            const TDefineStoragePool storagePool = readStoragePoolResponse.GetStatus(0).GetStoragePool(0);
            UNIT_ASSERT_VALUES_EQUAL(storagePool.GetDefaultGroupSizeInUnits(), 0u);

            // Request DefineStoragePool DefaultGroupSizeInUnits# 2 NumGroups# 2
            NKikimrBlobStorage::TConfigRequest defineStoragePoolRequest;
            auto* cmd2 = defineStoragePoolRequest.AddCommand()->MutableDefineStoragePool();
            cmd2->CopyFrom(storagePool);
            cmd2->SetNumGroups(2);
            cmd2->SetDefaultGroupSizeInUnits(2u);
            cmd2->SetItemConfigGeneration(1);
            NKikimrBlobStorage::TConfigResponse defineStoragePoolResponse = env.Invoke(defineStoragePoolRequest);
            UNIT_ASSERT_C(defineStoragePoolResponse.GetSuccess(), defineStoragePoolResponse.GetErrorDescription());
            UNIT_ASSERT_C(defineStoragePoolResponse.GetStatus(0).GetSuccess(), defineStoragePoolResponse.GetStatus(0).GetErrorDescription());
            env.Sim(TDuration::Minutes(1));

            // Validate NumActiveSlots
            UNIT_ASSERT_VALUES_EQUAL(pdiskMockState.GetNumActiveSlots(), 3);
        }

        {
            // Validate BaseConfig.Groups
            auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
            auto group0 = env.GetGroupInfo(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(group0->GroupGeneration, 1);
            UNIT_ASSERT_VALUES_EQUAL(group0->GroupSizeInUnits, 0u);
            auto group1 = env.GetGroupInfo(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(group1->GroupGeneration, 1);
            UNIT_ASSERT_VALUES_EQUAL(group1->GroupSizeInUnits, 2u);

            // Validate BaseConfig.VSlot.GroupSizeInUnits
            TBaseConfig baseConfig_rev2 = env.FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(baseConfig_rev2.VSlotSize(), 2);
            THashMap<ui32, const TBaseConfig_TVSlot*> vslotsByGroupId;
            for (size_t i = 0; i < baseConfig_rev2.VSlotSize(); ++i) {
                const auto& vslot = baseConfig_rev2.GetVSlot(i);
                vslotsByGroupId[vslot.GetGroupId()] = &vslot;
            }

            const auto* vslot0 = vslotsByGroupId.at(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(vslot0->GetGroupGeneration(), group0->GroupGeneration);
            const auto* vslot1 = vslotsByGroupId.at(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(vslot1->GetGroupGeneration(), group1->GroupGeneration);
        }
    }
}
