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
        Cerr << baseConfig_rev1.DebugString() << Endl;

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
            Cerr << "TReadStoragePool response: " << readStoragePoolResponse.DebugString() << Endl;
            UNIT_ASSERT_C(readStoragePoolResponse.GetSuccess(), readStoragePoolResponse.GetErrorDescription());
            UNIT_ASSERT_C(readStoragePoolResponse.GetStatus(0).GetSuccess(), readStoragePoolResponse.GetStatus(0).GetErrorDescription());

            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.StatusSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.GetStatus(0).StoragePoolSize(), 1);
            const TDefineStoragePool storagePool = readStoragePoolResponse.GetStatus(0).GetStoragePool(0);
            UNIT_ASSERT_VALUES_EQUAL(storagePool.GetDefaultGroupSizeInUnits(), 0u);

            // Request DefineStoragePool NumGroups# 2 DefaultGroupSizeInUnits# 2
            NKikimrBlobStorage::TConfigRequest defineStoragePoolRequest;
            auto* cmd2 = defineStoragePoolRequest.AddCommand()->MutableDefineStoragePool();
            cmd2->CopyFrom(storagePool);
            cmd2->SetNumGroups(2);
            cmd2->SetDefaultGroupSizeInUnits(2u);
            cmd2->SetItemConfigGeneration(1);
            NKikimrBlobStorage::TConfigResponse defineStoragePoolResponse = env.Invoke(defineStoragePoolRequest);
            Cerr << "TDefineStoragePool response: " << defineStoragePoolResponse.DebugString() << Endl;
            UNIT_ASSERT_C(defineStoragePoolResponse.GetSuccess(), defineStoragePoolResponse.GetErrorDescription());
            UNIT_ASSERT_C(defineStoragePoolResponse.GetStatus(0).GetSuccess(), defineStoragePoolResponse.GetStatus(0).GetErrorDescription());
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

            UNIT_ASSERT_VALUES_EQUAL(pdiskMockState.GetNumActiveSlots(), 3);

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

        // Request VDiskState from NodeWhiteboard
        auto requestWhiteboardVDiskState = [&]() {
            TActorId whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
            TActorId edge = env.Runtime->AllocateEdgeActor(nodeId);

            auto vdiskRequest = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>();
            Cerr << "Sending TEvVDiskStateRequest# " << vdiskRequest->ToString()
                << " To# " << whiteboardId
                << Endl;
            env.Runtime->Send(new IEventHandle(whiteboardId, edge, vdiskRequest.release()), nodeId);

            Cerr << "Waiting for response..." << Endl;
            auto vdiskResponse = env.WaitForEdgeActorEvent<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse>(
                edge, true, env.Runtime->GetClock() + TDuration::Seconds(1));
            UNIT_ASSERT(vdiskResponse);
            Cerr << "TEvVDiskStateResponse received successfully " << vdiskResponse->ToString() << Endl;
            return vdiskResponse->Get()->Record;
        };

        {
            // Validate Whiteboard TVDiskStateInfo
            const NKikimrWhiteboard::TEvVDiskStateResponse& record = requestWhiteboardVDiskState();
            UNIT_ASSERT_VALUES_EQUAL(record.VDiskStateInfoSize(), 2);

            THashMap<ui32, const NKikimrWhiteboard::TVDiskStateInfo*> vdisksByGroupId;
            for (size_t i = 0; i < record.VDiskStateInfoSize(); ++i) {
                const auto& vdisk = record.GetVDiskStateInfo(i);
                vdisksByGroupId[vdisk.GetVDiskId().GetGroupID()] = &vdisk;
            }

            auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

            const auto* vdisk0 = vdisksByGroupId.at(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(vdisk0->GetGroupSizeInUnits(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(vdisk0->GetPDiskId(), pdiskId);
            const auto* vdisk1 = vdisksByGroupId.at(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(vdisk1->GetGroupSizeInUnits(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(vdisk1->GetPDiskId(), pdiskId);
        }

        {
            // Request ChangeGroupSizeInUnits BoxId# 1 StoragePoolId# 1 SizeInUnits# 9
            NKikimrBlobStorage::TConfigRequest changeGroupSizeInUnitsRequest;
            auto* cmd = changeGroupSizeInUnitsRequest.AddCommand()->MutableChangeGroupSizeInUnits();
            cmd->SetBoxId(1);
            cmd->SetStoragePoolId(1);
            cmd->SetStoragePoolGeneration(2);
            cmd->SetSizeInUnits(9u);
            NKikimrBlobStorage::TConfigResponse changeGroupSizeInUnitsResponse = env.Invoke(changeGroupSizeInUnitsRequest);
            Cerr << "ChangeGroupSizeInUnits response: " << changeGroupSizeInUnitsResponse.DebugString() << Endl;
            UNIT_ASSERT_C(changeGroupSizeInUnitsResponse.GetSuccess(), changeGroupSizeInUnitsResponse.GetErrorDescription());
            UNIT_ASSERT_C(changeGroupSizeInUnitsResponse.GetStatus(0).GetSuccess(), changeGroupSizeInUnitsResponse.GetStatus(0).GetErrorDescription());
        }

        {
            // Validate BaseConfig.Group entries
            auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
            auto group0 = env.GetGroupInfo(groups[0]); // implies FetchBaseConfig
            UNIT_ASSERT_VALUES_EQUAL(group0->GroupGeneration, 2);
            UNIT_ASSERT_VALUES_EQUAL(group0->GroupSizeInUnits, 9u);
            auto group1 = env.GetGroupInfo(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(group1->GroupGeneration, 2);
            UNIT_ASSERT_VALUES_EQUAL(group1->GroupSizeInUnits, 9u);

            // Validate BaseConfig.VSlot entries
            TBaseConfig baseConfig_rev3 = env.FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(baseConfig_rev3.VSlotSize(), 2);

            THashMap<ui32, const TBaseConfig_TVSlot*> vslotsByGroupId;
            for (size_t i = 0; i < baseConfig_rev3.VSlotSize(); ++i) {
                const auto& vslot = baseConfig_rev3.GetVSlot(i);
                vslotsByGroupId[vslot.GetGroupId()] = &vslot;
            }

            const auto* vslot0 = vslotsByGroupId.at(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(vslot0->GetGroupGeneration(), group0->GroupGeneration);
            const auto* vslot1 = vslotsByGroupId.at(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(vslot1->GetGroupGeneration(), group1->GroupGeneration);

            UNIT_ASSERT_VALUES_EQUAL(pdiskMockState.GetNumActiveSlots(), 18);
        }

        {
            // Validate Whiteboard TVDiskStateInfo after size change
            const NKikimrWhiteboard::TEvVDiskStateResponse& record = requestWhiteboardVDiskState();
            UNIT_ASSERT_VALUES_EQUAL(record.VDiskStateInfoSize(), 2);

            // Create map of VDisks by GroupID
            THashMap<ui32, const NKikimrWhiteboard::TVDiskStateInfo*> vdisksByGroupId;
            for (size_t i = 0; i < record.VDiskStateInfoSize(); ++i) {
                const auto& vdisk = record.GetVDiskStateInfo(i);
                vdisksByGroupId[vdisk.GetVDiskId().GetGroupID()] = &vdisk;
            }

            auto groups = env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

            const auto* vdisk0 = vdisksByGroupId.at(groups[0]);
            UNIT_ASSERT_VALUES_EQUAL(vdisk0->GetGroupSizeInUnits(), 9u);
            const auto* vdisk1 = vdisksByGroupId.at(groups[1]);
            UNIT_ASSERT_VALUES_EQUAL(vdisk1->GetGroupSizeInUnits(), 9u);
        }

        {
            // Reqest ReadStoragePool again
            NKikimrBlobStorage::TConfigRequest readStoragePoolRequest;
            auto* cmd1 = readStoragePoolRequest.AddCommand()->MutableReadStoragePool();
            cmd1->SetBoxId(1);
            NKikimrBlobStorage::TConfigResponse readStoragePoolResponse = env.Invoke(readStoragePoolRequest);
            Cerr << "TReadStoragePool response: " << readStoragePoolResponse.DebugString() << Endl;
            UNIT_ASSERT_C(readStoragePoolResponse.GetSuccess(), readStoragePoolResponse.GetErrorDescription());
            UNIT_ASSERT_C(readStoragePoolResponse.GetStatus(0).GetSuccess(), readStoragePoolResponse.GetStatus(0).GetErrorDescription());

            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.StatusSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readStoragePoolResponse.GetStatus(0).StoragePoolSize(), 1);
            const TDefineStoragePool storagePool = readStoragePoolResponse.GetStatus(0).GetStoragePool(0);
            UNIT_ASSERT_VALUES_EQUAL(storagePool.GetDefaultGroupSizeInUnits(), 2u);

            // Try to allocate more groups than possible (PDisk has 16 slots total)
            NKikimrBlobStorage::TConfigRequest defineStoragePoolRequest;
            auto* cmd = defineStoragePoolRequest.AddCommand()->MutableDefineStoragePool();
            cmd->CopyFrom(storagePool);
            cmd->SetItemConfigGeneration(3);
            cmd->SetDefaultGroupSizeInUnits(1u);
            cmd->SetNumGroups(3);

            NKikimrBlobStorage::TConfigResponse defineStoragePoolResponse = env.Invoke(defineStoragePoolRequest);
            Cerr << "TDefineStoragePool response: " << defineStoragePoolResponse.DebugString() << Endl;

            // Verify failure
            UNIT_ASSERT(!defineStoragePoolResponse.GetSuccess());
            UNIT_ASSERT_STRINGS_EQUAL(
                defineStoragePoolResponse.GetErrorDescription(),
                "Group fit error BoxId# 1 StoragePoolId# 1 Error# failed to allocate group: no group options PDisks# {[(1:1000-s[18/16])]}"
            );

            // Verify existing groups remain unchanged
            UNIT_ASSERT_VALUES_EQUAL(pdiskMockState.GetNumActiveSlots(), 18);
        }
    }
}
