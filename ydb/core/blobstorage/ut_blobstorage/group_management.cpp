#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

Y_UNIT_TEST_SUITE(GroupManagement) {
    using TTestCtx = TTestCtxBase;
    Y_UNIT_TEST(CreateOnInactive) {
        TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureMirror3dc;
        TTestCtx ctx{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .ControllerNodeId = 1,
        }};

        ctx.Initialize();
        for (ui32 idx = 0; idx < 3; ++idx) {
            const auto& pdisk = ctx.BaseConfig.GetPDisk(idx);
            NKikimrBlobStorage::TConfigRequest request;
            auto* cmd = request.AddCommand()->MutableUpdateDriveStatus();
            cmd->MutableHostKey()->SetNodeId(pdisk.GetNodeId());
            cmd->SetPDiskId(pdisk.GetPDiskId());
            cmd->SetStatus(NKikimrBlobStorage::INACTIVE);
            auto response = ctx.Env->Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        auto createGroup = [&](bool shouldSuccess) {
            NKikimrBlobStorage::TConfigRequest request;
            auto* cmd = request.AddCommand()->MutableDefineStoragePool();
            cmd->SetBoxId(1);
            cmd->SetStoragePoolId(1);
            cmd->SetName(ctx.Env->StoragePoolName);
            cmd->SetKind(ctx.Env->StoragePoolName);
            cmd->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(erasure.GetErasure()));
            cmd->SetVDiskKind("Default");
            cmd->SetNumGroups(2);
            cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            cmd->SetItemConfigGeneration(1);
            auto response = ctx.Env->Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess() == shouldSuccess, response.GetErrorDescription());
        };

        createGroup(false);
        
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand();
            auto *us = cmd->MutableUpdateSettings();
            us->AddAllowSlotAllocationOnNonActive(true);
            auto response = ctx.Env->Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        createGroup(true);
    }
}
