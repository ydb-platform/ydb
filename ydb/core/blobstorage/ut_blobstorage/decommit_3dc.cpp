#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(Decommit3dc) {
    Y_UNIT_TEST(Test) {
        TEnvironmentSetup env{{
            .NodeCount = 12,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .NumDataCenters = 4,
        }};

        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand();
            auto *us = cmd->MutableUpdateSettings();
            us->AddEnableDonorMode(true);
            us->AddEnableSelfHeal(true);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        auto config = env.FetchBaseConfig();

        std::set<ui32> nodesToSettle;
        TString datacenterToSettle;
        for (const auto& node : config.GetNode()) {
            const auto& location = node.GetLocation();
            if (!datacenterToSettle) {
                datacenterToSettle = location.GetDataCenter();
            }
            if (datacenterToSettle == location.GetDataCenter()) {
                nodesToSettle.insert(node.GetNodeId());
            }
        }

        NKikimrBlobStorage::TConfigRequest request;

        std::set<std::pair<ui32, ui32>> pdisksToSettle;
        for (const auto& pdisk : config.GetPDisk()) {
            if (nodesToSettle.count(pdisk.GetNodeId())) {
                pdisksToSettle.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
                auto *cmd = request.AddCommand();
                auto *ds = cmd->MutableUpdateDriveStatus();
                ds->MutableHostKey()->SetNodeId(pdisk.GetNodeId());
                ds->SetPDiskId(pdisk.GetPDiskId());
                ds->SetStatus(NKikimrBlobStorage::EDriveStatus::DECOMMIT_PENDING);
            }
        }

        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        std::set<std::pair<ui32, ui32>> movedOutPDisks;
        for (const auto& [nodeId, pdiskId] : pdisksToSettle) {
            request.Clear();
            auto *cmd = request.AddCommand();
            auto *ds = cmd->MutableUpdateDriveStatus();
            ds->MutableHostKey()->SetNodeId(nodeId);
            ds->SetPDiskId(pdiskId);
            ds->SetStatus(NKikimrBlobStorage::EDriveStatus::DECOMMIT_IMMINENT);
            movedOutPDisks.emplace(nodeId, pdiskId);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            env.Sim(TDuration::Seconds(60));
            auto config = env.FetchBaseConfig();
            for (const auto& vslot : config.GetVSlot()) {
                const auto& vslotId = vslot.GetVSlotId();
                UNIT_ASSERT(!movedOutPDisks.count({vslotId.GetNodeId(), vslotId.GetPDiskId()}));
            }
        }
    }
}
