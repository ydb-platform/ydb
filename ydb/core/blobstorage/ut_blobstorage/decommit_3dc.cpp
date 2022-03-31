#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(Decommit3dc) {
    Y_UNIT_TEST(Test) {
        for (ui32 numNodes : {12, 16})
        for (ui32 numGroups : {1, 7})
        for (bool resetToNone : {false, true})
        for (ui32 numDecommitted = 0; numDecommitted <= numNodes / 4; ++numDecommitted) {
            Cerr << "numNodes# " << numNodes
                << " numGroups# " << numGroups
                << " resetToNone# " << (resetToNone ? "true" : "false")
                << " numDecommitted# " << numDecommitted
                << Endl;

            TEnvironmentSetup env{{
                .NodeCount = numNodes,
                .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                .NumDataCenters = 4,
            }};

            env.UpdateSettings(true, true);
            env.CreateBoxAndPool(1, numGroups);
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

            std::set<std::pair<ui32, ui32>> pdisksToSettle;
            for (const auto& pdisk : config.GetPDisk()) {
                if (nodesToSettle.count(pdisk.GetNodeId())) {
                    pdisksToSettle.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
                    env.UpdateDriveStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(), {},
                        NKikimrBlobStorage::EDecommitStatus::DECOMMIT_PENDING);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(pdisksToSettle.size(), numNodes / 4);
            std::set<std::pair<ui32, ui32>> movedOutPDisks;
            auto it = pdisksToSettle.begin();
            for (ui32 i = 0; i < numDecommitted; ++i, ++it) {
                UNIT_ASSERT(it != pdisksToSettle.end());
                movedOutPDisks.insert(*it);
                env.UpdateDriveStatus(it->first, it->second, {}, NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT);

                env.Sim(TDuration::Seconds(60));
                auto config = env.FetchBaseConfig();
                for (const auto& vslot : config.GetVSlot()) {
                    const auto& vslotId = vslot.GetVSlotId();
                    UNIT_ASSERT(!movedOutPDisks.count({vslotId.GetNodeId(), vslotId.GetPDiskId()}));
                }
            }
            if (resetToNone) {
                for (const auto& [nodeId, pdiskId] : pdisksToSettle) {
                    env.UpdateDriveStatus(nodeId, pdiskId, {}, NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
                }
                movedOutPDisks.clear();
            }
            for (const auto& [nodeId, pdiskId] : pdisksToSettle) {
                Cerr << "nodeId# " << nodeId << " pdiskId# " << pdiskId << Endl;

                env.UpdateDriveStatus(nodeId, pdiskId, NKikimrBlobStorage::EDriveStatus::FAULTY, {});
                movedOutPDisks.emplace(nodeId, pdiskId);

                env.Sim(TDuration::Seconds(90));
                auto config = env.FetchBaseConfig();
                for (const auto& vslot : config.GetVSlot()) {
                    const auto& vslotId = vslot.GetVSlotId();
                    Cerr << vslotId.GetNodeId() << ":" << vslotId.GetPDiskId() << " " << vslot.GetFailRealmIdx()
                        << " " << vslot.GetFailDomainIdx() << Endl;
                    UNIT_ASSERT(!movedOutPDisks.count({vslotId.GetNodeId(), vslotId.GetPDiskId()}) ||
                        (numNodes == 12 && numDecommitted == 0));
                }

                env.UpdateDriveStatus(nodeId, pdiskId, NKikimrBlobStorage::EDriveStatus::ACTIVE, {});
                movedOutPDisks.erase({nodeId, pdiskId});
            }
        }
    }
}
