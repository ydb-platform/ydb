#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(SpaceCheckForDiskReassign) {

    Y_UNIT_TEST(Basic) {
        using TPDiskId = TNodeWardenMockActor::TPDiskId;
        using TVSlotId = TNodeWardenMockActor::TVSlotId;

        TNodeWardenMockActor::TSetup::TPtr setup = MakeIntrusive<TNodeWardenMockActor::TSetup>();
        setup->TabletId = MakeBSControllerID();

        std::vector<std::vector<ui32>> driveSize = {
            // 8 nodes
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            {10, 10, 10, 10, 8, 8},
            // 8 nodes added
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            {10, 10, 6, 6, 8, 8},
            // some random stuff (8 nodes)
            {10, 10, 10, 6, 6},
            {10, 10, 10, 6, 6},
            {10, 10, 10, 8, 8},
            {10, 10, 10, 6},
            {10, 10, 10, 6},
            {10, 10, 10, 8},
            {10, 10, 10, 8},
            {10, 10, 10},
        };

        const ui32 numNodes = driveSize.size();
        ui32 numDrives = 0;

        ui64 totalSize = 0;

        NKikimrBlobStorage::TConfigRequest request;
        {
            std::map<std::vector<ui32>, ui64> configs;
            ui64 hostConfigId = 0;

            NKikimrBlobStorage::TDefineBox box;
            box.SetBoxId(1);

            ui32 nodeId = 1;
            for (const std::vector<ui32>& node : driveSize) {
                ui64& id = configs[node];
                NKikimrBlobStorage::TDefineHostConfig *cmd = nullptr;
                if (!id) {
                    id = ++hostConfigId;
                    cmd = request.AddCommand()->MutableDefineHostConfig();
                    cmd->SetHostConfigId(id);
                }
                for (ui32 i = 0; i < node.size(); ++i) {
                    TString path = TStringBuilder() << "/dev/disk/by-partlabel/testing_hdd_" << i;
                    if (cmd) {
                        auto *drive = cmd->AddDrive();
                        drive->SetPath(path);
                        drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
                        drive->MutablePDiskConfig()->SetExpectedSlotCount(node[i]);
                    }
                    setup->PDisks.emplace(std::make_tuple(nodeId, path), TNodeWardenMockActor::TSetup::TPDiskInfo{
                        node[i] * (ui64)1000000000000}); // TB
                    totalSize += node[i];
                    ++numDrives;
                }
                auto *host = box.AddHost();
                host->MutableKey()->SetNodeId(nodeId);
                host->SetHostConfigId(id);
                ++nodeId;
            }

            request.AddCommand()->MutableDefineBox()->CopyFrom(box);
        }

        TEnvironmentSetup env(numNodes, setup);

        // provide time for PDisks to report their metrics
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Minutes(1));

        const ui32 numGroups = numDrives * 6 / 8;

        request.Clear();
        auto *cmd2 = request.AddCommand()->MutableDefineStoragePool();
        cmd2->SetBoxId(1);
        cmd2->SetStoragePoolId(1);
        cmd2->SetName("storage-pool-1");
        cmd2->SetErasureSpecies("block-4-2");
        cmd2->SetVDiskKind("Default");
        cmd2->SetNumGroups(numGroups);
        cmd2->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);

        const ui32 numSlots = numGroups * 8;
        totalSize *= (ui64)1000000000000;

        const ui64 maxSlotSize = totalSize / numSlots;

        for (ui32 i = 0, groupId = 0x82000000; i < numGroups; ++i, ++groupId) {
            const ui64 slotSize =
                i == 0    ? maxSlotSize * 2        :
                i % 6 < 3 ? maxSlotSize * 50 / 100 :
                i % 6 < 4 ? maxSlotSize * 75 / 100 :
                i % 6 < 5 ? maxSlotSize * 80 / 100 :
                            maxSlotSize * 90 / 100;

            setup->Groups[groupId] = {slotSize};
            cmd2->AddExpectedGroupSlotSize(slotSize);
        }

        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        request.Clear();
        request.AddCommand()->MutableEnableSelfHeal()->SetEnable(true);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());


        ui32 numIterations = 0;
        ui32 numCyclesWithoutProgress = 0;
        constexpr ui32 maxCyclesWithoutProgress = 1; // mins after full replication
        constexpr ui32 targetIterations = 50;

        for (;;) {
            request.Clear();
            request.AddCommand()->MutableQueryBaseConfig();
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            std::unordered_set<TPDiskId, TPDiskId::THash> faultyPDiskIds;
            std::vector<TPDiskId> pdiskIds;
            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                pdiskIds.push_back(pdiskId);
                if (pdisk.GetDriveStatus() == NKikimrBlobStorage::EDriveStatus::FAULTY) {
                    faultyPDiskIds.insert(pdiskId);
                }
            }
            bool notReady = false;
            bool faultyVSlots = false;
            for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
                const auto& id = vslot.GetVSlotId();
                const TVSlotId vslotId(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId());
                if (faultyPDiskIds.count(vslotId)) {
                    faultyVSlots = true;
                }
                if (vslot.GetStatus() != "READY") {
                    notReady = true;
                }
            }
            if (notReady) {
                env.Sim(TDuration::Seconds(30));
                continue;
            }

            if (faultyVSlots && numCyclesWithoutProgress < maxCyclesWithoutProgress) {
                env.Sim(TDuration::Minutes(1));
                ++numCyclesWithoutProgress;
                continue;
            }

            request.Clear();
            auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
            if (faultyPDiskIds.size() >= 8 || numCyclesWithoutProgress >= maxCyclesWithoutProgress) {
                auto it = faultyPDiskIds.begin();
                std::advance(it, RandomNumber(faultyPDiskIds.size()));
                Cerr << "Making " << *it << " a normal one" << Endl;
                cmd->MutableHostKey()->SetNodeId(it->NodeId);
                cmd->SetPDiskId(it->PDiskId);
                cmd->SetStatus(NKikimrBlobStorage::EDriveStatus::ACTIVE);
            } else {
                size_t index = RandomNumber(pdiskIds.size());
                const auto& pdiskId = pdiskIds[index];
                Cerr << "Making " << pdiskId << " a faulty one" << Endl;
                cmd->MutableHostKey()->SetNodeId(pdiskId.NodeId);
                cmd->SetPDiskId(pdiskId.PDiskId);
                cmd->SetStatus(NKikimrBlobStorage::EDriveStatus::FAULTY);
            }
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            if (numCyclesWithoutProgress < maxCyclesWithoutProgress) {
                ++numIterations;
            }
            Cerr << "numIterations# " << numIterations << " numFaulty# " << faultyPDiskIds.size()
                << " numCyclesWithoutProgress# " << numCyclesWithoutProgress << Endl;
            numCyclesWithoutProgress = 0;
            if (numIterations == targetIterations) {
                break;
            }
        }
    }

}
