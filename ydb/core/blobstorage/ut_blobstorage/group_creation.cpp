#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage_common.h>

Y_UNIT_TEST_SUITE(GroupCreationInactive) {

    struct TTestCtx {
        std::unique_ptr<TEnvironmentSetup> Env;
        ui32 NumDCs = 3;
        ui32 NumNodesInDC = 3;
        ui32 DisksPerNode = 2;
        ui32 NumNodes = NumDCs * NumNodesInDC + 1; // last node hosts BSC

        TTestCtxs() {
            Env = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
                .NodeCount = NumNodes,
                .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                .ControllerNodeId = NumNodes,
                .LocationGenerator = [this](ui32 nodeId) {
                    NActorsInterconnect::TNodeLocation proto;
                    if (nodeId == NumNodes) {
                        proto.SetDataCenter("bsc");
                        proto.SetRack("bsc");
                        proto.SetUnit("1");
                    } else {
                        const ui32 dc = (nodeId - 1) / NumNodesInDC;
                        const ui32 rack = (nodeId - 1) % NumNodesInDC;
                        proto.SetDataCenter(ToString(dc));
                        proto.SetRack(ToString(rack));
                        proto.SetUnit("1");
                    }
                    return TNodeLocation(proto);
                },
            });
            Env->CreateBoxAndPool(DisksPerNode, /*numGroups=*/1, /*numStorageNodes=*/NumNodes - 1);
            Env->Sim(TDuration::Seconds(30));
        }

        ui32 NodeDC(ui32 nodeId) const {
            return (nodeId - 1) / NumNodesInDC;
        }

        void MarkDCInactive(ui32 dc) {
            auto config = Env->FetchBaseConfig();
            for (const auto& pdisk : config.GetPDisk()) {
                if (pdisk.GetNodeId() == NumNodes) {
                    continue;
                }
                if (NodeDC(pdisk.GetNodeId()) == dc) {
                    Env->UpdateDriveStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(),
                        NKikimrBlobStorage::EDriveStatus::INACTIVE,
                        NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
                }
            }
            Env->Sim(TDuration::Seconds(5));
        }

        NKikimrBlobStorage::TConfigResponse SetNumGroups(ui32 numGroups, ui32 itemConfigGeneration) {
            NKikimrBlobStorage::TConfigRequest readReq;
            auto* read = readReq.AddCommand()->MutableReadStoragePool();
            read->SetBoxId(1);
            auto readResp = Env->Invoke(readReq);
            UNIT_ASSERT_C(readResp.GetSuccess(), readResp.GetErrorDescription());
            UNIT_ASSERT_VALUES_EQUAL(readResp.GetStatus(0).StoragePoolSize(), 1);
            auto pool = readResp.GetStatus(0).GetStoragePool(0);

            NKikimrBlobStorage::TConfigRequest req;
            auto* cmd = req.AddCommand()->MutableDefineStoragePool();
            cmd->CopyFrom(pool);
            cmd->SetNumGroups(numGroups);
            cmd->SetItemConfigGeneration(itemConfigGeneration);
            return Env->Invoke(req);
        }
    };

    Y_UNIT_TEST(CreateGroupWithOneDcInactive) {
        TTestCtx ctx;

        auto initialGroups = ctx.Env->GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(initialGroups.size(), 1);

        ctx.MarkDCInactive(0);

        auto response = ctx.SetNumGroups(/*numGroups=*/2, /*itemConfigGeneration=*/1);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        ctx.Env->Sim(TDuration::Seconds(30));

        auto groups = ctx.Env->GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

        ui32 newGroupId = (groups[0] == initialGroups[0]) ? groups[1] : groups[0];
        auto baseConfig = ctx.Env->FetchBaseConfig();
        std::set<ui32> dcsCovered;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == newGroupId) {
                dcsCovered.insert(ctx.NodeDC(vslot.GetVSlotId().GetNodeId()));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(dcsCovered.size(), 3);
        UNIT_ASSERT(dcsCovered.count(0));
        UNIT_ASSERT(dcsCovered.count(1));
        UNIT_ASSERT(dcsCovered.count(2));
    }

    Y_UNIT_TEST(NoSlotReassignmentToInactivePdisk) {
        TTestCtx ctx;
        auto groups = ctx.Env->GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups[0];

        auto baseConfig = ctx.Env->FetchBaseConfig();

        std::optional<NKikimrBlobStorage::TBaseConfig::TVSlot> sourceSlot;
        std::set<std::pair<ui32, ui32>> usedPDisks;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == groupId) {
                usedPDisks.emplace(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
                if (!sourceSlot) {
                    sourceSlot = vslot;
                }
            }
        }
        UNIT_ASSERT(sourceSlot.has_value());
        const ui32 sourceDC = ctx.NodeDC(sourceSlot->GetVSlotId().GetNodeId());

        std::optional<NKikimrBlobStorage::TBaseConfig::TPDisk> targetPDisk;
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == ctx.NumNodes) {
                continue;
            }
            if (ctx.NodeDC(pdisk.GetNodeId()) != sourceDC) {
                continue;
            }
            if (usedPDisks.count({pdisk.GetNodeId(), pdisk.GetPDiskId()})) {
                continue;
            }
            targetPDisk = pdisk;
            break;
        }
        UNIT_ASSERT(targetPDisk);

        ctx.Env->UpdateDriveStatus(targetPDisk->GetNodeId(), targetPDisk->GetPDiskId(),
            NKikimrBlobStorage::EDriveStatus::INACTIVE,
            NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
        ctx.Env->Sim(TDuration::Seconds(5));

        NKikimrBlobStorage::TConfigRequest req;
        auto* reassign = req.AddCommand()->MutableReassignGroupDisk();
        reassign->SetGroupId(sourceSlot->GetGroupId());
        reassign->SetGroupGeneration(sourceSlot->GetGroupGeneration());
        reassign->SetFailRealmIdx(sourceSlot->GetFailRealmIdx());
        reassign->SetFailDomainIdx(sourceSlot->GetFailDomainIdx());
        reassign->SetVDiskIdx(sourceSlot->GetVDiskIdx());
        auto* target = reassign->MutableTargetPDiskId();
        target->SetNodeId(targetPDisk->GetNodeId());
        target->SetPDiskId(targetPDisk->GetPDiskId());

        auto response = ctx.Env->Invoke(req);
        UNIT_ASSERT_C(!response.GetSuccess(),
            "Reassignment to INACTIVE PDisk unexpectedly succeeded: "
                << response.DebugString());
    }

    Y_UNIT_TEST(CreateGroupStrictWhenActiveSufficient) {
        TTestCtx ctx;
        UNIT_ASSERT_VALUES_EQUAL(ctx.Env->GetGroups().size(), 1);

        auto response = ctx.SetNumGroups(/*numGroups=*/2, /*itemConfigGeneration=*/1);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        ctx.Env->Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(ctx.Env->GetGroups().size(), 2);
    }

}
