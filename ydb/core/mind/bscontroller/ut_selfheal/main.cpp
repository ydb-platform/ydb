#include <ydb/core/mind/bscontroller/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include "env.h"

#include <ydb/core/mind/bscontroller/layout_helpers.h>
#include <ydb/core/util/pb.h>

Y_UNIT_TEST_SUITE(BsControllerTest) {

    void TestSelfHeal(const ui32 numDCs = 3, ui32 numRacksPerDC = 4, const ui32 numUnitsPerRack = 4, const ui32 numDisksPerNode = 2, const ui32 numGroups = 64,
            TString erasure = "block-4-2", TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block) {
        ui32 numNodes = numDCs * numRacksPerDC * numUnitsPerRack;
        auto locationGenerator = [=](ui32 nodeId) {
            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter(ToString((nodeId - 1) / (numUnitsPerRack * numRacksPerDC)));
            proto.SetRack(ToString((nodeId - 1) / numUnitsPerRack));
            proto.SetUnit(ToString((nodeId - 1)));
            return TNodeLocation(proto);
        };

        TEnvironmentSetup env(numNodes, locationGenerator);

        const TGroupGeometryInfo geom = CreateGroupGeometry(groupType);
        ui32 disksNum = geom.GetNumFailRealms() * geom.GetNumFailDomainsPerFailRealm() * geom.GetNumVDisksPerFailDomain();

        NKikimrBlobStorage::TConfigRequest request;
        TVector<TEnvironmentSetup::TDrive> drives;
        for (ui32 i = 0; i < numDisksPerNode; ++i) {
            drives.push_back({ .Path = "/dev/disk" + std::to_string(1 + i)});
        }
        env.DefineBox(1, drives, {{1, numNodes}}, &request);
        env.DefineStoragePool(1, 1, numGroups, NKikimrBlobStorage::ROT, {}, &request, erasure);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        env.WaitForNodeWardensToConnect();

        request.Clear();
        auto *cmd = request.AddCommand()->MutableEnableSelfHeal();
        cmd->SetEnable(true);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        std::set<TPDiskId> active, faulty;

        request = {};
        env.QueryBaseConfig(&request);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
            active.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
        }

        TString error;
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, response.GetStatus(0).GetBaseConfig(), true, error),
                "Initial group layout is incorrect, ErrorReason# " << error);

        UNIT_ASSERT_VALUES_EQUAL(active.size(), numNodes * numDisksPerNode);

        auto move = [&](auto& from, auto& to, NKikimrBlobStorage::EDriveStatus status) {
            auto it = from.begin();
            std::advance(it, RandomNumber(from.size()));
            Ctest << "PDisk# " << *it
                << " setting status to " << NKikimrBlobStorage::EDriveStatus_Name(status)
                << Endl;
            request = {};
            env.UpdateDriveStatus(*it, status, &request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            to.insert(from.extract(it));
        };

        for (size_t i = 0; i < numNodes; ++i) {
            env.Wait(TDuration::Seconds(300));
            if (faulty.size() < disksNum) {
                move(active, faulty, NKikimrBlobStorage::FAULTY);
            } else {
                move(faulty, active, NKikimrBlobStorage::ACTIVE);
            }
            env.Wait(TDuration::Seconds(300));

            request = {};
            env.QueryBaseConfig(&request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                if (pdisk.GetDriveStatus() == NKikimrBlobStorage::ACTIVE) {
                    UNIT_ASSERT(active.count(pdiskId));
                } else {
                    UNIT_ASSERT(pdisk.GetDriveStatus() == NKikimrBlobStorage::FAULTY);
                    UNIT_ASSERT(faulty.count(pdiskId));
                }
            }
            for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
                const auto& id = vslot.GetVSlotId();
                const TPDiskId pdiskId(id.GetNodeId(), id.GetPDiskId());
                if (!active.count(pdiskId)) {
                    Ctest << "active# { ";
                    for (auto id : active) {
                        Ctest << id.ToString() << " ";
                    }
                    Ctest << " }" << Endl;
                    Ctest << "faulty# { ";
                    for (auto id : faulty) {
                        Ctest << id.ToString() << " ";
                    }
                    Ctest << " }" << Endl;
                    Ctest << "pdiskId# " << pdiskId.ToString() << Endl;
                    UNIT_FAIL("non-active disk is present in group");
                }
            }
            UNIT_ASSERT_C(CheckBaseConfigLayout(geom, response.GetStatus(0).GetBaseConfig(), true, error), "Error on step# " << i
                << ", ErrorReason# " << error);
        }
    }

    Y_UNIT_TEST(SelfHealBlock4Plus2) {
        TestSelfHeal(1, 32, 1, 2, 64, "block-4-2", TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(SelfHealMirror3dc) {
        TestSelfHeal(3, 4, 3, 4, 128, "mirror-3-dc", TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(DecommitRejected) {
        for (const bool useRejected : {false, true}) {
            constexpr ui32 numNodes = 3 + 3 + 6 + 3;
            const ui32 nodeToDataCenter[numNodes] = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4};

            auto locationGenerator = [&](ui32 nodeId) {
                return TNodeLocation(ToString(nodeToDataCenter[nodeId - 1]), TString(), ToString(nodeId - 1), ToString(nodeId - 1));
            };

            TEnvironmentSetup env(numNodes, locationGenerator);

            NKikimrBlobStorage::TConfigRequest request;

            TVector<TEnvironmentSetup::TDrive> drives;
            drives.push_back({.Path = "/dev/disk"});
            env.DefineBox(1, drives, {{1, numNodes}}, &request);

            env.DefineStoragePool(1, 1, 1, drives[0].Type, {}, &request, "mirror-3-dc");

            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            request.Clear();
            auto *cmd = request.AddCommand()->MutableEnableSelfHeal();
            cmd->SetEnable(true);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            env.Sim(TDuration::Minutes(1));

            request.Clear();
            env.QueryBaseConfig(&request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            request.Clear();
            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                const ui32 nodeId = pdisk.GetNodeId();
                if (nodeId >= 7 && nodeId <= 9) {
                    auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
                    cmd->MutableHostKey()->SetNodeId(nodeId);
                    cmd->SetPDiskId(pdisk.GetPDiskId());
                    cmd->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT);
                }
                if (useRejected && nodeId >= 10 && nodeId <= 12) {
                    auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
                    cmd->MutableHostKey()->SetNodeId(nodeId);
                    cmd->SetPDiskId(pdisk.GetPDiskId());
                    cmd->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED);
                }
            }
            env.DefineStoragePool(1, 2, 1, drives[0].Type, {}, &request, "mirror-3-dc");
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            env.Sim(TDuration::Minutes(10));

            request.Clear();
            env.QueryBaseConfig(&request);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            THashSet<ui32> group0nodes, group1nodes;

            for (const auto& vdisk : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
                if (vdisk.GetGroupId() == 0x80000000) {
                    group0nodes.insert(vdisk.GetVSlotId().GetNodeId());
                } else {
                    group1nodes.insert(vdisk.GetVSlotId().GetNodeId());
                }
            }

            if (useRejected) {
                UNIT_ASSERT_EQUAL(group0nodes, (THashSet<ui32>{{1, 2, 3, 4, 5, 6, 13, 14, 15}}));
            } else {
                UNIT_ASSERT_EQUAL(group0nodes, (THashSet<ui32>{{1, 2, 3, 4, 5, 6, 10, 11, 12}}));
            }
            UNIT_ASSERT_EQUAL(group1nodes, (THashSet<ui32>{{1, 2, 3, 10, 11, 12, 13, 14, 15}}));
        }
    }
}
