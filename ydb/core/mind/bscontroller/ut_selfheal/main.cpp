#include <ydb/core/mind/bscontroller/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include "env.h"

#include <ydb/core/mind/bscontroller/layout_helpers.h>
#include <ydb/core/util/pb.h>

Y_UNIT_TEST_SUITE(BsControllerTest) {

    struct TTestSelfHeal {
        TTestSelfHeal(
            ui32 numDCs = 3, ui32 numRacksPerDC = 4, ui32 numUnitsPerRack = 4, ui32 numDisksPerNode = 2, ui32 numGroups = 64,
            TString erasure = "block-4-2", TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block
        )
            : NumDCs(numDCs)
            , NumRacksPerDC(numRacksPerDC)
            , NumUnitsPerRack(numUnitsPerRack)
            , NumNodes(NumDCs * NumRacksPerDC * NumUnitsPerRack)
            , NumDisksPerNode(numDisksPerNode)
            , NumGroups(numGroups)
            , Erasure(erasure)
            , GroupType(groupType)
            , Env(NumNodes, [=](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation proto;
                proto.SetDataCenter(ToString((nodeId - 1) / (NumUnitsPerRack * NumRacksPerDC)));
                proto.SetRack(ToString((nodeId - 1) / NumUnitsPerRack));
                proto.SetUnit(ToString((nodeId - 1)));
                return TNodeLocation(proto);
            })
            , Geom(CreateGroupGeometry(GroupType))
        {
        }

        void InitCluster() {
            NKikimrBlobStorage::TConfigRequest request;
            TVector<TEnvironmentSetup::TDrive> drives;
            for (ui32 i = 0; i < NumDisksPerNode; ++i) {
                drives.push_back({ .Path = "/dev/disk" + std::to_string(1 + i)});
            }
            Env.DefineBox(1, drives, {{1, NumNodes}}, &request);
            Env.DefineStoragePool(1, 1, NumGroups, NKikimrBlobStorage::ROT, {}, &request, Erasure);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            Env.WaitForNodeWardensToConnect();

            request.Clear();
            auto *cmd = request.AddCommand()->MutableEnableSelfHeal();
            cmd->SetEnable(true);
            response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        void SetBSCSettings(std::optional<bool> useSelfHealLocalPolicy, std::optional<bool> tryToRelocateBrokenDisksLocallyFirst, ui32 additionalSlots) {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableUpdateSettings();
            if (useSelfHealLocalPolicy.has_value()) {
                cmd->AddUseSelfHealLocalPolicy(*useSelfHealLocalPolicy);
            }
            if (tryToRelocateBrokenDisksLocallyFirst.has_value()) {
                cmd->AddTryToRelocateBrokenDisksLocallyFirst(*tryToRelocateBrokenDisksLocallyFirst);
            }
            cmd->AddGroupReserveMin(additionalSlots);
            cmd->AddEnableDonorMode(true);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        std::set<TPDiskId> GetActiveDisks() {
            std::set<TPDiskId> active;

            NKikimrBlobStorage::TConfigRequest request;
            Env.QueryBaseConfig(&request);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                active.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
            }

            TString error;
            UNIT_ASSERT_C(CheckBaseConfigLayout(Geom, response.GetStatus(0).GetBaseConfig(), true, error),
                    "Initial group layout is incorrect, ErrorReason# " << error);

            UNIT_ASSERT_VALUES_EQUAL(active.size(), NumNodes * NumDisksPerNode);

            return active;
        }

        TPDiskId Move(std::set<TPDiskId>& from, std::set<TPDiskId>& to, NKikimrBlobStorage::EDriveStatus status, bool random=true) {
            auto it = from.begin();
            auto pDiskId = *it;
            if (random) {
                std::advance(it, RandomNumber(from.size()));
            }
            Ctest << "PDisk# " << *it
                << " setting status to " << NKikimrBlobStorage::EDriveStatus_Name(status)
                << Endl;
            NKikimrBlobStorage::TConfigRequest request;
            Env.UpdateDriveStatus(*it, status, &request);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            to.insert(from.extract(it));
            return pDiskId;
        }

        auto RequestBasicConfig() {
            NKikimrBlobStorage::TConfigRequest request;
            Env.QueryBaseConfig(&request);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            return response.GetStatus(0).GetBaseConfig();
        }

        void CheckDiskStatuses(const std::set<TPDiskId>& active, std::set<TPDiskId>& faulty) {
            auto conf = RequestBasicConfig();
            for (const auto& pdisk : conf.GetPDisk()) {
                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                if (pdisk.GetDriveStatus() == NKikimrBlobStorage::ACTIVE) {
                    UNIT_ASSERT(active.count(pdiskId));
                } else {
                    UNIT_ASSERT(faulty.count(pdiskId));
                }
            }
        }

        void CheckDiskLocations(const std::set<TPDiskId>& active, std::set<TPDiskId>& faulty) {
            auto conf = RequestBasicConfig();
            THashMap<std::pair<ui32, ui32>, ui32> myMap;
            for (const auto& vslot : conf.GetVSlot()) {
                const auto& id = vslot.GetVSlotId();
                const TPDiskId pdiskId(id.GetNodeId(), id.GetPDiskId());
                myMap[std::make_pair(id.GetNodeId(), id.GetPDiskId())] += 1;
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
            Ctest << "ajdnsjkbfhkbsfksj" << Endl;
            for (const auto& [k, v]: myMap) {
                Ctest << "(" << k.first << " : " << k.second << ") = " << v << Endl;
            }
        }

        void TestCorrectMoves() {
            ui32 disksNum = Geom.GetNumFailRealms() * Geom.GetNumFailDomainsPerFailRealm() * Geom.GetNumVDisksPerFailDomain();
            std::set<TPDiskId> active = GetActiveDisks(), faulty;

            for (size_t i = 0; i < NumNodes; ++i) {
                Env.Wait(TDuration::Seconds(300));
                if (faulty.size() < disksNum) {
                    Move(active, faulty, NKikimrBlobStorage::FAULTY);
                } else {
                    Move(faulty, active, NKikimrBlobStorage::ACTIVE);
                }
                Env.Wait(TDuration::Seconds(300));

                CheckDiskStatuses(active, faulty);
                CheckDiskLocations(active, faulty);

                TString error;
                UNIT_ASSERT_C(CheckBaseConfigLayout(Geom, RequestBasicConfig(), true, error), "Error on step# " << i
                    << ", ErrorReason# " << error);
            }
        }

        void RunTestCorrectMoves() {
            InitCluster();
            TestCorrectMoves();
        }

        THashMap<ui32, ui32> CountVDisksPerNode() {
            THashMap<ui32, ui32> result;
            const auto conf = RequestBasicConfig();
            for (const auto& vslot : conf.GetVSlot()) {
                ++result[vslot.GetVSlotId().GetNodeId()];
            }
            return result;
        }

        bool CheckUniformPDisksPerNode() {
            for (const auto& [_, count]: CountVDisksPerNode()) {
                if (count != 8 * NumDisksPerNode) {
                    return false;
                }
            }
            return true;
        }

        void RunTestCorrectLocalMovesFaulty() {
            InitCluster();
            SetBSCSettings(true, std::nullopt, 0);
            std::set<TPDiskId> active = GetActiveDisks(), faulty;
            UNIT_ASSERT(CheckUniformPDisksPerNode());

            CheckDiskLocations(active, faulty);

            Env.Wait(TDuration::Seconds(300));
            Move(active, faulty, NKikimrBlobStorage::FAULTY);
            Env.Wait(TDuration::Seconds(300 * 8));

            CheckDiskStatuses(active, faulty);
            UNIT_ASSERT(CheckUniformPDisksPerNode());
            CheckDiskLocations(active, faulty);
        }

        void RunTestCorrectLocalMovesIfPossibleBroken() {
            InitCluster();
            SetBSCSettings(std::nullopt, true, 4);
            std::set<TPDiskId> active = GetActiveDisks(), faulty;

            Env.Wait(TDuration::Seconds(300));

            Move(active, faulty, NKikimrBlobStorage::BROKEN, false);
            Env.Wait(TDuration::Seconds(300 * 8));
            CheckDiskStatuses(active, faulty);
            UNIT_ASSERT(CheckUniformPDisksPerNode());
            CheckDiskLocations(active, faulty);

            Move(active, faulty, NKikimrBlobStorage::BROKEN, false);
            Env.Wait(TDuration::Seconds(300 * 8));
            CheckDiskStatuses(active, faulty);
            UNIT_ASSERT(CheckUniformPDisksPerNode());
            CheckDiskLocations(active, faulty);

            Move(active, faulty, NKikimrBlobStorage::BROKEN, false);
            Env.Wait(TDuration::Seconds(300 * 8));
            CheckDiskStatuses(active, faulty);
            UNIT_ASSERT(!CheckUniformPDisksPerNode());
            CheckDiskLocations(active, faulty);
        }

        const ui32 NumDCs;
        const ui32 NumRacksPerDC;
        const ui32 NumUnitsPerRack;
        const ui32 NumNodes;
        const ui32 NumDisksPerNode;
        const ui32 NumGroups;
        const TString Erasure;
        const TBlobStorageGroupType GroupType;

        TEnvironmentSetup Env;
        const TGroupGeometryInfo Geom;
    };

    Y_UNIT_TEST(SelfHealBlock4Plus2) {
        TTestSelfHeal(1, 32, 1, 2, 64, "block-4-2", TBlobStorageGroupType::Erasure4Plus2Block).RunTestCorrectMoves();
    }

    Y_UNIT_TEST(SelfHealMirror3dc) {
        TTestSelfHeal(3, 4, 3, 4, 128, "mirror-3-dc", TBlobStorageGroupType::ErasureMirror3dc).RunTestCorrectMoves();
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

    Y_UNIT_TEST(TestLocalSelfHeal) {
        TTestSelfHeal(3, 4, 3, 4, 3 * 4 * 3 * 4 / 9 * 8, "mirror-3-dc", TBlobStorageGroupType::ErasureMirror3dc).RunTestCorrectLocalMovesFaulty();
    }

    Y_UNIT_TEST(TestLocalBrokenRelocation) {
        TTestSelfHeal(3, 4, 3, 4, 3 * 4 * 3 * 4 / 9 * 8, "mirror-3-dc", TBlobStorageGroupType::ErasureMirror3dc).RunTestCorrectLocalMovesIfPossibleBroken();
    }
}
