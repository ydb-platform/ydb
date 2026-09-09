#include "viewer.h"
#include "json_handlers.h"
#include "storage_groups.h"
#include "viewer_counters.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NViewer {

Y_UNIT_TEST_SUITE(WideErasureViewer) {
    Y_UNIT_TEST(CountersPreserveVDiskRecordsForHistograms) {
        NMon::TEvHttpInfo::TPtr event;
        TJsonCounters counters(nullptr, event);
        NKikimrWhiteboard::TEvVDiskStateResponse response;
        for (ui32 disk = 0; disk != 12; ++disk) {
            auto& info = *response.AddVDiskStateInfo();
            VDiskIDFromVDiskID(TVDiskID(1, 1, 0, disk, 0), info.MutableVDiskId());
            info.SetVDiskState(NKikimrWhiteboard::OK);
            info.SetReplicated(true);
        }
        const TString original = response.SerializeAsString();
        TStringStream json;
        const TEvInterconnect::TNodeInfo node(1, "", "storage-1", "", 19001, TNodeLocation());
        counters.RenderStats(json, response, node);
        UNIT_ASSERT_VALUES_EQUAL(response.SerializeAsString(), original);
        UNIT_ASSERT_STRING_CONTAINS(json.Str(), "\"VDiskState\":\"OK\"},\"value\":12");
    }

    Y_UNIT_TEST(StorageGroupsFailureMatrix) {
        using namespace NKikimrViewer;
        const std::array expected{EFlag::Green, EFlag::Yellow, EFlag::Orange, EFlag::Red};
        for (ui32 failed = 0; failed <= 3; ++failed) {
            for (const auto status : {NKikimrBlobStorage::ERROR, NKikimrBlobStorage::REPLICATING, NKikimrBlobStorage::INIT_PENDING}) {
                TStorageGroups::TGroup group;
                group.ErasureSpecies = TErasureType::Erasure8Plus2Block;
                for (ui32 disk = 0; disk != 12; ++disk) {
                    auto& vdisk = group.VDisks.emplace_back();
                    vdisk.VDiskId = TVDiskID(1, 1, 0, disk, 0);
                    vdisk.Present = true;
                    vdisk.VDiskStatus = disk >= 12 - failed ? status : NKikimrBlobStorage::READY;
                }
                group.CalcState();
                UNIT_ASSERT_VALUES_EQUAL(group.VDisks.size(), 12);
                UNIT_ASSERT_VALUES_EQUAL(group.MissingDisks, failed);
                const auto flag = failed == 1 && status != NKikimrBlobStorage::ERROR ? EFlag::Blue : expected[failed];
                UNIT_ASSERT_VALUES_EQUAL(ui32(group.Overall), ui32(flag));
                UNIT_ASSERT(!group.State.empty());
            }
        }
        TStorageGroups::TGroup unknown;
        unknown.ErasureSpecies = TErasureType::ErasureSpeciesCount;
        unknown.CalcState();
        UNIT_ASSERT_VALUES_EQUAL(ui32(unknown.Overall), ui32(EFlag::Red));
        UNIT_ASSERT_VALUES_EQUAL(unknown.State, "unknown erasure");
    }

    Y_UNIT_TEST(StorageGroupsMissingWhiteboardCountsOnce) {
        for (auto species : {TErasureType::Erasure4Plus2Block, TErasureType::Erasure8Plus2Block}) {
            TStorageGroups::TGroup group;
            group.ErasureSpecies = species;
            const ui32 size = TBlobStorageGroupType(species).BlobSubgroupSize();
            for (ui32 disk = 0; disk != size; ++disk) {
                auto& vdisk = group.VDisks.emplace_back();
                vdisk.VDiskId = TVDiskID(1, 1, 0, disk, 0);
                vdisk.Present = disk != size - 1;
                vdisk.VDiskStatus = vdisk.Present ? NKikimrBlobStorage::READY : NKikimrBlobStorage::ERROR;
            }
            group.CalcState();
            UNIT_ASSERT_VALUES_EQUAL(group.MissingDisks, 1);
            UNIT_ASSERT_VALUES_EQUAL(ui32(group.Overall), ui32(NKikimrViewer::EFlag::Yellow));
        }
    }

    Y_UNIT_TEST(ViewerStorageFailureMatrix) {
        using namespace NKikimrViewer;
        const std::array expected{EFlag::Green, EFlag::Yellow, EFlag::Orange, EFlag::Red};
        for (ui32 failed = 0; failed <= 3; ++failed) {
            for (const bool replicating : {false, true}) {
                NKikimrWhiteboard::TBSGroupStateInfo group;
                group.SetErasureSpecies("block-8-2");
                std::array<NKikimrWhiteboard::TVDiskStateInfo, 12> vdisks;
                std::array<NKikimrWhiteboard::TPDiskStateInfo, 12> pdisks;
                TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&> vi;
                TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&> pi;
                for (ui32 disk = 0; disk != 12; ++disk) {
                    auto& id = *group.AddVDiskIds();
                    VDiskIDFromVDiskID(TVDiskID(1, 1, 0, disk, 0), &id);
                    auto& vdisk = vdisks[disk];
                    vdisk.SetNodeId(disk + 1);
                    vdisk.SetPDiskId(1);
                    vdisk.SetVDiskState(NKikimrWhiteboard::OK);
                    vdisk.SetReplicated(!(replicating && disk >= 12 - failed));
                    pdisks[disk].SetState(NKikimrBlobStorage::TPDiskState::Normal);
                    if (replicating || disk < 12 - failed) {
                        vi.emplace(id, vdisk);
                    }
                    pi.emplace(std::make_pair(disk + 1, 1), pdisks[disk]);
                }
                auto result = GetBSGroupOverallStateWithoutLatency(group, vi, pi);
                UNIT_ASSERT_VALUES_EQUAL(result.MissingDisks, failed);
                UNIT_ASSERT_VALUES_EQUAL(ui32(result.Overall), ui32(failed == 1 && replicating ? EFlag::Blue : expected[failed]));
                group.SetErasureSpecies("unknown-erasure");
                UNIT_ASSERT_VALUES_EQUAL(ui32(GetBSGroupOverallStateWithoutLatency(group, vi, pi).Overall), ui32(EFlag::Red));
            }
        }
    }
}

} // namespace NKikimr::NViewer
