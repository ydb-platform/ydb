#include <library/cpp/testing/unittest/registar.h>
#include "blobstorage_syncer_data.h"
#include "blobstorage_syncer_dataserdes.h"
#include "guid_history.h"

using namespace NKikimr;
using namespace NSync;


Y_UNIT_TEST_SUITE(TSyncNeighborsTests) {

    Y_UNIT_TEST(GuidHistoryCoverageWithTwoUnavailablePeers) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block, 1U, 8U);
        const auto& top = info->GetTopology();
        const auto self = info->GetVDiskId(0);
        TBlobStorageGroupInfo::TGroupVDisks sources(&top, self);
        sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info->GetVDiskId(1));
        sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info->GetVDiskId(2));
        UNIT_ASSERT(!NSyncer::HasGuidHistoryCoverage(*info, sources, self, false));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(*info, sources, self, true));
        sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info->GetVDiskId(3));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(*info, sources, self, false));
    }

    Y_UNIT_TEST(GuidHistoryCoverageExcludesErasedSelf) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1U, 3U, 3U);
        const auto& top = info->GetTopology();
        const auto self = info->GetVDiskId(4);
        TBlobStorageGroupInfo::TGroupVDisks sources(&top);
        for (ui32 disk = 4; disk < 9; ++disk) {
            sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info->GetVDiskId(disk));
        }
        // The only intersection with publication quorum {0,1,2,3,4} is self.
        UNIT_ASSERT(top.GetQuorumChecker().CheckQuorumForGroup(sources));
        UNIT_ASSERT(!NSyncer::HasGuidHistoryCoverage(*info, sources, self, false));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(*info, sources, self, true));
        sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info->GetVDiskId(0));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(*info, sources, self, false));
    }

    Y_UNIT_TEST(GuidHistoryCoverageInSoloRealm) {
        using TDC = NKikimrBlobStorage::TDataCenterState;
        TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
        dynamic.Mirror3dcState = {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1,
            {TDC::INCLUDED, TDC::EXCLUDED, TDC::EXCLUDED}};
        TBlobStorageGroupInfo info(TBlobStorageGroupInfo::TTopology(
            TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), std::move(dynamic), "");
        const auto& top = info.GetTopology();
        const auto self = info.GetVDiskId(0);
        TBlobStorageGroupInfo::TGroupVDisks sources(&top);
        for (ui32 disk = 3; disk < 9; ++disk) {
            sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, info.GetVDiskId(disk));
        }
        // Excluded realms cannot establish coverage of the surviving realm's publication quorum.
        UNIT_ASSERT(!NSyncer::HasGuidHistoryCoverage(info, sources, self, false));
        sources |= TBlobStorageGroupInfo::TGroupVDisks(&top, self);
        UNIT_ASSERT(!NSyncer::HasGuidHistoryCoverage(info, sources, self, false));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(info, sources, self, true));
        // A surviving peer intersects every SOLO publication quorum, even after self was erased.
        sources = TBlobStorageGroupInfo::TGroupVDisks(&top, info.GetVDiskId(1));
        UNIT_ASSERT(NSyncer::HasGuidHistoryCoverage(info, sources, self, false));
    }

    template<typename... TArgs>
    void SerDes(TArgs&&... args) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(std::forward<TArgs>(args)...);
        auto it = info->VDisksBegin();
        ++it;
        auto vd = info->GetVDiskId(it->OrderNumber);
        const TVDiskID self = vd;

        // fill in values
        TSyncNeighbors n("Prefix", TActorId(), self, info->PickTopology());
        ui64 i = 123456u;
        for (auto &x: n) {
            x.Get().PeerSyncState.SchTime = TInstant::MicroSeconds(++i);
        }

        // old serialize/parse
        TSyncNeighbors n2("Prefix", TActorId(), self, info->PickTopology());
        {
            TStringStream output;
            n.OldSerialize(output, info.Get());
            n2.OldParse(output.Str());
            i = 123456u;
            for (const auto &x: n2) {
                UNIT_ASSERT_VALUES_EQUAL(x.Get().PeerSyncState.SchTime, TInstant::MicroSeconds(++i));
            }
        }

        // new serialize/parse
        TSyncNeighbors n3("Prefix", TActorId(), self, info->PickTopology());
        {
            TStringStream output;
            n2.Serialize(output, info.Get());
            n3.Parse(output.Str());
            i = 123456u;
            for (const auto &x: n3) {
                UNIT_ASSERT_VALUES_EQUAL(x.Get().PeerSyncState.SchTime, TInstant::MicroSeconds(++i));
            }
        }

        // Immutable GUID witnesses must survive a syncer entry-point round trip.
        TSyncerData data("Prefix", TActorId(), self, info->PickTopology());
        data.GuidHistory.AddWitnesses()->CopyFrom(NSyncer::MakeGuidWitness(info->GetVDiskId(0), 42, self));
        TSyncerData restored("Prefix", TActorId(), self, info->PickTopology(), data.Serialize(info.Get()));
        UNIT_ASSERT_VALUES_EQUAL(restored.GuidHistory.SerializeAsString(), data.GuidHistory.SerializeAsString());
    }

    Y_UNIT_TEST(SerDes1) {
        SerDes(TBlobStorageGroupType::Erasure4Plus2Block, 2U, 4U);
    }

    Y_UNIT_TEST(SerDes2) {
        SerDes(TBlobStorageGroupType::Erasure4Plus2Block, 1U, 8U);
    }

    Y_UNIT_TEST(SerDes3) {
        SerDes(TBlobStorageGroupType::Erasure4Plus2Block, 2U, 8U);
    }
}
