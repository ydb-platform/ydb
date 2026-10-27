#include <library/cpp/testing/unittest/registar.h>
#include "blobstorage_syncer_data.h"
#include "blobstorage_syncer_dataserdes.h"
#include "guid_history.h"

using namespace NKikimr;
using namespace NSync;


Y_UNIT_TEST_SUITE(TSyncNeighborsTests) {

    Y_UNIT_TEST(GuidHistoryRequiresEveryNormalSourceRegardlessOfCopyGuarantees) {
        using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        using TGroupVDisks = TBlobStorageGroupInfo::TGroupVDisks;
        for (const auto normalState : {TFailRealm::N0, TFailRealm::N1, TFailRealm::N2}) {
            TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
            dynamic.FailRealmStates = {normalState, normalState, TFailRealm::R};
            const TBlobStorageGroupInfo info(TBlobStorageGroupInfo::TTopology(
                TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), std::move(dynamic), "");
            const auto& topology = info.GetTopology();
            TGroupVDisks sources(&topology);
            // A complete first normal realm and all R peers cannot replace the second normal realm.
            for (const ui32 realm : {0u, 2u}) {
                for (const auto& domain : topology.FailRealms[realm].FailDomains) {
                    for (const auto& disk : domain.VDisks) {
                        sources |= TGroupVDisks(&topology, info.GetVDiskId(disk.OrderNumber));
                    }
                }
            }
            UNIT_ASSERT(!NSyncer::HasAllNormalGuidSources(info, sources));
            const auto& domains = topology.FailRealms[1].FailDomains;
            for (ui32 index = 0; index < domains.size(); ++index) {
                for (const auto& disk : domains[index].VDisks) {
                    sources |= TGroupVDisks(&topology, info.GetVDiskId(disk.OrderNumber));
                }
                UNIT_ASSERT_VALUES_EQUAL(NSyncer::HasAllNormalGuidSources(info, sources), index + 1 == domains.size());
            }
            const TGroupVDisks legacyRecoveringPeer(&topology, info.GetVDiskId(6));
            UNIT_ASSERT(NSyncer::HasAllNormalGuidSources(info, ~legacyRecoveringPeer));
            const TGroupVDisks legacyNormalPeer(&topology, info.GetVDiskId(0));
            UNIT_ASSERT(!NSyncer::HasAllNormalGuidSources(info, ~legacyNormalPeer));
        }
    }

    Y_UNIT_TEST(GuidHistorySurvivesEntryPointReload) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1U, 3U, 3U);
        const auto self = info->GetVDiskId(0);
        TSyncerData data("test", TActorId(), self, info->PickTopology());
        const auto witness = NSyncer::MakeGuidWitness(info->GetVDiskId(6), 123, info->GetVDiskId(3));
        UNIT_ASSERT(NSyncer::MergeGuidWitness(*info, witness, &data.GuidHistory));
        TSyncerData recovered("test", TActorId(), self, info->PickTopology(), data.Serialize(info.Get()));
        UNIT_ASSERT_VALUES_EQUAL(recovered.GuidHistory.WitnessesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(recovered.GuidHistory.GetWitnesses(0).SerializeAsString(), witness.SerializeAsString());
        // Forwarding preserves the original witness and does not populate current quorum votes.
        for (const auto& peer : *recovered.Neighbors) {
            UNIT_ASSERT_VALUES_EQUAL(peer.Get().PeerGuidInfo.Info.GetState(), NKikimrBlobStorage::TSyncGuidInfo::Empty);
        }
    }

    Y_UNIT_TEST(GuidHistoryDeduplicatesAndRejectsConflicts) {
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1U, 3U, 3U);
        NKikimrBlobStorage::TSyncGuidHistory history;
        const auto first = NSyncer::MakeGuidWitness(info->GetVDiskId(6), 123, info->GetVDiskId(0));
        UNIT_ASSERT(NSyncer::MergeGuidWitness(*info, first, &history));
        UNIT_ASSERT(NSyncer::MergeGuidWitness(*info,
            NSyncer::MakeGuidWitness(info->GetVDiskId(6), 123, info->GetVDiskId(3)), &history));
        UNIT_ASSERT_VALUES_EQUAL(history.WitnessesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(history.GetWitnesses(0).SerializeAsString(), first.SerializeAsString());
        UNIT_ASSERT(!NSyncer::MergeGuidWitness(*info,
            NSyncer::MakeGuidWitness(info->GetVDiskId(6), 456, info->GetVDiskId(3)), &history));
        auto futureOwner = info->GetVDiskId(6);
        ++futureOwner.GroupGeneration;
        UNIT_ASSERT(!NSyncer::MergeGuidWitness(*info,
            NSyncer::MakeGuidWitness(futureOwner, 123, info->GetVDiskId(3)), &history));
        UNIT_ASSERT(!NSyncer::MergeGuidWitness(*info, NKikimrBlobStorage::TSyncGuidWitness(), &history));
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
