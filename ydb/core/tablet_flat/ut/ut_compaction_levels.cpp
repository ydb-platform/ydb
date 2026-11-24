#include <ydb/core/tablet_flat/flat_boot_switch.h>
#include <ydb/core/tablet_flat/flat_comp.h>
#include <ydb/core/tablet_flat/flat_store_hotdog.h>
#include <ydb/core/tablet_flat/flat_store_solid.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    constexpr ui32 TableId = 101;

    TLogoBlobID Label(ui32 serial) {
        return TLogoBlobID(1, 1, serial, 1, 16, 0);
    }

    NTable::TPartComponents Components(ui32 serial, i64 epoch = 20) {
        return {
            .PageCollectionComponents = {{
                .LargeGlobId = NPageCollection::TLargeGlobId(11, Label(serial)),
            }},
            .Epoch = NTable::TEpoch::FromIndex(epoch),
        };
    }

    NKikimrExecutorFlat::TBundle& AddBundle(NKikimrExecutorFlat::TLogTableSnap& snap,
            ui32 serial, std::optional<ui32> level = {}) {
        auto* bundle = snap.AddBundles();
        auto* collection = bundle->AddPageCollections();
        TLargeGlobIdProto::Put(*collection->MutableLargeGlobId(),
            NPageCollection::TLargeGlobId(11, Label(serial)));
        bundle->SetEpoch(20);
        if (level) {
            bundle->SetCompactionLevel(*level);
        }
        return *bundle;
    }

    template<class TProto>
    TProto RoundTrip(const TProto& original) {
        TProto restored;
        UNIT_ASSERT(restored.ParseFromString(original.SerializeAsString()));
        return restored;
    }

}

Y_UNIT_TEST_SUITE(TCompactionLevelPersistenceTest) {

    Y_UNIT_TEST(LegacySnapshotKeepsItsCommonLevel) {
        NKikimrExecutorFlat::TLogTableSnap snap;
        TPageCollectionProtoHelper::Snap(&snap, Components(1), TableId, 3);
        UNIT_ASSERT(!snap.GetBundles(0).HasCompactionLevel());

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(snap));
        UNIT_ASSERT_VALUES_EQUAL(restored.Bundles.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(restored.Bundles[0].Level, 3);
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 3);
    }

    Y_UNIT_TEST(MixedOutputsRoundTripThroughOneAtomicSwitch) {
        NKikimrExecutorFlat::TTablePartSwitch turn;
        turn.SetTableId(TableId);
        auto* snap = turn.MutableIntroducedParts();
        TPageCollectionProtoHelper::Snap(snap, Components(1), TableId, 1);
        snap->MutableBundles(0)->SetCompactionLevel(1);
        TPageCollectionProtoHelper::Snap(snap, Components(2), TableId, 4);
        snap->MutableBundles(1)->SetCompactionLevel(4);
        TPageCollectionProtoHelper::Snap(snap, Components(3), TableId, 255);
        snap->MutableBundles(2)->SetCompactionLevel(255);

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(turn));
        UNIT_ASSERT_VALUES_EQUAL(restored.Table, TableId);
        UNIT_ASSERT_VALUES_EQUAL(restored.Bundles.size(), 3);
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(2)), 4);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(3)), 255);
        for (const auto& bundle : restored.Bundles) {
            UNIT_ASSERT_VALUES_EQUAL(bundle.Epoch, NTable::TEpoch::FromIndex(20));
        }
    }

    Y_UNIT_TEST(BundleOverrideAndLegacyFallbackCanCoexist) {
        NKikimrExecutorFlat::TLogTableSnap snap;
        snap.SetTable(TableId);
        snap.SetCompactionLevel(2);
        AddBundle(snap, 1, 4);
        AddBundle(snap, 2);
        AddBundle(snap, 3, 255);

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(snap));
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 4);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(2)), 2);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(3)), 255);
    }

    Y_UNIT_TEST(MissingCommonLevelDefaultsToFinalParts) {
        NKikimrExecutorFlat::TLogTableSnap snap;
        snap.SetTable(TableId);
        AddBundle(snap, 1);
        AddBundle(snap, 2, 3);

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(snap));
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 255);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(2)), 3);
    }

    Y_UNIT_TEST(LegacyPageCollectionAlsoInheritsTheCommonLevel) {
        NKikimrExecutorFlat::TLogTableSnap snap;
        snap.SetTable(TableId);
        snap.SetCompactionLevel(3);
        LogoBlobIDFromLogoBlobID(Label(1), snap.AddDbPartMetaInfoBody());

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(snap));
        UNIT_ASSERT_VALUES_EQUAL(restored.Bundles.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(restored.Bundles[0].Level, 3);
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 3);
    }

    Y_UNIT_TEST(ReplayingASwitchRemovesInputsAndKeepsOtherPlacements) {
        NKikimrExecutorFlat::TTablePartSwitch turn;
        turn.SetTableId(TableId);
        LogoBlobIDFromLogoBlobID(Label(1), turn.AddLeavingBundles());
        auto* snap = turn.MutableIntroducedParts();
        snap->SetTable(TableId);
        snap->SetCompactionLevel(255);
        AddBundle(*snap, 3, 2);
        AddBundle(*snap, 4, 5);

        NTable::TCompactionState state;
        state.PartLevels[Label(1)] = 1;
        state.PartLevels[Label(2)] = 4;
        NBoot::TSwitch restored;
        restored.Init(RoundTrip(turn));
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT(!state.PartLevels.contains(Label(1)));
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(2)), 4);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(3)), 2);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(4)), 5);
    }

    Y_UNIT_TEST(SubsequentSnapshotsResolveEachBundlesOwnFallback) {
        NKikimrExecutorFlat::TLogTableSnap first;
        first.SetTable(TableId);
        first.SetCompactionLevel(2);
        AddBundle(first, 1);
        NKikimrExecutorFlat::TLogTableSnap second;
        second.SetTable(TableId);
        AddBundle(second, 2);

        NBoot::TSwitch restored;
        restored.Init(RoundTrip(first));
        restored.Init(RoundTrip(second));
        NTable::TCompactionState state;
        restored.ApplyPartLevels(state.PartLevels);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(1)), 2);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(Label(2)), 255);
    }

}

}
