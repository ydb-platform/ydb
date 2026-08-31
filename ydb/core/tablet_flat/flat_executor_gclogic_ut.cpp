#include "flat_executor_gclogic.h"
#include "flat_sausage_grind.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {

constexpr ui32 HistoryCutterUtBlobSize = 42;

TLogoBlobID HistoryCutterUtBlob(ui64 tabletId, ui32 generation, ui32 channel) {
    return TLogoBlobID(tabletId, generation, 0, channel, HistoryCutterUtBlobSize, 0);
}

// Build a minimal cookie allocator for TExecutorGCLogic unit tests.
// The allocator is only exercised during WriteToLog/SendCollectGarbage paths;
// ApplyLogEntry only touches ChannelInfo and HistoryCutter, so a single-entry
// slot array with a wide cookie range is sufficient.
// The caller owns the returned pointer and must pass it to TExecutorGCLogic.
TAutoPtr<NPageCollection::TSteppedCookieAllocator> MakeGCCookies(
        const TTabletStorageInfo& info, ui32 generation = 1) {
    // Register every channel present in info so the allocator slot map does
    // not assert on an unknown channel.  GroupBy() is never called in the
    // test paths we exercise, so the group value is a placeholder.
    TVector<NPageCollection::TSlot> slots;
    for (ui32 ch = 0; ch < (ui32)info.Channels.size(); ++ch) {
        const ui32 group = info.Channels[ch].History.empty()
            ? 1u : info.Channels[ch].History.front().GroupID;
        slots.emplace_back(static_cast<ui8>(ch), group);
    }
    return new NPageCollection::TSteppedCookieAllocator(
        info.TabletID,
        ui64(generation) << 32,
        NPageCollection::TCookieRange{0, 999},
        TArrayRef<const NPageCollection::TSlot>(slots)
    );
}

} // namespace

Y_UNIT_TEST_SUITE(TFlatTableExecutorGC) {
    bool TestDeduplication(TVector<TLogoBlobID> keep, TVector<TLogoBlobID> dontkeep, ui32 gen, ui32 step, TVector<TLogoBlobID> expectKeep, TVector<TLogoBlobID> expectnot) {
        DeduplicateGCKeepVectors(&keep, &dontkeep, gen, step);
        return (keep == expectKeep) && (dontkeep == expectnot);
    }

    Y_UNIT_TEST(TestGCVectorDeduplicaton) {
        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            }
        ));


        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 3,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            }
        ));
    }
}


Y_UNIT_TEST_SUITE(THistoryCutter) {
    Y_UNIT_TEST(TestHistoryCutter) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(1, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        ui32 group = 0;
        for (ui32 gen : {1, 2, 5, 6, 7, 9, 10}) {
            info->Channels[0].History.emplace_back(gen, ++group);
        }
        THistoryCutter cutter(info);
        for (ui32 gen : {3, 4, 8, 9}) {
            cutter.SeenBlob(TLogoBlobID(1, gen, 1, 0, 42, 0));
        }
        std::vector<const TTabletChannelInfo::THistoryEntry*> toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
        UNIT_ASSERT_VALUES_EQUAL(toCut[2]->FromGeneration, 6);
    }



    Y_UNIT_TEST(NoCutsWhenHistoryHasLessThanTwoEntries) {
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            THistoryCutter cutter(info);
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            info->Channels[0].History.emplace_back(1, 100);
            THistoryCutter cutter(info);
            cutter.SeenBlob(HistoryCutterUtBlob(7, 999, 0));
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
    }

    Y_UNIT_TEST(BecomeUncertainDisablesCutsForThatChannel) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(2, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 10);
        info->Channels[0].History.emplace_back(100, 20);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(BecomeUncertainDoesNotAffectOtherChannels) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(3, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        // Channel 1: no blobs seen in [1, 10) => first history entry is cuttable.
        auto toCut = cutter.GetHistoryToCut(1);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(ForeignTabletBlobIsIgnored) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(4, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(99999, 5, 0)); // wrong tablet
        // No valid seen generations => entire first segment looks empty.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(SeenGenerationInsideRangeBlocksCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(5, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(5, 50, 0)); // 50 in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT(toCut.empty());
    }

    Y_UNIT_TEST(SeenGenerationAtNextBoundaryAllowsCutOfPreviousSegment) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(6, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(6, 100, 0)); // first seen at next boundary, none in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 10);
    }

    Y_UNIT_TEST(LastHistoryEntryIsNeverCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(8, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(5, 2);
        info->Channels[0].History.emplace_back(9, 3);
        THistoryCutter cutter(info);
        // No blobs seen — both leading segments cuttable; latest entry (9) must not appear.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
    }

    Y_UNIT_TEST(ChannelIsolation) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(9, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(9, 5, 0)); // only channel 0
        UNIT_ASSERT(cutter.GetHistoryToCut(1).size() == 1);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(DuplicateSeenBlobIsIdempotent) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(10, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        const auto b = HistoryCutterUtBlob(10, 5, 0);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    // ApplyDelta must call HistoryCutter.SeenBlob for every
    // blob in delta.Deleted so that a pending DoNotKeep mark pins the history
    // entry that resolves the blob's generation to a group.  Without the call
    // the entry could be cut before GC delivers the flag, making the group
    // irresolvable.
    //
    // Ablation: remove the HistoryCutter.SeenBlob(blobId) call inside the
    // delta.Deleted loop in ApplyDelta.  With that line absent, GetHistoryToCut
    // returns the [10, 100) entry (no blob was observed there), so the
    // UNIT_ASSERT below fails.  Restoring the call makes the test pass.
    Y_UNIT_TEST(DeletedBlobInApplyDeltaPinsHistoryEntry) {
        const ui64 tabletId = 30;

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        // Two history entries: [10, 100) -> group 1, [100, inf) -> group 2.
        info->Channels[0].History.emplace_back(10u, 1u);
        info->Channels[0].History.emplace_back(100u, 2u);

        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info));

        // Put a DoNotKeep blob at generation 50 (inside [10, 100)) into Deleted.
        TGCBlobDelta delta;
        delta.Deleted.push_back(HistoryCutterUtBlob(tabletId, 50, 0));

        // ApplyLogEntry is the public entry point; it calls ApplyDelta internally.
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // The history entry covering [10, 100) must be blocked: generation 50
        // was seen there, so the entry must not appear in the cut list.
        auto toCut = gcLogic.HistoryCutter.GetHistoryToCut(0);
        UNIT_ASSERT_C(toCut.empty(),
            "history entry [10, 100) must not be cuttable while gen-50 blob has a pending DoNotKeep mark");
    }
}

}
}
