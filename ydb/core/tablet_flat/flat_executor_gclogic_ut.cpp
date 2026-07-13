#include "flat_executor_gclogic.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {

constexpr ui32 HistoryCutterUtBlobSize = 42;

TLogoBlobID HistoryCutterUtBlob(ui64 tabletId, ui32 generation, ui32 channel) {
    return TLogoBlobID(tabletId, generation, 0, channel, HistoryCutterUtBlobSize, 0);
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
}

}
}
