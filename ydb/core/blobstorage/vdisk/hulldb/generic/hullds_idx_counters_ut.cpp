#include "hullds_idx.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TLevelIndexMonitoring) {
    Y_UNIT_TEST(SortedTotalsIncludeLevelsBeyondCounterLimit) {
        using TIndex = TLevelIndex<TKeyLogoBlob, TMemRecLogoBlob>;
        using TSst = TIndex::TLevelSegment;
        constexpr ui32 MaxCounterLevels = NMonGroup::TLsmAllLevelsStat::MaxCounterLevels;

        TTestContexts context(8u << 20);
        auto index = MakeIntrusive<TIndex>(context.GetLevelIndexSettings(),
            std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate));
        index->LoadCompleted();
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NMonGroup::TLsmAllLevelsStat stat(counters);

        auto checkSorted = [&](const NMonGroup::TLsmLevelStats& expected) {
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.SstNum().Val(), expected.SstNum);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItems().Val(), expected.NumItems);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItemsInplaced().Val(), expected.NumItemsInplaced);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.NumItemsHuge().Val(), expected.NumItemsHuge);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.DataInplaced().Val(), expected.DataInplaced);
            UNIT_ASSERT_VALUES_EQUAL(stat.SortedLevels.DataHuge().Val(), expected.DataHuge);
        };
        auto addSst = [&](ui32 level, ui32 scale) {
            auto sst = MakeIntrusive<TSst>(context.GetVCtx());
            TTrackableVector<TSst::TRec> records(TMemoryConsumer(context.GetVCtx()->SstIndex));
            for (ui32 i = 0; i < 10 * scale; ++i) {
                TMemRecLogoBlob memRec;
                if (i < 2 * scale) {
                    memRec.SetDiskBlob(TDiskPart(1, i * 10, 10));
                } else if (i < 5 * scale) {
                    memRec.SetHugeBlob(TDiskPart(2, i * 10, 10));
                }
                records.emplace_back(TKeyLogoBlob(TLogoBlobID(1, 1, i + 1, 0, 10, 0, 1)), memRec);
            }
            sst->LoadLinearIndex(records);
            sst->Info.Items = 10 * scale;
            sst->Info.ItemsWithInplacedData = 2 * scale;
            sst->Info.ItemsWithHugeData = 3 * scale;
            sst->Info.InplaceDataTotalSize = 20 * scale;
            sst->Info.HugeDataTotalSize = 30 * scale;
            if (level == 0) {
                index->CurSlice->Level0.Put(sst);
            } else {
                index->CurSlice->SortedLevels[level - 1].Put(sst);
            }
        };

        for (ui32 i = 0; i <= MaxCounterLevels; ++i) {
            index->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob());
        }
        index->UpdateLevelStat(stat);
        checkSorted({});

        addSst(0, 1);
        addSst(1, 2);
        addSst(MaxCounterLevels - 1, 3);
        index->UpdateLevelStat(stat);
        checkSorted({2, 50, 10, 15, 100, 150});
        UNIT_ASSERT_VALUES_EQUAL(stat.Levels[MaxCounterLevels - 1]->SstNum(), 1);

        auto& sorted = index->CurSlice->SortedLevels;
        std::swap(sorted[MaxCounterLevels - 2], sorted[MaxCounterLevels - 1]);
        index->UpdateLevelStat(stat);
        checkSorted({2, 50, 10, 15, 100, 150});
        UNIT_ASSERT_VALUES_EQUAL(stat.Levels[MaxCounterLevels - 1]->SstNum(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index->GetIndexSnapshot().AllLevelsDataInplaced, 120);

        addSst(MaxCounterLevels + 1, 4);
        index->UpdateLevelStat(stat);
        checkSorted({3, 90, 18, 27, 180, 270});
        UNIT_ASSERT_VALUES_EQUAL(index->GetIndexSnapshot().AllLevelsDataInplaced, 200);
        UNIT_ASSERT(!stat.Group->FindSubgroup("level", ToString(MaxCounterLevels)));

        sorted.clear();
        index->UpdateLevelStat(stat);
        checkSorted({});
        UNIT_ASSERT_VALUES_EQUAL(stat.Levels[1]->SstNum(), 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.Levels[0]->SstNum(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stat.Levels[0]->NumItems(), 10);
        UNIT_ASSERT_VALUES_EQUAL(index->GetIndexSnapshot().AllLevelsDataInplaced, 20);
    }
}

} // namespace NKikimr
