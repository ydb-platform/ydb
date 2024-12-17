#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include "flat_stat_part.h"
#include "flat_stat_table.h"
#include "flat_page_other.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

Y_UNIT_TEST_SUITE(TLegacy) {

    /* This is legacy place for UT, do not put here more tests */

    static TIntrusiveConstPtr<NPage::TFrames> CookFrames()
    {
        NPage::TFrameWriter writer(3);

        writer.Put(100, 0, 100);
        writer.Put(120, 2, 200);
        writer.Put(169, 1, 300);
        writer.Put(200, 0, 400);
        writer.Put(200, 1, 500);
        writer.Put(210, 2, 600);
        writer.Put(338, 2, 700);
        writer.Put(700, 2, 800);

        return new NPage::TFrames(writer.Make());
    }

    Y_UNIT_TEST(IndexIter) {
        TNullOutput devNull;
        IOutputStream& dbgOut = devNull; //*/ Cerr;

        NScheme::TTypeRegistry typeRegistry;

        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Uint32)
            .Key({ 0, 1 });

        TPartCook cook(lay, { true, 4096 });
        TPartCook vcook(lay, { true, 4096 });

        const ui64 X1 = 0, X2 = 3000;

        for (ui64 key1 = X1; key1 <= X2; key1++) {
            for (ui32 key2 = 0; key2 < 1 + key1/1000; key2++) {
                cook.AddN(key1, key2, key2);
                for (int i = 0; i < 10; ++i) {
                    vcook.Ver(TRowVersion(1000, 1000 - i)).AddN(key1, key2, key2 + i);
                }
            }
        }

        TPartEggs eggs = cook.Finish();
        UNIT_ASSERT_C(eggs.Parts.size() == 1,
            "Unexpected " << eggs.Parts.size() << " results");
        TPartEggs veggs = vcook.Finish();
        UNIT_ASSERT_C(veggs.Parts.size() == 1,
            "Unexpected " << veggs.Parts.size() << " results");

        auto fnIterate = [&dbgOut, &typeRegistry] (
                TIntrusiveConstPtr<TPartStore> part,
                TIntrusiveConstPtr<TRowScheme> scheme,
                std::vector<ui64>& sizes)
        {
            TDataStats stats = { };
            TTestEnv env;
            // TScreenedPartIndexIterator without screen previously was TPartGroupFlatIndexItererator
            TStatsScreenedPartIterator idxIter(TPartView{part, nullptr, nullptr}, &env, scheme->Keys, nullptr, nullptr, 0, 0);
            sizes.clear();

            UNIT_ASSERT_VALUES_EQUAL(idxIter.Start(), EReady::Data);
            while (idxIter.IsValid()) {
                TDbTupleRef key = idxIter.GetCurrentKey();
                dbgOut << DbgPrintTuple(key, typeRegistry) << " " << stats.RowCount << " " << stats.DataSize.Size << Endl;
                sizes.push_back(stats.DataSize.Size);
                UNIT_ASSERT(idxIter.Next(stats) != EReady::Page);
            }
        };

        dbgOut << "Iterate with the matching row scheme" << Endl;
        std::vector<ui64> sizes;
        fnIterate(eggs.At(0), eggs.Scheme, sizes);

        dbgOut << "Iterate same data with versions" << Endl;
        std::vector<ui64> vsizes;
        fnIterate(veggs.At(0), veggs.Scheme, vsizes);

        UNIT_ASSERT_C(vsizes.back() / sizes.back() >= 5,
            "Expected to have 5-15x more bytes in versioned " << vsizes.back() << " vs unversioned " << sizes.back() << " part");

        // Add a column with default value to the key
        ui32 def10 = 121212;
        TLayoutCook newLay;
        newLay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Uint32)
            .Col(0, 10,  NScheme::NTypeIds::Uint32, TCell((const char*)&def10, sizeof(def10)))
            .Key({ 0, 1, 10});

        dbgOut << "Iterate with added key column with default value" << Endl;
        std::vector<ui64> sizesWithDefaults;
        fnIterate(eggs.At(0), newLay.RowScheme(), sizesWithDefaults);
    }

    Y_UNIT_TEST(ScreenedIndexIter) {
        TNullOutput devNull;
        IOutputStream& dbgOut = devNull; //*/ Cerr;

        NScheme::TTypeRegistry typeRegistry;

        TLayoutCook lay;
        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Uint32)
            .Key({ 0, 1});

        const ui64 DATA_PAGE_SIZE = 4096;
        TPartCook cook(lay, NPage::TConf(true, DATA_PAGE_SIZE));

        const ui64 X1 = 0, X2 = 3000;

        for (ui64 key1 = X1; key1 <= X2; key1++) {
            ui32 key2 = 3333;
            cook.AddN(key1, key2, key2);
        }

        TPartEggs eggs = cook.Finish();
        UNIT_ASSERT_C(eggs.Parts.size() == 1,
            "Unexpected " << eggs.Parts.size() << " results");

        auto fnIterate = [&dbgOut, &typeRegistry] (TIntrusiveConstPtr<TPartStore> part, TIntrusiveConstPtr<TScreen> screen,
                            TIntrusiveConstPtr<TRowScheme> scheme, TIntrusiveConstPtr<NPage::TFrames> frames) -> std::pair<ui64, ui64> {
            TDataStats stats = { };
            TTestEnv env;
            TStatsScreenedPartIterator idxIter(TPartView{part, screen, nullptr}, &env, scheme->Keys, std::move(frames), nullptr, 0, 0);

            UNIT_ASSERT_VALUES_EQUAL(idxIter.Start(), EReady::Data);
            while (idxIter.IsValid()) {
                TDbTupleRef key = idxIter.GetCurrentKey();
                dbgOut << DbgPrintTuple(key, typeRegistry)
                     << " " << stats.RowCount << " " << stats.DataSize.Size << Endl;
                UNIT_ASSERT(idxIter.Next(stats) != EReady::Page);
            }

            return {stats.RowCount, stats.DataSize.Size};
        };

        dbgOut << "Hide all" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({});
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, 0, "RowCount should be 0");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, 0, "DataSize should be 0");
        }

        const ui64 ROWS_PER_PAGE = 169;     // emperical
        const ui64 REAL_PAGE_SIZE = 4076;   // also emperical
        ui64 expectedRowCount = X2 + 1;
        ui64 expectedTotalSize = 0;
        ui64 expectedPageCount = (expectedRowCount + ROWS_PER_PAGE - 1) / ROWS_PER_PAGE;
        for (ui32 pageId = 0; pageId < expectedPageCount; ++pageId) {
            expectedTotalSize += eggs.At(0)->GetPageSize(pageId, {});
        }

        dbgOut << "Hide none" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({TScreen::THole(true)});
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, expectedRowCount, "RowCount doesn't match");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, expectedTotalSize, "DataSize doesn't match");
        }

        dbgOut << "Hide 2 pages" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({TScreen::THole(0,150), TScreen::THole(550, 10000)});
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, expectedRowCount - 400, "RowCount doesn't match");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, expectedTotalSize - REAL_PAGE_SIZE*2, "DataSize doesn't match");
        }

        dbgOut << "Hide all except 3 pages" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({TScreen::THole(150, 400)});
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, 250, "RowCount doesn't match");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, REAL_PAGE_SIZE*3, "DataSize doesn't match");
        }

        dbgOut << "Hide 2 rows in one page - we just ignore this" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({TScreen::THole(0,150), TScreen::THole(152, 10000)});
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, expectedRowCount - 2, "RowCount doesn't match");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, expectedTotalSize, "DataSize doesn't match");
        }

        dbgOut << "Hide 4 pages in 3 different ranges" << Endl;
        {
            TIntrusiveConstPtr<TScreen> screen = new TScreen({
                TScreen::THole(400, 600),
                TScreen::THole(850, 950),
                TScreen::THole(1200, 10000)
                });
            auto res = fnIterate(eggs.At(0), screen, eggs.Scheme, nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(res.first, expectedRowCount - 400 - 250 - 250, "RowCount doesn't match");
            UNIT_ASSERT_VALUES_EQUAL_C(res.second, expectedTotalSize - REAL_PAGE_SIZE*4, "DataSize doesn't match");
        }

        dbgOut << "Attach outer pages to index with screen" << Endl;
        {
            auto frames = CookFrames();

            // This screen takes two pages, one of them has 4 small blobs, 1800 total bytes
            TIntrusiveConstPtr<TScreen> screen = new TScreen({
                TScreen::THole(169, 338),
                TScreen::THole(845, 1014)
            });

            auto res0 = fnIterate(eggs.At(0), nullptr, eggs.Scheme, frames);
            UNIT_ASSERT_VALUES_EQUAL_C(res0.second, expectedTotalSize + 3600, "DataSize doesn't match without a screen");
            auto res1 = fnIterate(eggs.At(0), screen, eggs.Scheme, frames);
            UNIT_ASSERT_VALUES_EQUAL_C(res1.second, REAL_PAGE_SIZE*2 + 1800, "DataSize doesn't match with a screen");
        }
    }

    Y_UNIT_TEST(StatsIter) {
        TNullOutput devNull;
        IOutputStream& dbgOut = devNull; //*/ Cerr;

        NScheme::TTypeRegistry typeRegistry;

        TLayoutCook lay1;
        lay1
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Uint32)
            .Key({ 0, 1});

        TPartCook cook1(lay1, { true, 4096 });

        {
            const ui64 X1 = 0, X2 = 3000;

            for (ui64 key1 = X1; key1 <= X2; key1++) {
                for (ui32 key2 = 0; key2 < 10; key2++)
                    cook1.AddN(key1, key2, key2);
            }
        }

        TPartEggs eggs1 = cook1.Finish();
        UNIT_ASSERT_C(eggs1.Parts.size() == 1,
            "Unexpected " << eggs1.Parts.size() << " results");

        // Add a column with default value to the key
        ui32 def10 = 3;
        TLayoutCook lay2;
        lay2
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Col(0, 2,  NScheme::NTypeIds::Uint32)
            .Col(0, 10,  NScheme::NTypeIds::Uint32, TCell((const char*)&def10, sizeof(def10)))
            .Key({ 0, 1, 10});


        TPartCook cook2(lay2, { true, 4096 });

        {
            const ui64 X1 = 2000, X2 = 5000;

            for (ui64 key1 = X1; key1 <= X2; key1++) {
                for (ui32 key2 = 0; key2 < key1%10; key2++)
                    cook2.AddN(key1, key2, key2, key2);
            }
        }

        TPartEggs eggs2 = cook2.Finish();
        UNIT_ASSERT_C(eggs2.Parts.size() == 1,
            "Unexpected " << eggs2.Parts.size() << " results");

        TIntrusiveConstPtr<TScreen> screen1 = new TScreen({
                TScreen::THole(400, 600),
                TScreen::THole(700, 800),
                TScreen::THole(1200, 100000)
                });

        TIntrusiveConstPtr<TScreen> screen2 = new TScreen({
                TScreen::THole(2400, 2600),
                TScreen::THole(2700, 2800),
                TScreen::THole(4200, 100000)
                });

        TDataStats stats = { };
        TTestEnv env;
        TStatsIterator stIter(lay2.RowScheme()->Keys);
        {
            auto it1 = MakeHolder<TStatsScreenedPartIterator>(TPartView{eggs2.At(0), screen2, nullptr}, &env, lay2.RowScheme()->Keys, nullptr, nullptr, 0, 0);
            auto it2 = MakeHolder<TStatsScreenedPartIterator>(TPartView{eggs1.At(0), screen1, nullptr}, &env, lay2.RowScheme()->Keys, nullptr, nullptr, 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(it1->Start(), EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(it2->Start(), EReady::Data);
            stIter.Add(std::move(it1));
            stIter.Add(std::move(it2));
        }

        TSerializedCellVec prevKey;
        ui64 prevRowCount = 0;
        ui64 prevDataSize = 0;
        while (true) {
            auto ready = stIter.Next(stats);
            if (ready == EReady::Gone) {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(ready, EReady::Data);

            TDbTupleRef key = stIter.GetCurrentKey();

            dbgOut << DbgPrintTuple(key, typeRegistry)
                   << " " << stats.RowCount << " " << stats.DataSize.Size << Endl;

            UNIT_ASSERT_C(CompareTypedCellVectors(key.Columns, prevKey.GetCells().data(), key.Types, key.ColumnCount, prevKey.GetCells().size()) > 0,
                          "Keys must be sorted");

            UNIT_ASSERT(prevRowCount < stats.RowCount);
            UNIT_ASSERT(prevDataSize < stats.DataSize.Size);

            prevKey = TSerializedCellVec(TConstArrayRef<TCell>(key.Columns, key.ColumnCount));
            prevRowCount = stats.RowCount;
            prevDataSize = stats.DataSize.Size;
        }
    }

}

} // namespace NTest
} // namspace NTable
} // namespace NKikimr


