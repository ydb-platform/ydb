#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_comp.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TCompactionMulti) {

    using namespace NTest;

    Y_UNIT_TEST(ManyParts)
    {
        const TMass mass(new TModelStd(false), 32 * 1025);

        auto conf = NPage::TConf{ };
        conf.Group(0).PageSize = 2 * 1024;
        conf.MainPageCollectionEdge = 64 * 1024;

        auto subset = TMake(mass).Mixed(0, 19, NTest::TMixerRnd(19));
        auto eggs = TCompaction(nullptr, conf).Do(*subset);
        UNIT_ASSERT_C(eggs.Parts.size() > 3,
            "Compaction produced " << eggs.Parts.size() << " parts");

        TCheckIter(eggs, { }).IsTheSame(mass.Saved);
    }

    void RunMainEdgeTest(
            TIntrusiveConstPtr<TRowScheme> scheme,
            const TRowsHeap& rows,
            const NPage::TConf& initialConf,
            bool strictPageSize = true,
            int attempts = 10)
    {
        auto initial =
            TPartCook(scheme, initialConf)
                .Add(rows.begin(), rows.end())
                .Finish();
        UNIT_ASSERT_C(initial.Parts.size() == 1,
            "Unexpected " << initial.Parts.size() << " results");

        auto* initialPart = initial.Parts.at(0).Get();

        auto fullPageSize = initialPart->Store->GetPage(0, 0)->size();
        auto fullPartSize = initialPart->Store->PageCollectionBytes(0);

        if (strictPageSize) {
            // Verify the first page is used to its full size
            UNIT_ASSERT_VALUES_EQUAL(fullPageSize, initialConf.Groups[0].PageSize);

            // Verify the last page contains a single row
            TTestEnv env;
            auto iter = CreateIndexIter(initialPart, &env, { });
            Y_ABORT_UNLESS(iter->SeekLast() == EReady::Data, "Unexpected failure to find the last index page");
            auto count = iter->GetEndRowId() - iter->GetRowId();
            UNIT_ASSERT_C(count == 1, "Unexpected " << count << " rows on the last page");
        }

        auto lastPartSize = fullPartSize;

        for (int attempt : xrange(attempts)) {
            auto conf = initialConf;
            conf.MainPageCollectionEdge = lastPartSize;

            // Verify we produce part with the exact same size
            {
                auto born = TCompaction(nullptr, conf).Do(initial);
                UNIT_ASSERT_VALUES_EQUAL(born.Parts.size(), attempt ? 2u : 1u);
                UNIT_ASSERT_VALUES_EQUAL(born.At(0)->Store->PageCollectionBytes(0), lastPartSize);

                TCheckIter(born, { }).IsTheSame(rows).Is(EReady::Gone);
            }

            --conf.MainPageCollectionEdge;

            // Verify we produce part that is slightly smaller when not enough space
            {
                const auto born = TCompaction(nullptr, conf).Do(initial);
                UNIT_ASSERT_VALUES_EQUAL(born.Parts.size(), 2u);

                lastPartSize = born.At(0)->Store->PageCollectionBytes(0);
                UNIT_ASSERT_C(lastPartSize <= conf.MainPageCollectionEdge,
                    "Produced part with size " << lastPartSize
                    << " expected no more than " << conf.MainPageCollectionEdge);

                TCheckIter(born, { }).IsTheSame(rows).Is(EReady::Gone);

                // The first part must gen one less row than the last time
                auto size = rows.end() - rows.begin();
                auto split = size - attempt - 1;

                // The first part must get one less row than the last time
                TPartEggs egg1{ nullptr, born.Scheme, { born.At(0) } };
                TPartEggs egg2{ nullptr, born.Scheme, { born.At(1) } };
                TCheckIter(egg1, { })
                    .IsTheSame(rows.begin(), rows.begin() + split)
                    .Is(EReady::Gone);
                TCheckIter(egg2, { })
                    .IsTheSame(rows.begin() + split, rows.end())
                    .Is(EReady::Gone);

                // Verify bloom filter correctness
                if (conf.ByKeyFilter) {
                    TRowTool tool(*born.Scheme);
                    for (auto it : xrange(size)) {
                        const auto key = tool.KeyCells(rows[it]);
                        const NBloom::TPrefix prefix(key);
                        auto* part = born.At(it >= split ? 1 : 0).Get();
                        UNIT_ASSERT_C(part->ByKey, "Part is missing a bloom filter");
                        UNIT_ASSERT_C(
                            part->MightHaveKey(prefix.Get(key.size())),
                            "Part's bloom filter is missing key for row "
                            << tool.Describe(rows[it]));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(MainPageCollectionEdge)
    {
        const TMass mass(new TModelStd(false), 32 * 1025);

        auto initialConf = NPage::TConf{ };
        // precise size estimation doesn't work with cut keys
        initialConf.CutIndexKeys = false;
        initialConf.SmallEdge = 19;
        initialConf.LargeEdge = 29;
        initialConf.ByKeyFilter = true;
        initialConf.MaxRows = mass.Saved.Size();

        RunMainEdgeTest(mass.Model->Scheme, mass.Saved, initialConf, false, 2);
    }

    Y_UNIT_TEST(MainPageCollectionEdgeMany)
    {
        const TMass mass(new TModelStd(false), 32 * 1025);

        auto initialConf = NPage::TConf{ };
        // precise size estimation doesn't work with cut keys
        initialConf.CutIndexKeys = false;
        initialConf.SmallEdge = 19;
        initialConf.LargeEdge = 29;
        initialConf.ByKeyFilter = true;
        initialConf.MaxRows = mass.Saved.Size();

        auto initial =
            TPartCook(mass.Model->Scheme, initialConf)
                .Add(mass.Saved.begin(), mass.Saved.end())
                .Finish();
        UNIT_ASSERT_C(initial.Parts.size() == 1,
            "Unexpected " << initial.Parts.size() << " results");

        auto fullPartSize = initial.Parts.at(0)->Store->PageCollectionBytes(0);

        auto conf = initialConf;
        conf.MainPageCollectionEdge = fullPartSize / 4;

        auto born = TCompaction(nullptr, conf).Do(initial);
        UNIT_ASSERT_C(born.Parts.size() >= 4,
            "Unexpected " << born.Parts.size() << " results after compaction");

        for (auto &part : born.Parts) {
            auto partSize = part->Store->PageCollectionBytes(0);
            UNIT_ASSERT_C(partSize <= conf.MainPageCollectionEdge,
                "Unexpected main page collection with " << partSize << " bytes,"
                << " maximum of " << conf.MainPageCollectionEdge << " allowed");
        }

        TCheckIter(born, { }).IsTheSame(mass.Saved).Is(EReady::Gone);
    }

    Y_UNIT_TEST(MainPageCollectionOverflow)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 8,  NScheme::NTypeIds::Uint64)
            .Key({ 0 });

        // This number of rows causes last row to be on its own page (overflow)
        int nrows = 88 * 2 + 1; // 23 bytes per row, 88 rows on a page

        TRowsHeap rows(nrows);
        for (int i = 0; i < nrows; ++i) {
            rows.Put(*TSchemedCookRow(*lay).Col(ui64(i), ui64(i * 10)));
        }

        auto initialConf = NPage::TConf{ false, 2044 };
        // precise size estimation doesn't work with cut keys
        initialConf.CutIndexKeys = false;
        initialConf.ByKeyFilter = true;
        initialConf.MaxRows = rows.Size();

        RunMainEdgeTest(lay.RowScheme(), rows, initialConf);
    }

    Y_UNIT_TEST(MainPageCollectionOverflowSmallRefs)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 8,  NScheme::NTypeIds::String)
            .Key({ 0 });

        // This number of rows causes last row to be on its own page (overflow)
        int nrows = 88 * 2 + 1; // 23 bytes per row, 88 rows on a page

        TRowsHeap rows(nrows);
        for (int i = 0; i < nrows; ++i) {
            if (i >= nrows-10) {
                // last 10 rows are strings offloaded to small blobs
                rows.Put(*TSchemedCookRow(*lay).Col(ui64(i), TString("Hello, world!")));
            } else {
                rows.Put(*TSchemedCookRow(*lay).Col(ui64(i), nullptr));
            }
        }

        auto initialConf = NPage::TConf{ false, 2044 };
        // precise size estimation doesn't work with cut keys
        initialConf.CutIndexKeys = false;
        initialConf.ByKeyFilter = true;
        initialConf.MaxRows = rows.Size();
        initialConf.SmallEdge = 13;

        RunMainEdgeTest(lay.RowScheme(), rows, initialConf);
    }

    Y_UNIT_TEST(MainPageCollectionOverflowLargeRefs)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 8,  NScheme::NTypeIds::String)
            .Key({ 0 });

        // This number of rows causes last row to be on its own page (overflow)
        int nrows = 88 * 2 + 1; // 23 bytes per row, 88 rows on a page

        TRowsHeap rows(nrows);
        for (int i = 0; i < nrows; ++i) {
            if (i >= nrows-10) {
                // last 10 rows are strings offloaded to small blobs
                rows.Put(*TSchemedCookRow(*lay).Col(ui64(i), TString("Hello, world!")));
            } else {
                rows.Put(*TSchemedCookRow(*lay).Col(ui64(i), nullptr));
            }
        }

        auto initialConf = NPage::TConf{ false, 2044 };
        // precise size estimation doesn't work with cut keys
        initialConf.CutIndexKeys = false;
        initialConf.ByKeyFilter = true;
        initialConf.MaxRows = rows.Size();
        initialConf.LargeEdge = 13;

        RunMainEdgeTest(lay.RowScheme(), rows, initialConf);
    }
}

}
}
