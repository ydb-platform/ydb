#include <flat_part_loader.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>
#include <ydb/core/tablet_flat/flat_part_slice.h>
#include <ydb/core/tablet_flat/flat_part_keys.h>
#include <ydb/core/tablet_flat/flat_sausage_gut.h>
#include <ydb/core/tablet_flat/flat_store_hotdog.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>
#include <ydb/core/tablet_flat/util_basics.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

using namespace NTest;
using TPageCollectionProtoHelper = NTabletFlatExecutor::TPageCollectionProtoHelper;
using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

namespace {
    NPage::TConf PageConf() noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).IndexMin = 1024; /* Should cover index buffer grow code */
        conf.Group(0).BTreeIndexNodeTargetSize = 512; /* Should cover up/down moves */
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */
        conf.SliceSize = conf.Group(0).PageSize * 4;

        return conf;
    }

    const NTest::TMass& Mass0()
    {
        static const NTest::TMass mass0(new NTest::TModelStd(false), 24000);
        return mass0;
    }

    const NTest::TPartEggs& Eggs0()
    {
        static const NTest::TPartEggs eggs0 = NTest::TPartCook::Make(Mass0(), PageConf());
        UNIT_ASSERT_C(eggs0.Parts.size() == 1,
            "Unexpected " << eggs0.Parts.size() << " results");
        return eggs0;
    }

    const TIntrusiveConstPtr<NTest::TPartStore>& Part0()
    {
        static const auto part = Eggs0().At(0);
        return part;
    }

    bool IsBTreeIndex()
    {
        return Eggs0().Lone()->IndexPages.BTreeGroups.size();
    }

    class TTestPartPageCollection : public NPageCollection::IPageCollection {
    public:
        TTestPartPageCollection(TIntrusiveConstPtr<NTest::TPartStore> part, ui32 room)
            : Part(std::move(part))
            , Room(room)
        {
        }

        const TLogoBlobID& Label() const noexcept override
        {
            return Part->Label;
        }

        ui32 Total() const noexcept override
        {
            return Part->Store->PageCollectionPagesCount(Room);
        }

        NPageCollection::TInfo Page(ui32 page) const noexcept override
        {
            const auto array = Part->Store->PageCollectionArray(Room);

            return { array.at(page).size(), ui32(EPage::Undef) };
        }

        NPageCollection::TBorder Bounds(ui32) const noexcept override
        {
            Y_ABORT("Unexpected Bounds(...) call");
        }

        NPageCollection::TGlobId Glob(ui32) const noexcept override
        {
            Y_ABORT("Unexpected Glob(...) call");
        }

        bool Verify(ui32, TArrayRef<const char>) const noexcept override
        {
            Y_ABORT("Unexpected Verify(...) call");
        }

        size_t BackingSize() const noexcept override
        {
            return Part->Store->PageCollectionBytes(Room);
        }

    private:
        TIntrusiveConstPtr<NTest::TPartStore> Part;
        ui32 Room;
    };

    struct TCheckResult {
        size_t Pages = 0;
        TIntrusivePtr<TSlices> Run;
    };

    void VerifyRunOrder(TIntrusiveConstPtr<TSlices> run, const TKeyCellDefaults& keys)
    {
        const TSlice* prev = nullptr;
        for (auto& slice : *run) {
            if (prev) {
                UNIT_ASSERT_C(TSlice::LessByRowId(*prev, slice),
                    "Two slices intersect by row ids: " <<
                    NFmt::Do(*prev) << " and " << NFmt::Do(slice));
                UNIT_ASSERT_C(TSlice::LessByKey(*prev, slice, keys),
                    "Two slices intersect by keys: " <<
                    NFmt::Do(*prev) << " and " << NFmt::Do(slice));
            }
            prev = &slice;
        }
    }

    void VerifyKey(const NTest::TMass& mass, TSerializedCellVec& actual, TRowId rowId) {
        auto tool = NTest::TRowTool(*mass.Model->Scheme);
        auto expected = tool.KeyCells(mass.Saved[rowId]);
        UNIT_ASSERT_VALUES_EQUAL_C(TSerializedCellVec::Serialize(expected), TSerializedCellVec::Serialize(actual.GetCells()),
            "row " << rowId << " mismatch");
    }

    TCheckResult RunLoaderTest(TIntrusiveConstPtr<NTest::TPartStore> part, TIntrusiveConstPtr<TScreen> screen)
    {
        TCheckResult result;

        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection = new TTestPartPageCollection(part, 0);
        NTable::TLoader::TLoaderEnv env(new TCache(pageCollection));
        env.ProvidePart(part.Get());
        TKeysLoader loader(part.Get(), &env);

        while (!(result.Run = loader.Do(screen))) {
            if (auto fetch = env.GetFetch()) {
                UNIT_ASSERT_C(fetch->PageCollection.Get() == pageCollection.Get(),
                    "TLoader wants to fetch from an unexpected pageCollection");
                UNIT_ASSERT_C(fetch->Pages, "TLoader wants a fetch, but there are no pages");
                result.Pages += fetch->Pages.size();

                for (auto pageId : fetch->Pages) {
                    auto* page = part->Store->GetPage(0, pageId);
                    UNIT_ASSERT_C(page, "TLoader wants a missing page " << pageId);

                    env.Save(fetch->Cookie, { pageId, TSharedPageRef::MakePrivate(*page) });
                }
            } else {
                UNIT_ASSERT_C(false, "TKeysLoader was stalled");
            }
        }
        env.EnsureNoNeedPages(); /* On success there shouldn't be left loads */

        const auto scrSize = screen ? screen->Size() : 1;

        UNIT_ASSERT_C(result.Run->size() == scrSize,
            "Restored slice bounds have " << result.Run->size() <<
            " slices, expected to have " << scrSize);

        auto& mass = Mass0();
        for (size_t i = 0; i < scrSize; i++) {
            auto &sliceItem = *(result.Run->begin() + i);
            auto screenItem = screen ? *(screen->begin() + i) : TScreen::THole(0, mass.Saved.Size());
            UNIT_ASSERT_VALUES_EQUAL(sliceItem.FirstRowId, screenItem.Begin);
            UNIT_ASSERT_VALUES_EQUAL(sliceItem.LastRowId, Min(screenItem.End, mass.Saved.Size() - 1));
            UNIT_ASSERT_VALUES_EQUAL(sliceItem.FirstInclusive, true);
            UNIT_ASSERT_VALUES_EQUAL(sliceItem.LastInclusive, screenItem.End >= mass.Saved.Size());
            VerifyKey(mass, sliceItem.FirstKey, sliceItem.FirstRowId);
            VerifyKey(mass, sliceItem.LastKey, sliceItem.LastRowId);
        }

        VerifyRunOrder(result.Run, *Eggs0().Scheme->Keys);

        return result;
    }

}

Y_UNIT_TEST_SUITE(TPartSliceLoader) {

    Y_UNIT_TEST(RestoreMissingSlice) {
        auto result = RunLoaderTest(Part0(), nullptr);
        UNIT_ASSERT_C(result.Pages == (IsBTreeIndex() ? 5 : 1) + 2, // index + first + last
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreOneSlice) {
        for (int startOff = 0; startOff < 5; startOff++) {
            for (int endOff = -5; endOff < 5; endOff++) {
                TVector<TScreen::THole> holes;
                holes.emplace_back(startOff, IndexTools::GetEndRowId(*Part0()) + endOff);
                TIntrusiveConstPtr<TScreen> screen = new TScreen(std::move(holes));
                auto result = RunLoaderTest(Part0(), screen);

                UNIT_ASSERT_VALUES_EQUAL_C(result.Pages, (IsBTreeIndex() ? 5 : 1) + 2, // index + first + last
                    "Restoring slice [" << startOff << ", " << IndexTools::GetEndRowId(*Part0()) + endOff << "] bounds needed "
                        << result.Pages << " extra pages");
            }
        }
    }

    Y_UNIT_TEST(RestoreMissingSliceFullScreen) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Construct screen from every index page
            TVector<TScreen::THole> holes;
            TTestEnv env;
            auto index = CreateIndexIter(&*Part0(), &env, { });

            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            while (index->IsValid()) {
                auto from = index->GetRowId();
                auto to = Max<TRowId>();
                if (index->Next() == EReady::Data) {
                    to = index->GetRowId();
                }
                holes.emplace_back(from, to);
            }
            UNIT_ASSERT_C(holes.size() == IndexTools::CountMainPages(*Part0()),
                "Generated screen has " << holes.size() << " intervals");
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_VALUES_EQUAL_C(result.Pages, (IsBTreeIndex() ? 84 : 1) + IndexTools::CountMainPages(*Part0()), // index + all data pages
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreFromScreenIndexKeys) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Construct screen from every even index page
            TVector<TScreen::THole> holes;
            TTestEnv env;
            auto index = CreateIndexIter(&*Part0(), &env, { });
            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            while (index->IsValid()) {
                auto from = index->GetRowId();
                auto to = Max<TRowId>();
                if (index->Next() == EReady::Data) {
                    to = index->GetRowId();
                    index->Next();
                }
                holes.emplace_back(from, to);
            }
            UNIT_ASSERT_C(holes.size() == (IndexTools::CountMainPages(*Part0()) + 1) / 2, "Generated screen has only " << holes.size() << " intervals");
            // Make sure the last page is always included
            holes.back().End = Max<TRowId>();
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_VALUES_EQUAL_C(result.Pages, (IsBTreeIndex() ? 84 : 1) + IndexTools::CountMainPages(*Part0()), // index + all data pages
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreFromScreenDataKeys) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Use every even index page, without first and last key
            TVector<TScreen::THole> holes;
            TTestEnv env;
            auto index = CreateIndexIter(&*Part0(), &env, { });
            Y_ABORT_UNLESS(index->Seek(0) == EReady::Data);
            while (index->IsValid()) {
                TRowId begin = index->GetRowId() + 1;
                TRowId end;
                if (index->Next() == EReady::Data) {
                    end = index->GetRowId() - 1;
                    index->Next();
                } else {
                    end = IndexTools::GetEndRowId(*Part0()) - 1;
                }
                if (begin < end) {
                    holes.emplace_back(begin, end);
                }
            }
            UNIT_ASSERT_C(holes.size() > 2, "Generated screen has only " << holes.size() << " intervals");
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_VALUES_EQUAL_C(result.Pages, (IsBTreeIndex() ? 84  : 1) + screen->Size(), // index + data pages
            "Restoring slice bounds needed " << result.Pages <<
            " extra pages, expected " << screen->Size());
    }
}

}
}
