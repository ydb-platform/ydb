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

using TPageCollectionProtoHelper = NTabletFlatExecutor::TPageCollectionProtoHelper;
using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

namespace {
    NPage::TConf PageConf() noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).IndexMin = 1024; /* Should cover index buffer grow code */
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
            Y_FAIL("Unexpected Bounds(...) call");
        }

        NPageCollection::TGlobId Glob(ui32) const noexcept override
        {
            Y_FAIL("Unexpected Glob(...) call");
        }

        bool Verify(ui32, TArrayRef<const char>) const noexcept override
        {
            Y_FAIL("Unexpected Verify(...) call");
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
        size_t Pages;
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

    TCheckResult RunLoaderTest(TIntrusiveConstPtr<NTest::TPartStore> part, TIntrusiveConstPtr<TScreen> screen)
    {
        TCheckResult result;

        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection = new TTestPartPageCollection(part, 0);
        TKeysEnv env(part.Get(), new TCache(pageCollection));
        TKeysLoader loader(part.Get(), &env);

        if (result.Run = loader.Do(screen)) {
            env.Check(false); /* On success there shouldn't be left loads */
            result.Pages = 0;
        } else  if (auto fetch = env.GetFetches()) {
            UNIT_ASSERT_C(fetch->PageCollection.Get() == pageCollection.Get(),
                "TLoader wants to fetch from an unexpected pageCollection");
            UNIT_ASSERT_C(fetch->Pages, "TLoader wants a fetch, but there are no pages");
            result.Pages = fetch->Pages.size();

            for (auto pageId : fetch->Pages) {
                auto* page = part->Store->GetPage(0, pageId);
                UNIT_ASSERT_C(page, "TLoader wants a missing page " << pageId);

                env.Save(fetch->Cookie, { pageId, TSharedPageRef::MakePrivate(*page) });
            }

            result.Run = loader.Do(screen);
            UNIT_ASSERT_C(result.Run, "TKeysLoader wants to do unexpected fetches");
            env.Check(false); /* On success there shouldn't be left loads */
        } else {
            UNIT_ASSERT_C(false, "TKeysLoader was stalled");
        }

        const auto scrSize = screen ? screen->Size() : 1;

        UNIT_ASSERT_C(result.Run->size() == scrSize,
            "Restored slice bounds have " << result.Run->size() <<
            " slices, expected to have " << scrSize);
        VerifyRunOrder(result.Run, *Eggs0().Scheme->Keys);

        return result;
    }

}

Y_UNIT_TEST_SUITE(TPartSliceLoader) {

    Y_UNIT_TEST(RestoreMissingSlice) {
        auto result = RunLoaderTest(Part0(), nullptr);
        UNIT_ASSERT_C(result.Pages == 0,
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreMissingSliceFullScreen) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Construct screen from every index page
            TVector<TScreen::THole> holes;
            auto index = Part0()->Index->Begin();
            while (index) {
                if (auto next = index + 1) {
                    holes.emplace_back(index->GetRowId(), next->GetRowId());
                    index = next;
                } else {
                    holes.emplace_back(index->GetRowId(), Max<TRowId>());
                    break;
                }
            }
            UNIT_ASSERT_C(holes.size() == Part0()->Index->Records,
                "Generated screen has " << holes.size() << " intervals");
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_C(result.Pages == 0,
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreFromScreenIndexKeys) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Construct screen from every even index page
            TVector<TScreen::THole> holes;
            auto index = Part0()->Index->Begin();
            while (index) {
                if (auto next = index + 1) {
                    holes.emplace_back(index->GetRowId(), next->GetRowId());
                    index = next + 1;
                } else {
                    holes.emplace_back(index->GetRowId(), Max<TRowId>());
                    break;
                }
            }
            UNIT_ASSERT_C(holes.size() > 2, "Generated screen has only " << holes.size() << " intervals");
            // Make sure the last page is always included
            holes.back().End = Max<TRowId>();
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_C(result.Pages == 0,
            "Restoring slice bounds needed " << result.Pages << " extra pages");
    }

    Y_UNIT_TEST(RestoreFromScreenDataKeys) {
        TIntrusiveConstPtr<TScreen> screen;
        {
            // Use every even index page, without first and last key
            TVector<TScreen::THole> holes;
            auto index = Part0()->Index->Begin();
            while (index) {
                TRowId begin = index->GetRowId() + 1;
                TRowId end;
                if (auto next = index + 1) {
                    end = next->GetRowId() - 1;
                    index = next + 1;
                } else {
                    end = Part0()->Index.GetLastKeyRecord()->GetRowId();
                    ++index;
                }
                if (begin < end) {
                    holes.emplace_back(begin, end);
                }
            }
            UNIT_ASSERT_C(holes.size() > 2, "Generated screen has only " << holes.size() << " intervals");
            screen = new TScreen(std::move(holes));
        }
        auto result = RunLoaderTest(Part0(), screen);
        UNIT_ASSERT_C(result.Pages == screen->Size(),
            "Restoring slice bounds needed " << result.Pages <<
            " extra pages, expected " << screen->Size());
    }
}

}
}
