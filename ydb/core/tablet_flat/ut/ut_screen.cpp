#include <ydb/core/tablet_flat/flat_part_shrink.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_curtain.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

namespace {
    const NTest::TMass& Mass0()
    {
        static const NTest::TMass mass0(new NTest::TModelStd(false), 24000);
        return mass0;
    }

    const NTest::TPartEggs& Eggs0()
    {
        static const NTest::TPartEggs eggs0 = NTest::TPartCook::Make(Mass0(), { });
        UNIT_ASSERT_C(eggs0.Parts.size() == 1,
            "Unexpected " << eggs0.Parts.size() << " results");
        return eggs0;
    }
}

Y_UNIT_TEST_SUITE(TScreen) {

    Y_UNIT_TEST(Cuts)
    {
        using namespace NTable::NTest;
        using THole = TScreen::THole;

        TIntrusiveConstPtr<TScreen> one(new TScreen({ { 3, 10 }, { 40, 70 }, { 80, 99 } }));

        auto cut = [=](THole hole) { return TScreen::Cut(one, hole); };

        UNIT_ASSERT(cut({ 1, 3 })->Bounds() == THole(false));
        UNIT_ASSERT(cut(THole(false))->Bounds() == THole(false));
        UNIT_ASSERT(cut({ 0, 5 })->Cmp(TScreen({ THole(3, 5) })));
        UNIT_ASSERT(cut({ 7, 10 })->Cmp(TScreen({ THole(7, 10) })));
        UNIT_ASSERT(cut({ 7, 25 })->Cmp(TScreen({ THole(7, 10) })));
        UNIT_ASSERT(cut({ 7, 44 })->Cmp(TScreen({ {7, 10}, {40, 44} })));
        UNIT_ASSERT(cut({ 7, 91 })->Bounds() == THole({ 7, 91 }));
        UNIT_ASSERT(cut({ 0, 101 })->Cmp(*one));
        UNIT_ASSERT(cut(THole(true))->Cmp(*one));
        UNIT_ASSERT(cut({ 7, 91 })->Cmp(
                TScreen({ {7, 10 }, { 40, 70 }, { 80, 91 } })));
    }

    Y_UNIT_TEST(Join)
    {
        using namespace NTable::NTest;
        using THole = TScreen::THole;

        TIntrusiveConstPtr<TScreen> whole(new TScreen({{ 3, 10 }, { 40, 70 }, { 80, 99 }}));

        { /* Check simple split and merge w/o gliding edges */

            auto left  = TScreen::Cut(whole, { 0,  33 });
            auto right = TScreen::Cut(whole, { 33, 666 });
            auto join  = TScreen::Join(left, right);

            UNIT_ASSERT(join && join->Cmp(*whole));
        }

        { /* Check simple split and join to the same screen */
            auto left  = TScreen::Cut(whole, { 0,  55 });
            auto right = TScreen::Cut(whole, { 55, 666 });
            auto join  = TScreen::Join(left, right);

            UNIT_ASSERT(join && join->Cmp(*whole));
        }

        { /* Check trivialiry of joining to the complete hole */

            auto *left = new TScreen({ THole(0, 666) });
            auto *right = new TScreen({ THole(666, Max<TRowId>()) });

            UNIT_ASSERT(TScreen::Join(left, right) == nullptr);
        }
    }

    Y_UNIT_TEST(Sequential)
    {
        using namespace NTable::NTest;

        NTest::TCurtain cook(Eggs0());

        for (size_t off = 0; off < 16; off++) {
            const size_t len = off << 8;

            auto cu0 = cook.Make(Mass0().Saved, 3, (7 + (len >> 1) - 3) >> 1);
            auto cu = cook.Make(Mass0().Saved, 7 + (len >> 1), 13 + len);
            Y_ABORT_UNLESS(cu0.End != cu.Begin);

            for (int joined = 0; joined < 2; ++joined) {
                auto screen = joined ? TScreen::Join(cu0.Screen, cu.Screen) : cu.Screen;
                auto slice = TSlicer(*Eggs0().Scheme).Cut(*Eggs0().Lone(), *screen);

                { /*_ Check that simple screen is really working */
                    TCheckIter iter(Eggs0(), { new TForwardEnv(1, 2), 3 }, slice);

                    iter.To(4 + (off << 2) + (joined << 1))
                        .Seek({}, ESeek::Lower);

                    if (joined) {
                        iter.Is(cu0.Begin, cu0.End);
                    }

                    iter.Is(cu.Begin, cu.End);

                    iter.Is(EReady::Gone);
                }

                { /* Check working partial screening on hole edge */
                    TCheckIter iter(Eggs0(), { new TForwardEnv(1, 2), 3 }, slice);

                    auto it = cu.Begin + (len >> 2);

                    iter.To(5 + (off << 2) + (joined << 1))
                        .Seek(*it, ESeek::Lower)
                        .Is(it, cu.End);

                    iter.Is(EReady::Gone);
                }

            }
        }
    }

    Y_UNIT_TEST(Random)
    {
        using namespace NTable::NTest;

        NTest::TCurtain cook(Eggs0());

        TMersenne<ui64> rnd;

        for (size_t z = 0; z < 8192; z++) {
            auto cu = cook.Make(Mass0().Saved, rnd, 8192);
            auto slice = TSlicer(*Eggs0().Scheme).Cut(*Eggs0().Lone(), *cu.Screen);

            TCheckIter iter(Eggs0(), { new TTestEnv, 0 }, slice);

            iter.To(4 + z);

            if (rnd.Uniform(0, 2) == 0) {
                auto it = Mass0().Saved.AnyIn(rnd, cu.Begin, cu.End);

                iter.Seek(*it, ESeek::Exact).Is(*it, true);
                iter.Seek(*it, ESeek::Lower).Is(*it, true);
            } else {
                auto it = Mass0().Saved.AnyOff(rnd, cu.Begin, cu.End);

                iter.Seek(*it, ESeek::Exact).Is(EReady::Gone);

                if (it < cu.Begin) {
                    iter.Seek(*it, ESeek::Lower).Is(*cu.Begin);
                    iter.Seek(*it, ESeek::Upper).Is(*cu.Begin);
                } else if (it >= cu.End) {
                    iter.Seek(*it, ESeek::Lower).Is(EReady::Gone);
                    iter.Seek(*it, ESeek::Upper).Is(EReady::Gone);
                } else {
                    Y_ABORT("Got AnyOff row within the range");
                }
            }
        }
    }

    Y_UNIT_TEST(Shrink)
    {
        using namespace NTable::NTest;

        const TRowTool tool(*Eggs0().Scheme);
        TAutoPtr<TTestEnv> env = new TTestEnv;
        TShrink shrink(env.Get(), Eggs0().Scheme->Keys);

        { /* shrink to empty set shoudn't produce any results */
            const auto key = tool.LookupKey(Mass0().Saved[0]);

            shrink.Put(Eggs0().ToPartView(), { }, key);

            UNIT_ASSERT(shrink.Skipped == 0 && shrink.PartView.size() == 0);
        }

        { /* shrink to the entire set should leave part as is */
            const auto key = tool.LookupKey(Mass0().Saved[0]);

            shrink.Put(Eggs0().ToPartView(), key, { });

            UNIT_ASSERT(shrink.Skipped == 0 && shrink.PartView.size() == 1);

            const auto partView = std::move(shrink.PartView[0]);

            UNIT_ASSERT(!partView.Screen && partView.Part.Get() == Eggs0().At(0).Get());
        }

        { /* basic regular shrink of some trivial subset */
            const auto hole = TScreen::THole(666, 891);

            auto begin = tool.LookupKey(Mass0().Saved[hole.Begin]);
            auto end = tool.LookupKey(Mass0().Saved[hole.End]);

            shrink.Put(Eggs0().ToPartView(), begin, end);

            UNIT_ASSERT(shrink.Skipped == 0 && shrink.PartView.size() == 2);

            const auto scr = std::move(shrink.PartView[1].Screen);
            const auto run = std::move(shrink.PartView[1].Slices);

            UNIT_ASSERT(scr && shrink.PartView[1].Part.Get() == Eggs0().At(0).Get());
            UNIT_ASSERT(scr->Size() == 1 && scr->Hole(0) == hole);
            UNIT_ASSERT(run && run->size() == 1);
            UNIT_ASSERT(run->front().FirstRowId == hole.Begin);
            UNIT_ASSERT(run->front().FirstInclusive == true);
            UNIT_ASSERT(run->back().LastRowId == hole.End);
            UNIT_ASSERT(run->back().LastInclusive == false);
        }
    }

    Y_UNIT_TEST(Cook)
    {
        TScreen::TCook cook;

        // Using for the first time
        {
            cook.Pass(1);
            cook.Pass(2);
            cook.Pass(3);
            cook.Pass(10);
            cook.Pass(15);
            cook.Pass(16);

            auto result = cook.Unwrap();
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 3u);
            UNIT_ASSERT(result[0] == TScreen::THole(1, 4));
            UNIT_ASSERT(result[1] == TScreen::THole(10, 11));
            UNIT_ASSERT(result[2] == TScreen::THole(15, 17));
        }

        // Unwrap should start a new screen
        {
            cook.Pass(2);
            cook.Pass(5);

            auto result = cook.Unwrap();
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 2u);
            UNIT_ASSERT(result[0] == TScreen::THole(2, 3));
            UNIT_ASSERT(result[1] == TScreen::THole(5, 6));
        }
    }

}

}
}
