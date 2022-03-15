#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

namespace {

    const NTest::TMass Mass(new NTest::TModelStd(false), 4 * 9);

    struct TTouchEnv : public NTest::TTestEnv {
        using TArr = std::initializer_list<ui16>;

        TTouchEnv(bool fail) : Fail(fail) { }

        const TSharedData* TryGetPage(const TPart *part, TPageId id, TGroupId groupId) override
        {
            Y_VERIFY(groupId.IsMain(), "TODO: support column groups");
            Touched.insert(id);

            return Fail ? nullptr : NTest::TTestEnv::TryGetPage(part, id, groupId);
        }

        bool Is(const TArr &arr, ui32 flags) const noexcept
        {
            size_t hits = 0, miss = 0, seq = 0, has = arr.size();

            for (auto page: arr) {
                if (++seq && Touched.count(page)) {
                    hits++;
                } else if ((flags & 0x01) && has > 1 && seq == 1) {
                    miss++; /* may have no the fist page */
                } else if ((flags & 0x02) && has > 1 && seq == has) {
                    miss++; /* may have no the last page */
                }
            }

            return hits == Touched.size() && (hits + miss) == arr.size();
        }

        const bool Fail = false;
        TSet<TPageId> Touched;
    };

    struct TCooker {
        TCooker(const TRowScheme &scheme)
            : Tool(scheme)
            , Writer(new TPartScheme(scheme.Cols), { }, NPage::TGroupId(0))
        {

        }

        TCooker& Add(const NTest::TRow &row, ui64 offset, ui32 page)
        {
            const TCelled key(Tool.LookupKey(row), *Tool.Scheme.Keys, false);

            return Writer.Add(key, offset, page), *this;
        }

        TSharedData Flush()
        {
            return Writer.Flush();
        }

    private:
        const NTest::TRowTool Tool;
        NPage::TIndexWriter Writer;
    };

    struct TModel : public NTest::TSteps<TModel> {
        using TArr = std::initializer_list<ui16>;

        TModel()
            : Tool(*Mass.Model->Scheme)
        {
            auto pages = Eggs.At(0)->Index->End() - Eggs.At(0)->Index->Begin();

            UNIT_ASSERT(pages == 9);
        }

        static NTest::TPartEggs MakeEggs() noexcept
        {
            NPage::TConf conf{ true, 8192 };

            conf.Group(0).PageRows = 3; /* each page has 3 physical rows, but... */

            NTest::TPartCook cook(Mass.Model->Scheme, conf);

            for (auto seq: xrange(Mass.Saved.Size())) {
                /* ... but rows pack has 4 rows per each page, the first row in
                    each pack is ommited and used as spacer between pages. */

                if (seq % 4 > 0) cook.Add(Mass.Saved[seq]);
            }

            return cook.Finish();
        }

        void Check(ui32 lower, ui32 upper, ui64 items, TArr arr) const
        {
            Index(lower, upper, items, arr);
            Iter(lower, upper, items ? items : Max<ui32>(), arr);
        }

        void Index(ui32 lower, ui32 upper, ui64 items, TArr arr) const
        {
            Y_VERIFY(lower <= upper && upper < Mass.Saved.Size());

            TTouchEnv env(true);

            const auto &keyDefaults = *Tool.Scheme.Keys;
            const auto from = Tool.KeyCells(Mass.Saved[lower]);
            const auto to = Tool.KeyCells(Mass.Saved[upper]);

            TRun run(keyDefaults);
            auto part = Eggs.Lone();
            for (auto& slice : *part->Slices) {
                run.Insert(part, slice);
            }

            TCharge::Range(&env, from, to, run, keyDefaults, TTagsRef{ }, items, Max<ui64>());

            if (!env.Is(arr, 0x00 /* require all pages */)) {
                Log()
                    << "Charge over keys [" << lower << ", " << upper << "]"
                    << " lim " << (items ? items : 9999) << "r"
                    << ", touched " << NFmt::Arr(env.Touched)
                    << ", expected " << NFmt::Arr(arr) << Endl;

                UNIT_ASSERT(false);
            }
        }

        void Iter(ui32 lower, ui32 upper, ui64 items, TArr arr) const
        {
            Y_VERIFY(lower <= upper && upper < Mass.Saved.Size());

            NTest::TCheckIt wrap(Eggs, { new TTouchEnv(false) });

            wrap.To(CurrentStep()).Seek(Mass.Saved[lower], ESeek::Lower);

            for (ui32 key = lower + 1; items-- > 1 && key <= upper; key++) {
                if (key % 4 == 0) {
                    ++key;
                }

                auto last = (key == Mass.Saved.Size() - 1);

                wrap.Next().Is(last ? EReady::Gone : EReady::Data);
            }

            auto env = wrap.Displace<TTouchEnv>(nullptr);

            if (!env->Is(arr, 0x02 /* last page may be unused */)) {
                Log()
                    << "Iter over keys [" << lower << ", " << upper << "]"
                    << " touched pages " << NFmt::Arr(env->Touched)
                    << ", expected " << NFmt::Arr(arr) << Endl;

                UNIT_ASSERT(false);
            }
        }

        const NTest::TRowTool Tool;
        const NTest::TPartEggs Eggs = MakeEggs();
    };

}

Y_UNIT_TEST_SUITE(Charge) {

    using namespace NTest;

    Y_UNIT_TEST(Lookups)
    {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        const auto foo = *TSchemedCookRow(*lay).Col(555_u32, "foo");
        const auto bar = *TSchemedCookRow(*lay).Col(777_u32, "bar");
        const auto baz = *TSchemedCookRow(*lay).Col(999_u32, "baz");

        NPage::TIndex me(
            TCooker(*lay)
                .Add(foo, 0, 1)
                .Add(bar, 10, 2)
                .Add(baz, 19, 2)
                .Flush());

        /* TODO: Special row keys such as { } or incomplete keys with +inf
            cells are not used in this UT set. It is ok since precharge
            code do not read keys bypassing it directly to NPage::TIndex.
            Thus, need to test TIndex::Lookup () for that cases only. */
    }

    Y_UNIT_TEST(Basics)
    {
        using TArr = TTouchEnv::TArr;

        TModel me;

        { /*_ 1xx: A set of some edge and basic cases */

            me.To(100).Check(33, 34, 0, { 8 });     // keys on the last page
            me.To(101).Check(0, 0, 0, { 0, 1 });    // keys before the part
            me.To(102).Check(3, 4, 0, { 0, 1 });
            me.To(103).Check(3, 5, 0, { 0, 1, 2 });
            me.To(104).Check(4, 8, 0, { 0, 1, 2 });
            me.To(105).Check(4, 12, 0, { 0, 1, 2, 3 });
            me.To(106).Check(8, 12, 0, { 1, 2, 3 });
            me.To(107).Check(8, 13, 0, { 1, 2, 3, 4 });
            me.To(108).Check(8, 14, 0, { 1, 2, 3, 4 });
            me.To(110).Check(0, 35, 0, { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        }

        /*_ 2xx: Simple loads rows within the same page */

        for (const ui16 it: xrange(8)) {
            const TArr span{ it, ui16(it + 1) };

            const ui32 base = 200 + it * 10, lead = it * 4;

            me.To(base + 1).Check(lead + 1, lead + 1, 0, span);
            me.To(base + 2).Check(lead + 1, lead + 2, 0, span);
            me.To(base + 3).Check(lead + 1, lead + 3, 0, span);
            me.To(base + 4).Check(lead + 2, lead + 3, 0, span);
            me.To(base + 5).Check(lead + 3, lead + 3, 0, span);
        }

        /*_ 3xx: Simple loads spans more than one page */

        for (const ui16 it: xrange(5)) {
            const TArr span{ it, ui16(it + 1), ui16(it + 2), ui16(it + 3) };

            const ui32 base = 300 + it * 10, lead = it * 4;

            me.To(base + 1).Check(lead + 1, lead + 9, 0, span);
            me.To(base + 2).Check(lead + 1, lead + 10, 0, span);
            me.To(base + 3).Check(lead + 1, lead + 11, 0, span);
        }
    }

    Y_UNIT_TEST(Limits)
    {
        /* XXX: Precharger is conservative about row limits, because it
            cannot know how many rows will really be consumed on the
            first page. Precharge will also add one extra row because
            datashard uses it to check for truncation, thus the limit
            of 8 (3 rows per page) rows really assumes 4 pages.
         */

        TModel me;

        /*_ 1xx: custom spanned loads scenarios */

        me.To(101).Index(0, 35, 8 /* rows */, { 0, 1, 2, 3 });
        me.To(102).Index(0, 35, 11 /* rows */, { 0, 1, 2, 3, 4 });
        me.To(103).Index(0, 35, 14 /* rows */, { 0, 1, 2, 3, 4, 5 });
        me.To(104).Index(3, 35, 5 /* rows */, { 0, 1, 2 });
        me.To(105).Index(3, 35, 6 /* rows */, { 0, 1, 2, 3 });
        me.To(106).Index(4, 35, 6 /* rows */, { 0, 1, 2, 3 });
        me.To(107).Index(5, 35, 5 /* rows */, { 1, 2, 3 });
        me.To(112).Index(9, 35, 11 /* rows */, { 2, 3, 4, 5, 6 });
        me.To(113).Index(9, 35, 14 /* rows */, { 2, 3, 4, 5, 6, 7 });
        me.To(120).Index(25, 35, 8 /* rows */, { 6, 7, 8 });

        /*_ 2xx: one row charge limit on two page */

        for (const ui16 page : xrange(4)) {
            const  TTouchEnv::TArr span{ page, ui16(page + 1) };

            const ui32 base = 200 + page * 10, lead = page * 4;

            me.To(base + 1).Check(lead + 1, 35, 1, span);
            me.To(base + 2).Check(lead + 2, 35, 1, span);
            me.To(base + 3).Check(lead + 3, 35, 1, span);
        }
    }
}

}
}
