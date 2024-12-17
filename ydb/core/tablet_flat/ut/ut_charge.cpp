#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge_range.h>
#include <ydb/core/tablet_flat/flat_part_charge_create.h>
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
    enum TPageIdFlags {
        IfIter = 1,
        IfFail = 2,
        IfNoFail = 4,
        IfSticky = 8
    };
    struct TFlaggedPageId {
        TPageId Page;
        TPageIdFlags Flags = static_cast<TPageIdFlags>(TPageIdFlags::IfIter | TPageIdFlags::IfFail | TPageIdFlags::IfNoFail | TPageIdFlags::IfSticky);

        TFlaggedPageId(TPageId page)
            : Page(page) {}

        TFlaggedPageId(TPageId page, TPageIdFlags flags)
            : Page(page), Flags(flags) {}
    };
    // if fail only
    TFlaggedPageId operator""_f(unsigned long long page)
    {
        return TFlaggedPageId(page, TPageIdFlags::IfFail);
    }
    // if iter or no fail
    TFlaggedPageId operator""_g(unsigned long long page)
    {
        return TFlaggedPageId(page, static_cast<TPageIdFlags>(TPageIdFlags::IfIter | TPageIdFlags::IfNoFail));
    }
    // if no iter
    TFlaggedPageId operator""_I(unsigned long long page)
    {
        return TFlaggedPageId(page, static_cast<TPageIdFlags>(TPageIdFlags::IfFail | TPageIdFlags::IfNoFail));
    }

    struct TTouchEnv : public NTest::TTestEnv {
        TTouchEnv(bool fail, TSet<std::pair<TGroupId, TPageId>> sticky) 
            : Fail(fail)
            , Sticky(std::move(sticky))
            { }

        const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
        {
            Touched[groupId].insert(pageId);
            
            if (!Fail || Sticky.contains({groupId, pageId})) {
                return NTest::TTestEnv::TryGetPage(part, pageId, groupId);
            }

            ToLoad[groupId].insert(pageId);
            return nullptr;
        }

        const bool Fail = false;
        TSet<std::pair<TGroupId, TPageId>> Sticky;
        TMap<TGroupId, TSet<TPageId>> Touched;
        TMap<TGroupId, TSet<TPageId>> ToLoad;
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
        NPage::TFlatIndexWriter Writer;
    };

    struct TModel : public NTest::TSteps<TModel> {
        using TArr = std::initializer_list<TFlaggedPageId>;
        using TGroupId = NPage::TGroupId;

        TModel(bool groups = false, bool history = false)
            : Mass(new NTest::TModelStd(groups), 4 * 9)
            , Eggs(MakeEggs(groups, history))
            , Tool(*Mass.Model->Scheme)
        {
            UNIT_ASSERT(NTest::IndexTools::CountMainPages(*Eggs.Lone()) == 9);
        }

        NTest::TPartEggs MakeEggs(bool groups, bool history) noexcept
        {
            NPage::TConf conf{ true, 8192 };

            auto families = Mass.Model->Scheme->Families.size();
            for (size_t family : xrange(families)) {
                conf.Group(family).PageRows = 3; /* each main page has 3 physical rows, but... */
            }
            if (groups) {
                conf.Group(1).PageRows = 2;
                conf.Group(2).PageRows = 1;
            }
            // TODO: rewrite tests when we deprecate flat index
            conf.WriteBTreeIndex = false;
            conf.WriteFlatIndex = true;

            NTest::TPartCook cook(Mass.Model->Scheme, conf);

            for (auto seq: xrange(Mass.Saved.Size())) {
                /* ... but rows pack has 4 rows per each page, the first row in
                    each pack is omitted and used as spacer between pages. */

                if (seq % 4 > 0) {
                    if (history) {
                        for (ui64 v : { 2, 1 })
                            cook.Ver({0, v}).Add(Mass.Saved[seq]);
                    } else {
                        cook.Add(Mass.Saved[seq]);
                    }
                }
            }

            return cook.Finish();
        }

        void CheckByKeys(ui32 lower, ui32 upper, ui64 items, TArr shouldPrecharge) const
        {
            CheckByKeys(lower, upper, items, {{TGroupId{}, shouldPrecharge}});
        }

        void CheckByKeys(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByKeys(lower, upper, items, TPageIdFlags::IfNoFail, shouldPrecharge, false, GetIndexPages());
            CheckPrechargeByKeys(lower, upper, items, TPageIdFlags::IfFail, shouldPrecharge, false, GetIndexPages());
            CheckIterByKeys(lower, upper, items ? items : Max<ui32>(), shouldPrecharge);
        }

        void CheckByKeysReverse(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByKeys(lower, upper, items, TPageIdFlags::IfNoFail, shouldPrecharge, true, GetIndexPages());
            CheckPrechargeByKeys(lower, upper, items, TPageIdFlags::IfFail, shouldPrecharge, true, GetIndexPages());
            CheckIterByKeysReverse(lower, upper, items ? items : Max<ui32>(), shouldPrecharge);
        }

        void CheckByRows(TPageId row1, TPageId row2, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByRows(row1, row2, items, false, shouldPrecharge, false);
            CheckPrechargeByRows(row1, row2, items, true, shouldPrecharge, false);
        }

        void CheckByRowsReverse(TPageId row1, TPageId row2, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByRows(row1, row2, items, false, shouldPrecharge, true);
            CheckPrechargeByRows(row1, row2, items, true, shouldPrecharge, true);
        }

        void CheckIndex(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge, TSet<TPageId> stickyIndex) const {
            TSet<std::pair<TGroupId, TPageId>> sticky;
            for (auto x : stickyIndex) {
                sticky.insert({TGroupId{}, x});
            }

            CheckPrechargeByKeys(lower, upper, items, static_cast<TPageIdFlags>(TPageIdFlags::IfFail | TPageIdFlags::IfSticky), shouldPrecharge, false, sticky);
        }

        void CheckPrechargeByKeys(ui32 lower, ui32 upper, ui64 items, TPageIdFlags flags, const TMap<TGroupId, TArr>& shouldPrecharge, bool reverse, TSet<std::pair<TGroupId, TPageId>> sticky) const
        {
            Y_ABORT_UNLESS(lower < Mass.Saved.Size() && upper < Mass.Saved.Size());

            bool fail(flags & TPageIdFlags::IfFail);
            TTouchEnv env(fail, sticky);

            const auto &keyDefaults = *Tool.Scheme.Keys;
            const auto from = Tool.KeyCells(Mass.Saved[lower]);
            const auto to = Tool.KeyCells(Mass.Saved[upper]);

            TRun run(keyDefaults);
            auto part = Eggs.Lone();
            for (auto& slice : *part->Slices) {
                run.Insert(part, slice);
            }

            auto tags = TVector<TTag>();
            for (auto c : Mass.Model->Scheme->Cols) {
                tags.push_back(c.Tag);
            }

            bool ready = !reverse
                ? ChargeRange(&env, from, to, run, keyDefaults, tags, items, Max<ui64>(), true)
                : ChargeRangeReverse(&env, from, to, run, keyDefaults, tags, items, Max<ui64>(), true);

            UNIT_ASSERT_VALUES_EQUAL_C(!fail || env.ToLoad.empty(), ready, AssertMessage(fail));

            CheckPrecharged(env.Touched, shouldPrecharge, sticky, flags);
        }

        void CheckPrechargeByRows(TPageId row1, TPageId row2, ui64 items, bool fail, TMap<TGroupId, TArr> shouldPrecharge, bool reverse) const
        {
            auto sticky = GetIndexPages();
            TTouchEnv env(fail, sticky);

            const auto &keyDefaults = *Tool.Scheme.Keys;
            
            TRun run(keyDefaults);
            auto part = Eggs.Lone();
            for (auto& slice : *part->Slices) {
                run.Insert(part, slice);
            }

            auto tags = TVector<TTag>();
            for (auto c : Mass.Model->Scheme->Cols) {
                tags.push_back(c.Tag);
            }

            auto charge = CreateCharge(&env, *run.begin()->Part, tags, false);
            bool result = reverse
                ? charge->DoReverse(row1, row2, keyDefaults, items, Max<ui64>())
                : charge->Do(row1, row2, keyDefaults, items, Max<ui64>());

            UNIT_ASSERT_VALUES_EQUAL_C(!fail, result, AssertMessage(fail));

            CheckPrecharged(env.Touched, shouldPrecharge, sticky, fail ? TPageIdFlags::IfFail : TPageIdFlags::IfNoFail);
        }

        void CheckIterByKeys(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& precharged) const
        {
            Y_ABORT_UNLESS(lower < Mass.Saved.Size() && upper < Mass.Saved.Size());

            auto sticky = GetIndexPages();
            NTest::TCheckIter wrap(Eggs, { new TTouchEnv(false, sticky) });

            wrap.To(CurrentStep());
            wrap.StopAfter(Tool.KeyCells(Mass.Saved[upper]));

            auto seek = true;
            for (ui32 key = lower; items-- + 1 > 0; key++) {
                if (key % 4 == 0) {
                    ++key;
                }

                if (seek) {
                    wrap.Seek(Mass.Saved[lower], ESeek::Lower);
                    seek = false;
                } else {
                    wrap.Next().Is(key > upper ? EReady::Gone : EReady::Data);
                }

                // forcibly touch the next stop element that is greater than upper
                // because instead of having |1 2 3| and stopping as soon as we see 2
                // we may have |1*2 2*2 3*2| = |2 4 6| and be requested with upper = 5 (not 4)
                if (key > upper) {
                    break;
                }
            }

            auto env = wrap.Displace<TTouchEnv>(nullptr);

            CheckPrecharged(env->Touched, precharged, sticky, TPageIdFlags::IfIter);
        }

        void CheckIterByKeysReverse(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& precharged) const
        {
            Y_ABORT_UNLESS(lower < Mass.Saved.Size() && upper < Mass.Saved.Size());

            auto sticky = GetIndexPages();
            NTest::TCheckReverseIter wrap(Eggs, { new TTouchEnv(false, sticky) });

            wrap.To(CurrentStep());
            wrap.StopAfter(Tool.KeyCells(Mass.Saved[upper]));

            auto seek = true;
            for (ui32 key = lower; items-- + 1 > 0; key--) {
                if (key % 4 == 0) {
                    --key;
                }

                if (seek) {
                    wrap.Seek(Mass.Saved[lower], ESeek::Lower);
                    seek = false;
                } else {
                    wrap.Next().Is(key < upper || key == (ui32)-1 ? EReady::Gone : EReady::Data);
                }

                // forcibly touch the next stop element that is greater than upper
                // because instead of having |1 2 3| and stopping as soon as we see 2
                // we may have |1*2 2*2 3*2| = |2 4 6| and be requested with upper = 2 (not 4)
                if (key < upper || key == (ui32)-1) {
                    break;
                }
            }

            auto env = wrap.Displace<TTouchEnv>(nullptr);

            CheckPrecharged(env->Touched, precharged, sticky, TPageIdFlags::IfIter);
        }

        const NTest::TMass Mass;
        const NTest::TPartEggs Eggs;
        const NTest::TRowTool Tool;

    private:
        TSet<std::pair<TGroupId, TPageId>> GetIndexPages() const {
            TSet<std::pair<TGroupId, TPageId>> result;

            auto &pages = Eggs.Lone()->IndexPages;
            TGroupId mainGroupId{};
            
            for (auto x : pages.FlatGroups) {
                result.insert({mainGroupId, x});
            }
            for (auto x : pages.FlatHistoric) {
                result.insert({mainGroupId, x});
            }

            for (auto &x : pages.BTreeGroups) {
                result.insert({mainGroupId, x.GetPageId()});
            }
            for (auto &x : pages.BTreeHistoric) {
                result.insert({mainGroupId, x.GetPageId()});
            }

            return result;
        }

        void CheckPrecharged(const TMap<TGroupId, TSet<TPageId>>& actual, const TMap<TGroupId, TArr>& expected, TSet<std::pair<TGroupId, TPageId>> sticky, TPageIdFlags flags) const {
            for (auto [groupId, arr] : expected) {
                if (groupId.IsHistoric() && flags == TPageIdFlags::IfIter) {
                    // isn't supported
                    continue;
                }

                TMap<ui64, ui64> absoluteId;
                NTest::TTestEnv env;
                auto groupIndex = CreateIndexIter(Eggs.Lone().Get(), &env, groupId);
                for (size_t i = 0; ; i++) {
                    auto ready = i == 0 ? groupIndex->Seek(0) : groupIndex->Next();
                    if (ready != EReady::Data) {
                        Y_ABORT_UNLESS(ready != EReady::Page);
                        break;
                    }
                    absoluteId[absoluteId.size()] = groupIndex->GetPageId();
                }

                TSet<TPageId> actualValue;
                for (auto p : actual.Value(groupId, TSet<TPageId>())) {
                    if (flags & TPageIdFlags::IfSticky || !sticky.contains({groupId, p})) {
                        actualValue.insert(p);
                    }
                }

                auto expectedValue = TSet<TPageId>{};
                for (auto p  : arr) {
                    if (flags & p.Flags) {
                        expectedValue.insert(absoluteId.Value(p.Page, p.Page));
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(expectedValue, actualValue, AssertMessage(groupId, flags));
            }
        }

        std::string AssertMessage(bool fail) const {
            return 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Fail: " + (fail ? "Yes" : "No");
        }
        std::string AssertMessage(TGroupId group) const {
            return 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Group: " + std::to_string(group.Index) + "," + std::to_string(group.IsHistoric());
        }
        std::string AssertMessage(TGroupId group, TPageIdFlags flags) const {
            auto result = 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Group: " + std::to_string(group.Index) + "," + std::to_string(group.IsHistoric());

            if (flags & TPageIdFlags::IfFail) {
                result += " Fail: Yes";
            }
            if (flags & TPageIdFlags::IfNoFail) {
                result += " Fail: No";
            }
            if (flags & TPageIdFlags::IfIter) {
                result += " Iter";
            }

            return result;
        }
    };

}

Y_UNIT_TEST_SUITE(Charge) {
    using namespace NTest;

    using TArr = TModel::TArr;
    using TGroupId = NPage::TGroupId;

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

        NPage::TFlatIndex me(
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

    Y_UNIT_TEST(ByKeysBasics)
    {
        TModel me;

        { /*_ 1xx: A set of some edge and basic cases */

            me.To(100).CheckByKeys(33, 34, 0, { 8 });     // keys on the last page
            me.To(101).CheckByKeys(0, 0, 0, { 0, 1_I });    // keys before the part
            me.To(102).CheckByKeys(3, 4, 0, { 0, 1 });
            me.To(103).CheckByKeys(3, 5, 0, { 0, 1, 2_I });
            me.To(104).CheckByKeys(4, 8, 0, { 0, 1, 2 });
            me.To(105).CheckByKeys(4, 12, 0, { 0, 1, 2, 3 });
            me.To(106).CheckByKeys(8, 12, 0, { 1, 2, 3 });
            me.To(107).CheckByKeys(8, 13, 0, { 1, 2, 3, 4_I });
            me.To(108).CheckByKeys(8, 14, 0, { 1, 2, 3, 4_I });
            me.To(110).CheckByKeys(0, 35, 0, { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        }

        /*_ 2xx: Simple loads rows within the same page */

        for (const ui16 it: xrange(8)) {
            const TArr span1{ it, operator""_I(it + 1) };
            const TArr span2{ it, it + 1 };

            const ui32 base = 200 + it * 10, lead = it * 4;

            me.To(base + 1).CheckByKeys(lead + 1, lead + 1, 0, span1);
            me.To(base + 2).CheckByKeys(lead + 1, lead + 2, 0, span1);
            me.To(base + 3).CheckByKeys(lead + 1, lead + 3, 0, span2);
            me.To(base + 4).CheckByKeys(lead + 2, lead + 3, 0, span2);
            me.To(base + 5).CheckByKeys(lead + 3, lead + 3, 0, span2);
        }

        /*_ 3xx: Simple loads spans more than one page */

        for (const ui16 it: xrange(5)) {
            const TArr span1{ it, it + 1, it + 2, operator""_I(it + 3) };
            const TArr span2{ it, it + 1, it + 2, it + 3 };

            const ui32 base = 300 + it * 10, lead = it * 4;

            me.To(base + 1).CheckByKeys(lead + 1, lead + 9, 0, span1);
            me.To(base + 2).CheckByKeys(lead + 1, lead + 10, 0, span1);
            me.To(base + 3).CheckByKeys(lead + 1, lead + 11, 0, span2);
        }

        { /*_ 4xx: key2 > key1 */
            me.To(400).CheckByKeys(9, 8, 0, { 2, 3_I });
            me.To(401).CheckByKeys(9, 7, 0, { 2, 3_I });
            me.To(402).CheckByKeys(9, 0, 0, { 2, 3_I });
            me.To(403).CheckByKeys(8, 7, 0, { 1, 2 });
            me.To(404).CheckByKeys(8, 6, 0, { 1, 2 });
            me.To(405).CheckByKeys(8, 0, 0, { 1, 2 });
            me.To(406).CheckByKeys(7, 6, 0, { 1, 2_I });
            me.To(407).CheckByKeys(7, 5, 0, { 1, 2_I });
            me.To(408).CheckByKeys(7, 0, 0, { 1, 2_I });
            me.To(409).CheckByKeys(6, 5, 0, { 1, 2_I });
            me.To(410).CheckByKeys(6, 4, 0, { 1, 2_I });
            me.To(411).CheckByKeys(6, 0, 0, { 1, 2_I });
            me.To(412).CheckByKeys(5, 4, 0, { 1, 2_I });
            me.To(413).CheckByKeys(5, 3, 0, { 1, 2_I });
            me.To(414).CheckByKeys(5, 0, 0, { 1, 2_I });
        }
    }

    Y_UNIT_TEST(ByKeysGroups)
    {
        TModel me(true);

        /*
        
        keys by pages:
        group0 = |1 2 3|5 6 7|9 10 11|13 14 15|..
        group1 = |1 2|3 5|6 7|9 10|11 13|14 15|..
        group3 = |1|2|3|5|6|7|9|10|11|13|14|15|..
        
        */

        /*_ 1xx: Play with first page */ {
            me.To(100).CheckByKeys(0, 0, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1_I}},
                {TGroupId{1}, {}},
                {TGroupId{2}, {}}
            });

            // key 0 transforms into row id 0 because it's before the slice first key 1
            me.To(101).CheckByKeys(0, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {0, 1, 2, 3_g}},
                {TGroupId{2}, {0, 1, 2, 3, 4, 5, 6_g}}
            });

            // key 1 also transforms into row id 0 because it's before the slice first key 1
            me.To(102).CheckByKeys(1, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {0, 1, 2, 3_g}},
                {TGroupId{2}, {0, 1, 2, 3, 4, 5, 6_g}}
            });

            me.To(103).CheckByKeys(2, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {0_g, 1, 2, 3_g}},
                {TGroupId{2}, {1_g, 2_g, 3, 4, 5, 6_g}}
            });

            me.To(104).CheckByKeys(3, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {1, 2, 3_g}},
                {TGroupId{2}, {2_g, 3, 4, 5, 6_g}}
            });

            me.To(105).CheckByKeys(4, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {1, 2, 3_g}},
                {TGroupId{2}, {3, 4, 5, 6_g}}
            });

            me.To(106).CheckByKeys(5, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3_I}},
                {TGroupId{1}, {1_g, 2_g, 3_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6_g}}
            });
        }

        /*_ 2xx: Play with intermidiate pages */ {
            me.To(200).CheckByKeys(4, 4, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1}},
                {TGroupId{1}, {}},
                {TGroupId{2}, {}}
            });
            
            me.To(201).CheckByKeys(5, 5, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2_I}},
                {TGroupId{1}, {1_g}},
                {TGroupId{2}, {3_g}}
            });

            me.To(202).CheckByKeys(5, 6, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2_I}},
                {TGroupId{1}, {1_g, 2_g}},
                {TGroupId{2}, {3_g, 4_g}}
            });

            me.To(203).CheckByKeys(5, 7, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2}},
                {TGroupId{1}, {1_g, 2_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g}}
            });

            me.To(204).CheckByKeys(5, 8, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2}},
                {TGroupId{1}, {1_g, 2_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g}} // don't touch 9's columns
            });

            me.To(205).CheckByKeys(5, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3_I}},
                {TGroupId{1}, {1_g, 2_g, 3_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6_g}}
            });

            me.To(206).CheckByKeys(5, 10, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3_I}},
                {TGroupId{1}, {1_g, 2_g, 3_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6_g, 7_g}}
            });

            me.To(207).CheckByKeys(5, 11, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {1_g, 2_g, 3_g, 4_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6_g, 7_g, 8_g}}
            });

            me.To(208).CheckByKeys(5, 12, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {1_g, 2_g, 3_g, 4_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6_g, 7_g, 8_g}}  // don't touch 13's columns
            });

            me.To(209).CheckByKeys(5, 13, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4_I}},
                {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always needed
                {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
            });

            me.To(210).CheckByKeys(5, 18, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5_I}},
                {TGroupId{1}, {1_g, 2_g, 3, 4, 5, 6_g}},
                {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9, 10, 11, 12_g, 13_g}}
            });

            me.To(211).CheckByKeys(6, 11, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {2_g, 3_g, 4_g}},
                {TGroupId{2}, {4_g, 5_g, 6_g, 7_g, 8_g}}
            });

            me.To(212).CheckByKeys(6, 12, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {2_g, 3_g, 4_g}},
                {TGroupId{2}, {4_g, 5_g, 6_g, 7_g, 8_g}}
            });

            me.To(213).CheckByKeys(6, 13, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4_I}},
                {TGroupId{1}, {2_g, 3, 4}}, // pages 3, 4 are always needed
                {TGroupId{2}, {4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
            });

            me.To(214).CheckByKeys(7, 11, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {2_g, 3_g, 4_g}},
                {TGroupId{2}, {5_g, 6_g, 7_g, 8_g}}
            });

            me.To(215).CheckByKeys(7, 12, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {2_g, 3_g, 4_g}},
                {TGroupId{2}, {5_g, 6_g, 7_g, 8_g}}
            });

            me.To(216).CheckByKeys(7, 13, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4_I}},
                {TGroupId{1}, {2_g, 3, 4}}, // pages 3, 4 are always needed
                {TGroupId{2}, {5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
            });

            me.To(217).CheckByKeys(8, 11, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {3_g, 4_g}},
                {TGroupId{2}, {6_g, 7_g, 8_g}}
            });

            me.To(218).CheckByKeys(8, 12, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3}},
                {TGroupId{1}, {3_g, 4_g}},
                {TGroupId{2}, {6_g, 7_g, 8_g}}
            });

            me.To(219).CheckByKeys(8, 13, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4_I}},
                {TGroupId{1}, {3, 4}}, // pages 3, 4 are always needed
                {TGroupId{2}, {6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
            });

            me.To(220).CheckByKeys(9, 13, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {2, 3, 4_I}},
                {TGroupId{1}, {3_g, 4_g}},
                {TGroupId{2}, {6_g, 7_g, 8_g, 9_g}}
            });
        }

        /*_ 3xx: Play with last page */ {
            me.To(300).CheckByKeys(32, 32, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {7, 8}},
                {TGroupId{1}, {}},
                {TGroupId{2}, {}}
            });

            // key 35 transforms into row id 26 because it's after the slice last key 35
            me.To(301).CheckByKeys(27, 35, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10, 11, 12, 13}},
                {TGroupId{2}, {20_g, 21, 22, 23, 24, 25, 26}}
            });

            me.To(302).CheckByKeys(27, 34, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10, 11, 12_g}},
                {TGroupId{2}, {20_g, 21, 22, 23, 24_g, 25_g}}
            });

            me.To(303).CheckByKeys(27, 33, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10, 11, 12_g}},
                {TGroupId{2}, {20_g, 21, 22, 23, 24_g}}
            });

            me.To(305).CheckByKeys(27, 32, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10_g, 11_g}},
                {TGroupId{2}, {20_g, 21_g, 22_g, 23_g}}
            });

            me.To(306).CheckByKeys(27, 31, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10_g, 11_g}},
                {TGroupId{2}, {20_g, 21_g, 22_g, 23_g}}
            });
        }

        { /*_ 4xx: key2 > key1 */
            me.To(400).CheckByKeys(9, 8, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}});
            me.To(401).CheckByKeys(9, 7, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}});
            me.To(402).CheckByKeys(9, 0, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(403).CheckByKeys(8, 7, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(404).CheckByKeys(8, 6, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(405).CheckByKeys(8, 0, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(406).CheckByKeys(7, 6, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(407).CheckByKeys(7, 5, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}});
            me.To(408).CheckByKeys(7, 0, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(409).CheckByKeys(6, 5, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(410).CheckByKeys(6, 4, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(411).CheckByKeys(6, 0, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(412).CheckByKeys(5, 4, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(413).CheckByKeys(5, 3, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
            me.To(414).CheckByKeys(5, 0, 0, TMap<TGroupId, TArr>{{TGroupId{2}, {}}}); 
        }
    }

    Y_UNIT_TEST(ByKeysGroupsLimits)
    {
        TModel me(true);

        /*
        
        keys by pages:
        group0 = |1 2 3|5 6 7|9 10 11|13 14 15|..
        group1 = |1 2|3 5|6 7|9 10|11 13|14 15|..
        group3 = |1|2|3|5|6|7|9|10|11|13|14|15|..
        
        */

        me.To(101).CheckByKeys(5, 13, 999, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_I}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always needed
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
        });

        me.To(102).CheckByKeys(5, 13, 7, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always needed
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
        });

        me.To(103).CheckByKeys(5, 13, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}},
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}}
        });

        me.To(104).CheckByKeys(5, 13, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}},
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8}}
        });

        me.To(105).CheckByKeys(5, 13, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4_f}}, // here we touch extra pages, but it's fine
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8_f}} // here we touch extra pages, but it's fine
        });

        me.To(106).CheckByKeys(7, 13, 3, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f}},
            {TGroupId{1}, {2_g, 3, 4}},
            {TGroupId{2}, {5_g, 6, 7, 8}}
        });
    }

    Y_UNIT_TEST(ByKeysLimits)
    {
        /* XXX: Precharger may be conservative about row limits, because it
            cannot know how many rows will really be consumed on the
            first page. Precharge will also add one extra row because
            datashard uses it to check for truncation, thus the limit
            of 8 (3 rows per page) rows really assumes 4 pages.
         */

        TModel me;

        /*_ 1xx: custom spanned loads scenarios */

        // key 0 transforms into row id 0 because it's before the slice first key 1
        me.To(101).CheckByKeys(0, 35, 8 /* rows */, { 0, 1, 2 });
        me.To(102).CheckByKeys(0, 35, 11 /* rows */, { 0, 1, 2, 3 });
        me.To(103).CheckByKeys(0, 35, 14 /* rows */, { 0, 1, 2, 3, 4 });

        me.To(104).CheckByKeys(3, 35, 5 /* rows */, { 0, 1, 2 });
        me.To(105).CheckByKeys(3, 35, 6 /* rows */, { 0, 1, 2, 3_I });
        me.To(106).CheckByKeys(4, 35, 6 /* rows */, { 0, 1, 2, 3 });
        me.To(107).CheckByKeys(5, 35, 5 /* rows */, { 1, 2, 3_I });
        me.To(112).CheckByKeys(9, 35, 11 /* rows */, { 2, 3, 4, 5, 6_I });
        me.To(113).CheckByKeys(9, 35, 14 /* rows */, { 2, 3, 4, 5, 6, 7_I });
        me.To(120).CheckByKeys(25, 35, 8 /* rows */, { 6, 7, 8 });


        /*_ 2xx: one row charge limit on two page */

        for (const ui16 page : xrange(1, 5)) {
            const TArr span1{ page, operator""_I(page + 1) };
            const TArr span2{ page, page + 1 };

            const ui32 base = 200 + page * 10, lead = page * 4;

            me.To(base + 1).CheckByKeys(lead + 1, 35, 1, span1);
            me.To(base + 2).CheckByKeys(lead + 2, 35, 1, span1);
            me.To(base + 3).CheckByKeys(lead + 3, 35, 1, span2);
        }
    }

    Y_UNIT_TEST(ByKeysReverse)
    {
        TModel me(true);

        /*
        
        keys by pages:
        group0 = |1 2 3|5 6 7|9 10 11|13 14 15|..
        group1 = |1 2|3 5|6 7|9 10|11 13|14 15|..
        group3 = |1|2|3|5|6|7|9|10|11|13|14|15|..
        
        */

        me.To(100).CheckByKeysReverse(15, 15, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2_I}},
            {TGroupId{2}, {11_g}}
        });

        me.To(101).CheckByKeysReverse(15, 5, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5_g, 4_g, 3_g}}
        });

        me.To(102).CheckByKeysReverse(15, 4, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3}}
        });

        me.To(103).CheckByKeysReverse(15, 3, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3, 2_g}}
        });

        // key 1 transforms into row id 0 because it's before the slice first key 1
        me.To(104).CheckByKeysReverse(15, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3, 2, 1, 0}}
        });

        me.To(105).CheckByKeysReverse(15, 0, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3, 2, 1, 0}}
        });

        me.To(106).CheckByKeysReverse(17, 17, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {4, 3}},
            {TGroupId{2}, {12_g}}
        });
        
        me.To(107).CheckByKeysReverse(16, 16, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2_I}},
            {TGroupId{2}, {}}
        });

        me.To(108).CheckByKeysReverse(35, 35, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7_I}},
            {TGroupId{2}, {26_g}}
        });

        me.To(109).CheckByKeysReverse(35, 33, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{2}, {26_g, 25_g, 24_g}}
        });

        // key 35 transforms into row id 26 because it's after the slice last key 35
        me.To(110).CheckByKeysReverse(35, 32, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7, 6_I}},
            {TGroupId{2}, {26, 25, 24}}
        });

        me.To(111).CheckByKeysReverse(4, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{2}, {2_g, 1_g, 0_g}}
        });

        me.To(112).CheckByKeysReverse(1, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{2}, {0_g}}
        });

        me.To(113).CheckByKeysReverse(1, 0, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{2}, {0_g}}
        });

        me.To(114).CheckByKeysReverse(0, 0, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {}},
            {TGroupId{2}, {}}
        });

        me.To(200).CheckByKeysReverse(15, 3, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0_f}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4_f, 3_f}} // here we touch extra pages, but it's fine
        });

        me.To(201).CheckByKeysReverse(15, 3, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5_f, 4_f, 3_f}} // here we touch extra pages, but it's fine
        });

        me.To(202).CheckByKeysReverse(15, 5, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6}}
        });

        me.To(203).CheckByKeysReverse(13, 3, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1}},
            {TGroupId{2}, {9_g, 8, 7, 6, 5, 4_f}} // here we touch extra pages, but it's fine
        });

        me.To(204).CheckByKeysReverse(13, 3, 3, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {9_g, 8, 7, 6, 5_f}} // here we touch extra pages, but it's fine
        });

        me.To(205).CheckByKeysReverse(13, 3, 2, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2}},
            {TGroupId{2}, {9_g, 8, 7, 6_f}} // here we touch extra pages, but it's fine
        });

        me.To(206).CheckByKeysReverse(13, 3, 1, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2}},
            {TGroupId{2}, {9_g, 8, 7_f}} // here we touch extra pages, but it's fine
        });
    }

    Y_UNIT_TEST(ByKeysHistory)
    {
        TModel me(true, true);

        me.To(100).CheckByKeys(9, 23, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {2, 3, 4, 5, 6}},
            {TGroupId{0, true}, {1_g, 2, 3, 4, 5_g}},
            {TGroupId{1}, {3_g, 4, 5, 6, 7, 8_g}},
            {TGroupId{1, true}, {3_g, 4, 5, 6_g, 7_g, 8_g}},
            {TGroupId{2}, {6_g, 7_g, 8_g, 9, 10, 11, 12, 13, 14, 15_g, 16_g, 17_g}},
            {TGroupId{2, true}, {6_g, 7_g, 8_g, 9, 10, 11, 12_g, 13_g, 14_g, 15_g, 16_g, 17_g}}
        });

        me.To(200).CheckByKeys(29, 35, 0, TMap<TGroupId, TArr>{
            {TGroupId{2, true}, {21_g, 22_g, 23_g, 24_g, 25_g, 26_g}}
        });

        me.To(201).CheckByKeys(29, 34, 0, TMap<TGroupId, TArr>{
            {TGroupId{2, true}, {21_g, 22_g, 23_g, 24_g, 25_g}}
        });
    }

    Y_UNIT_TEST(ByKeysIndex)
    {
        { // index
            TModel me(false, false);
            auto &pages = me.Eggs.Lone()->IndexPages;

            // no index => touch index
            me.To(100).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {pages.FlatGroups[0]}},
            }, TSet<TPageId> {

            });

            // index => touch pages + index
            me.To(101).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0]}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0]
            });
        }

        { // index + history index
            TModel me(false, true);
            auto &pages = me.Eggs.Lone()->IndexPages;

            // no index => touch index
            me.To(200).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {pages.FlatGroups[0]}},
                {TGroupId{0, true}, {}}
            }, TSet<TPageId> {

            });

            // no history index => touch main pages + index + history index
            me.To(201).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], pages.FlatHistoric[0]}},
                {TGroupId{0, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0]
            });

            // history index => touch main pages + history pages + index + history index
            me.To(202).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], pages.FlatHistoric[0]}},
                {TGroupId{0, true}, {1, 2, 3, 4}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatHistoric[0]
            });
        }
        
        { // index + groups
            TModel me(true, false);
            auto &pages = me.Eggs.Lone()->IndexPages;

            // no index => touch index
            me.To(300).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {pages.FlatGroups[0]}},
                {TGroupId{1}, {}}
            }, TSet<TPageId> {

            });

            // no groups index => touch main pages + index + all groups indexes
            me.To(301).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{1}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0]
            });

            // groups index => touch all pages + index + all groups indexes
            me.To(302).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{1}, {3, 4, 5, 6, 7}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatGroups[1]
            });
        }

        { // index + groups + history
            TModel me(true, true);
            auto &pages = me.Eggs.Lone()->IndexPages;

            // no index => touch index
            me.To(400).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {pages.FlatGroups[0]}},
                {TGroupId{0, true}, {}},
                {TGroupId{1}, {}},
                {TGroupId{1, true}, {}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            }, TSet<TPageId> {

            });

            // only index => touch main pages + index + all groups indexes + history index
            me.To(401).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], pages.FlatHistoric[0], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {}},
                {TGroupId{1}, {}},
                {TGroupId{1, true}, {}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0]
            });

            // history index => touch main pages + index + all groups indexes + main history pages
            me.To(402).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], 
                    pages.FlatHistoric[0], pages.FlatHistoric[1], pages.FlatHistoric[2], pages.FlatHistoric[3], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {1, 2, 3, 4}},
                {TGroupId{1}, {}},
                {TGroupId{1, true}, {}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatHistoric[0]
            });

            // main history and history => touch main pages + index + all groups indexes + history pages + history groups pages
            me.To(403).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], 
                    pages.FlatHistoric[0], pages.FlatHistoric[1], pages.FlatHistoric[2], pages.FlatHistoric[3], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {1, 2, 3, 4}},
                {TGroupId{1}, {}},
                {TGroupId{1, true}, {3, 4, 5}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatHistoric[0], pages.FlatHistoric[1] 
            });

            // groups index => touch main pages + index + history index + all groups indexes + groups pages
            me.To(404).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], 
                    pages.FlatHistoric[0], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {}},
                {TGroupId{1}, {3, 4, 5, 6, 7}},
                {TGroupId{1, true}, {}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatGroups[1]
            });

            // main history and groups => touch main pages + index + all groups indexes + groups pages + history main pages
            me.To(405).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], 
                    pages.FlatHistoric[0], pages.FlatHistoric[1], pages.FlatHistoric[2], pages.FlatHistoric[3], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {1, 2, 3, 4}},
                {TGroupId{1}, {3, 4, 5, 6, 7}},
                {TGroupId{1, true}, {}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatHistoric[0], pages.FlatGroups[1]
            });

            // all indexes
            me.To(406).CheckIndex(6, 22, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {1, 2, 3, 4, 5, 6, pages.FlatGroups[0], 
                    pages.FlatHistoric[0], pages.FlatHistoric[1], pages.FlatHistoric[2], pages.FlatHistoric[3], pages.FlatGroups[1], pages.FlatGroups[2], pages.FlatGroups[3]}},
                {TGroupId{0, true}, {1, 2, 3, 4}},
                {TGroupId{1}, {3, 4, 5, 6, 7}},
                {TGroupId{1, true}, {3, 4, 5}},
                {TGroupId{2}, {}},
                {TGroupId{2, true}, {}}
            },
            TSet<TPageId> {
                pages.FlatGroups[0], pages.FlatHistoric[0], pages.FlatHistoric[1], pages.FlatGroups[1]
            });
        }
    }

    Y_UNIT_TEST(ByRows)
    {
        TModel me(true);

        /*
        
        row ids by pages:
        group0 = |0 1 2|3 4 5|6 7 8|..
        group1 = |0 1|2 3|4 5|6 7|8 ..
        group2 = |0|1|2|3|4|5|6|7|8|..

        */

        me.To(100).CheckByRows(0, 0, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{1}, {0}},
            {TGroupId{2}, {0}}
        });

        me.To(101).CheckByRows(0, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{1}, {0}},
            {TGroupId{2}, {0, 1}}
        });

        me.To(102).CheckByRows(0, 2, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{1}, {0, 1}},
            {TGroupId{2}, {0, 1, 2}}
        });

        me.To(103).CheckByRows(0, 3, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1}},
            {TGroupId{1}, {0, 1}},
            {TGroupId{2}, {0, 1, 2, 3}}
        });

        me.To(104).CheckByRows(0, 4, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1}},
            {TGroupId{1}, {0, 1, 2}},
            {TGroupId{2}, {0, 1, 2, 3, 4}}
        });

        me.To(105).CheckByRows(1, 7, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {0, 1, 2, 3}},
            {TGroupId{2}, {1, 2, 3, 4, 5, 6, 7}}
        });

        me.To(106).CheckByRows(2, 6, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {1, 2, 3}},
            {TGroupId{2}, {2, 3, 4, 5, 6}}
        });

        me.To(107).CheckByRows(3, 5, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1}},
            {TGroupId{1}, {1, 2}},
            {TGroupId{2}, {3, 4, 5}}
        });

        me.To(200).CheckByRows(8, 7, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {2}},
            {TGroupId{1}, {}},
            {TGroupId{2}, {}}
        });

        me.To(201).CheckByRows(7, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {2}},
            {TGroupId{1}, {}},
            {TGroupId{2}, {}}
        });
    }

    Y_UNIT_TEST(ByRowsReverse)
    {
        TModel me(true);

        /*
        
        row ids by pages:
        group0 = ..|18 19 20|21 22 23|24 25 26|
        group1 = ..|18 19|20 21|22 23|24 25|26|
        group2 = ..|18|19|20|21|22|23|24|25|26|

        */

        me.To(100).CheckByRowsReverse(26, 26, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{1}, {13}},
            {TGroupId{2}, {26}}
        });

        me.To(101).CheckByRowsReverse(26, 25, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{1}, {13, 12}},
            {TGroupId{2}, {26, 25}}
        });

        me.To(102).CheckByRowsReverse(26, 24, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{1}, {13, 12}},
            {TGroupId{2}, {26, 25, 24}}
        });

        me.To(103).CheckByRowsReverse(26, 23, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{1}, {13, 12, 11}},
            {TGroupId{2}, {26, 25, 24, 23}}
        });

        me.To(104).CheckByRowsReverse(26, 22, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{1}, {13, 12, 11}},
            {TGroupId{2}, {26, 25, 24, 23, 22}}
        });

        me.To(105).CheckByRowsReverse(25, 19, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7, 6}},
            {TGroupId{1}, {12, 11, 10, 9}},
            {TGroupId{2}, {25, 24, 23, 22, 21, 20, 19}}
        });

        me.To(106).CheckByRowsReverse(24, 20, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7, 6}},
            {TGroupId{1}, {12, 11, 10}},
            {TGroupId{2}, {24, 23, 22, 21, 20}}
        });

        me.To(107).CheckByRowsReverse(23, 21, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {7}},
            {TGroupId{1}, {11, 10}},
            {TGroupId{2}, {23, 22, 21}}
        });

        me.To(200).CheckByRowsReverse(8, 9, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {2}},
            {TGroupId{1}, {}},
            {TGroupId{2}, {}}
        });

        me.To(201).CheckByRowsReverse(9, 15, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3}},
            {TGroupId{1}, {}},
            {TGroupId{2}, {}}
        });

        me.To(202).CheckByRowsReverse(100, 23, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{1}, {13, 12, 11}},
            {TGroupId{2}, {26, 25, 24, 23}}
        });
    }

    Y_UNIT_TEST(ByRowsLimits)
    {
        TModel me(true);

        /*
        
        row ids by pages:
        group0 = |0 1 2|3 4 5|6 7 8|..
        group1 = |0 1|2 3|4 5|6 7|8 ..
        group2 = |0|1|2|3|4|5|6|7|8|..

        */

        // Precharge touches [row1 .. Min(row2, row1 + itemsLimit)] rows (1 extra row)
        
        me.To(100).CheckByRows(1, 7, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {0, 1, 2, 3}},
            {TGroupId{2}, {1, 2, 3, 4, 5, 6, 7}}
        });

        me.To(101).CheckByRows(1, 7, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {0, 1, 2, 3}},
            {TGroupId{2}, {1, 2, 3, 4, 5, 6}}
        });

        me.To(102).CheckByRows(1, 7, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1}},
            {TGroupId{1}, {0, 1, 2}},
            {TGroupId{2}, {1, 2, 3, 4, 5}}
        });

        me.To(103).CheckByRows(1, 7, 1, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{1}, {0, 1}},
            {TGroupId{2}, {1, 2}}
        });

        me.To(104).CheckByRows(3, 20, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2}},
            {TGroupId{1}, {1, 2, 3, 4}},
            {TGroupId{2}, {3, 4, 5, 6, 7, 8}}
        });
    }

    Y_UNIT_TEST(ByRowsLimitsReverse)
    {
        TModel me(true);

        /*
        
        row ids by pages:
        group0 = ..|18 19 20|21 22 23|24 25 26|
        group1 = ..|18 19|20 21|22 23|24 25|26|
        group2 = ..|18|19|20|21|22|23|24|25|26|

        */

        // Precharge touches [Max(row2, row1 - itemsLimit) .. row1] rows (1 extra row)
        
        me.To(100).CheckByRowsReverse(25, 19, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7, 6}},
            {TGroupId{1}, {12, 11, 10, 9}},
            {TGroupId{2}, {25, 24, 23, 22, 21, 20, 19}}
        });

        me.To(101).CheckByRowsReverse(25, 19, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7, 6}},
            {TGroupId{1}, {12, 11, 10}},
            {TGroupId{2}, {25, 24, 23, 22, 21, 20}}
        });

        me.To(102).CheckByRowsReverse(25, 19, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{1}, {12, 11, 10}},
            {TGroupId{2}, {25, 24, 23, 22, 21}}
        });

        me.To(103).CheckByRowsReverse(25, 19, 1, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{1}, {12}},
            {TGroupId{2}, {25, 24}}
        });

        me.To(104).CheckByRowsReverse(23, 3, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {7, 6}},
            {TGroupId{1}, {11, 10, 9}},
            {TGroupId{2}, {23, 22, 21, 20, 19, 18}}
        });

        me.To(200).CheckByRowsReverse(100, 3, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{1}, {13, 12, 11, 10}},
            {TGroupId{2}, {26, 25, 24, 23, 22, 21}}
        });
    }
}

}
}
