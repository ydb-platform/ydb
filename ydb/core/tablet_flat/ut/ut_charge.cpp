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
    const NTest::TMass Mass(new NTest::TModelStd(true), 4 * 9);

    enum TPageIdFlags {
        IfIter = 1,
        IfFail = 2,
        IfNoFail = 4
    };
    struct TFlaggedPageId {
        TPageId Page;
        TPageIdFlags Flags = static_cast<TPageIdFlags>(TPageIdFlags::IfIter | TPageIdFlags::IfFail | TPageIdFlags::IfNoFail);

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
        TTouchEnv(bool fail) : Fail(fail) { }

        const TSharedData* TryGetPage(const TPart *part, TPageId id, TGroupId groupId) override
        {
            Touched[groupId].insert(id);
            return Fail ? nullptr : NTest::TTestEnv::TryGetPage(part, id, groupId);
        }

        const bool Fail = false;
        TMap<TGroupId, TSet<TPageId>> Touched;
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
        using TArr = std::initializer_list<TFlaggedPageId>;
        using TGroupId = NPage::TGroupId;

        TModel()
            : Tool(*Mass.Model->Scheme)
        {
            auto pages = Eggs.At(0)->Index->End() - Eggs.At(0)->Index->Begin();

            UNIT_ASSERT(pages == 9);
        }

        static NTest::TPartEggs MakeEggs() noexcept
        {
            NPage::TConf conf{ true, 8192 };

            auto groups = Mass.Model->Scheme->Families.size();
            for (size_t group : xrange(groups)) {
                conf.Group(group).PageRows = 3; /* each main page has 3 physical rows, but... */
            }
            conf.Group(1).PageRows = 2;
            conf.Group(2).PageRows = 1;

            NTest::TPartCook cook(Mass.Model->Scheme, conf);

            for (auto seq: xrange(Mass.Saved.Size())) {
                /* ... but rows pack has 4 rows per each page, the first row in
                    each pack is ommited and used as spacer between pages. */

                if (seq % 4 > 0) cook.Add(Mass.Saved[seq]);
            }

            return cook.Finish();
        }

        void CheckByKeys(ui32 lower, ui32 upper, ui64 items, TArr shouldPrecharge) const
        {
            CheckByKeys(lower, upper, items, {{TGroupId{}, shouldPrecharge}});
        }

        void CheckByKeys(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByKeys(lower, upper, items, false, shouldPrecharge, false);
            CheckPrechargeByKeys(lower, upper, items, true, shouldPrecharge, false);
            CheckIterByKeys(lower, upper, items ? items : Max<ui32>(), shouldPrecharge);
        }

        void CheckByKeysReverse(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& shouldPrecharge) const
        {
            CheckPrechargeByKeys(lower, upper, items, false, shouldPrecharge, true);
            CheckPrechargeByKeys(lower, upper, items, true, shouldPrecharge, true);
        }

        void CheckByRows(TPageId row1, TPageId row2, ui64 items, TMap<TGroupId, TArr> shouldPrecharge) const
        {
            CheckPrechargeByRows(row1, row2, items, false, shouldPrecharge);
            CheckPrechargeByRows(row1, row2, items, true, shouldPrecharge);
        }

        void CheckPrechargeByKeys(ui32 lower, ui32 upper, ui64 items, bool fail, const TMap<TGroupId, TArr>& shouldPrecharge, bool reverse) const
        {
            Y_VERIFY(lower < Mass.Saved.Size() && upper < Mass.Saved.Size());

            TTouchEnv env(fail);

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
                ? TCharge::Range(&env, from, to, run, keyDefaults, tags, items, Max<ui64>())
                : TCharge::RangeReverse(&env, from, to, run, keyDefaults, tags, items, Max<ui64>());

            UNIT_ASSERT_VALUES_EQUAL_C(!fail || env.Touched.empty(), ready, AssertMesage(fail));

            AssertEqual(env.Touched, shouldPrecharge, fail ? TPageIdFlags::IfFail : TPageIdFlags::IfNoFail);
        }

        void CheckPrechargeByRows(TPageId row1, TPageId row2, ui64 items, bool fail, TMap<TGroupId, TArr> shouldPrecharge) const
        {
            Y_VERIFY(row1 <= row2 && row2 < 3 * 9);

            TTouchEnv env(fail);

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

            UNIT_ASSERT_VALUES_EQUAL_C(
                !fail,
                TCharge(&env, *run.begin()->Part, tags, false).Do(row1, row2, keyDefaults, items, Max<ui64>()),
                AssertMesage(fail));

            AssertEqual(env.Touched, shouldPrecharge, fail ? TPageIdFlags::IfFail : TPageIdFlags::IfNoFail);
        }

        void CheckIterByKeys(ui32 lower, ui32 upper, ui64 items, const TMap<TGroupId, TArr>& precharged) const
        {
            Y_VERIFY(lower < Mass.Saved.Size() && upper < Mass.Saved.Size());

            NTest::TCheckIt wrap(Eggs, { new TTouchEnv(false) });

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

                // forcebly touch the next stop element that is greater than upper
                // because instead of having |1 2 3| and stopping as soon as we see 2
                // we may have |1*2 2*2 3*2| = |2 4 6| and be requested with upper = 5 (not 4)
                if (key > upper) {
                    break;
                }
            }

            auto env = wrap.Displace<TTouchEnv>(nullptr);

            AssertEqual(env->Touched, precharged, TPageIdFlags::IfIter);
        }

        const NTest::TRowTool Tool;
        const NTest::TPartEggs Eggs = MakeEggs();

    private:
        void AssertEqual(const TMap<TGroupId, TSet<TPageId>>& actual, const TMap<TGroupId, TArr>& expected, TPageIdFlags flags) const {
            for (auto [groupId, arr] : expected) {
                auto actualValue = actual.Value(groupId, TSet<TPageId>());
                auto expectedValue = TSet<TPageId>{};
                for (auto p  : arr) {
                    if (flags & p.Flags) {
                        expectedValue.insert(p.Page);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(expectedValue, actualValue, AssertMesage(groupId, flags));
            }
        }

        std::string AssertMesage(bool fail) const {
            return 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Fail: " + (fail ? "Yes" : "No");
        }
        std::string AssertMesage(TGroupId group) const {
            return 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Group: " + std::to_string(group.Index);
        }
        std::string AssertMesage(TGroupId group, TPageIdFlags flags) const {
            auto result = 
                "Seq: " + std::to_string(CurrentStep()) + 
                " Group: " + std::to_string(group.Index);

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
        TModel me;

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

            me.To(101).CheckByKeys(0, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {0_g, 1, 2, 3_g}},
                {TGroupId{2}, {0_g, 1_g, 2_g, 3, 4, 5, 6_g}}
            });

            me.To(102).CheckByKeys(1, 9, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {0, 1, 2, 3_I}},
                {TGroupId{1}, {0_g, 1, 2, 3_g}},
                {TGroupId{2}, {0_g, 1_g, 2_g, 3, 4, 5, 6_g}}
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
                {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always neded
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
                {TGroupId{1}, {2_g, 3, 4}}, // pages 3, 4 are always neded
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
                {TGroupId{1}, {2_g, 3, 4}}, // pages 3, 4 are always neded
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
                {TGroupId{1}, {3, 4}}, // pages 3, 4 are always neded
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

            me.To(301).CheckByKeys(27, 35, 0, TMap<TGroupId, TArr>{
                {TGroupId{0}, {6, 7, 8}},
                {TGroupId{1}, {10, 11, 12_g, 13_g}},
                {TGroupId{2}, {20_g, 21, 22, 23, 24_g, 25_g, 26_g}}
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
        TModel me;

        /*
        
        keys by pages:
        group0 = |1 2 3|5 6 7|9 10 11|13 14 15|..
        group1 = |1 2|3 5|6 7|9 10|11 13|14 15|..
        group3 = |1|2|3|5|6|7|9|10|11|13|14|15|..
        
        */

        me.To(101).CheckByKeys(5, 13, 999, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_I}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always neded
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
        });

        me.To(102).CheckByKeys(5, 13, 7, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_I}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}}, // pages 3, 4 are always neded
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}} // pages 6, 7, 8 are always needed
        });

        me.To(103).CheckByKeys(5, 13, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3, 4_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}},
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8, 9_g}}
        });

        me.To(104).CheckByKeys(5, 13, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f, 4_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4}},
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8}}
        });

        me.To(105).CheckByKeys(5, 13, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f, 4_f}},
            {TGroupId{1}, {1_g, 2_g, 3, 4_f}}, // here we touh extra pages, but it's fine
            {TGroupId{2}, {3_g, 4_g, 5_g, 6, 7, 8_f}} // here we touh extra pages, but it's fine
        });

        me.To(106).CheckByKeys(7, 13, 3, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2, 3_f, 4_f}},
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

        me.To(101).CheckByKeys(0, 35, 8 /* rows */, { 0, 1, 2, 3_f });
        me.To(102).CheckByKeys(0, 35, 11 /* rows */, { 0, 1, 2, 3, 4_f });
        me.To(103).CheckByKeys(0, 35, 14 /* rows */, { 0, 1, 2, 3, 4, 5_f });
        me.To(104).CheckByKeys(3, 35, 5 /* rows */, { 0, 1, 2 });
        me.To(105).CheckByKeys(3, 35, 6 /* rows */, { 0, 1, 2, 3_f });
        me.To(106).CheckByKeys(4, 35, 6 /* rows */, { 0, 1, 2, 3 });
        me.To(107).CheckByKeys(5, 35, 5 /* rows */, { 1, 2, 3_f });
        me.To(112).CheckByKeys(9, 35, 11 /* rows */, { 2, 3, 4, 5, 6_f });
        me.To(113).CheckByKeys(9, 35, 14 /* rows */, { 2, 3, 4, 5, 6, 7_f });
        me.To(120).CheckByKeys(25, 35, 8 /* rows */, { 6, 7, 8 });


        /*_ 2xx: one row charge limit on two page */

        for (const ui16 page : xrange(4)) {
            const TArr span1{ page, operator""_f(page + 1) };
            const TArr span2{ page, page + 1 };

            const ui32 base = 200 + page * 10, lead = page * 4;

            me.To(base + 1).CheckByKeys(lead + 1, 35, 1, span1);
            me.To(base + 2).CheckByKeys(lead + 2, 35, 1, span1);
            me.To(base + 3).CheckByKeys(lead + 3, 35, 1, span2);
        }
    }

    Y_UNIT_TEST(ByKeysReverse)
    {
        TModel me;

        /*
        
        keys by pages:
        group0 = |1 2 3|5 6 7|9 10 11|13 14 15|..
        group1 = |1 2|3 5|6 7|9 10|11 13|14 15|..
        group3 = |1|2|3|5|6|7|9|10|11|13|14|15|..
        
        */

        me.To(100).CheckByKeysReverse(15, 15, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3}},
            {TGroupId{2}, {11_g}}
        });

        me.To(101).CheckByKeysReverse(15, 5, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1}},
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

        me.To(104).CheckByKeysReverse(15, 1, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3, 2_g, 1_g, 0_g}}
        });

        me.To(105).CheckByKeysReverse(15, 0, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1, 0}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4, 3, 2, 1, 0}}
        });

        me.To(106).CheckByKeysReverse(17, 17, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {4}},
            {TGroupId{2}, {12_g}}
        });
        
        me.To(107).CheckByKeysReverse(16, 16, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3}},
            {TGroupId{2}, {}}
        });

        me.To(108).CheckByKeysReverse(35, 35, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{2}, {26_g}}
        });

        me.To(109).CheckByKeysReverse(35, 33, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8}},
            {TGroupId{2}, {26_g, 25_g, 24_g}}
        });

        me.To(110).CheckByKeysReverse(35, 32, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {8, 7}},
            {TGroupId{2}, {26_g, 25_g, 24_g}}
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
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5, 4_f, 3_f}} // here we touh extra pages, but it's fine
        });

        me.To(201).CheckByKeysReverse(15, 3, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6, 5_f, 4_f, 3_f}} // here we touh extra pages, but it's fine
        });

        me.To(202).CheckByKeysReverse(15, 5, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {11_g, 10_g, 9_g, 8, 7, 6}}
        });

        me.To(203).CheckByKeysReverse(13, 3, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1}},
            {TGroupId{2}, {9_g, 8, 7, 6, 5, 4_f}} // here we touh extra pages, but it's fine
        });

        me.To(204).CheckByKeysReverse(13, 3, 3, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2, 1_f}},
            {TGroupId{2}, {9_g, 8, 7, 6, 5_f}} // here we touh extra pages, but it's fine
        });

        me.To(205).CheckByKeysReverse(13, 3, 2, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2}},
            {TGroupId{2}, {9_g, 8, 7, 6_f}} // here we touh extra pages, but it's fine
        });

        me.To(206).CheckByKeysReverse(13, 3, 1, TMap<TGroupId, TArr>{
            {TGroupId{0}, {3, 2}},
            {TGroupId{2}, {9_g, 8, 7_f}} // here we touh extra pages, but it's fine
        });
    }

    Y_UNIT_TEST(ByRows)
    {
        TModel me;

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

        me.To(107).CheckByRows(4, 8, 0, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2}},
            {TGroupId{1}, {2, 3, 4}},
            {TGroupId{2}, {4, 5, 6, 7, 8}}
        });
    }

    Y_UNIT_TEST(ByRowsLimits)
    {
        TModel me;

        /*
        
        row ids by pages:
        group0 = |0 1 2|3 4 5|6 7 8|..
        group1 = |0 1|2 3|4 5|6 7|8 ..
        group2 = |0|1|2|3|4|5|6|7|8|..

        */

        // Precharge touches [row1 .. Min(row2, row1 + itemsLimit)] rows (1 extra row)
        
        me.To(101).CheckByRows(1, 7, 6, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {0, 1, 2, 3}},
            {TGroupId{2}, {1, 2, 3, 4, 5, 6, 7}}
        });

        me.To(102).CheckByRows(1, 7, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1, 2}},
            {TGroupId{1}, {0, 1, 2, 3}},
            {TGroupId{2}, {1, 2, 3, 4, 5, 6}}
        });

        me.To(103).CheckByRows(1, 7, 4, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0, 1}},
            {TGroupId{1}, {0, 1, 2}},
            {TGroupId{2}, {1, 2, 3, 4, 5}}
        });

        me.To(104).CheckByRows(1, 7, 1, TMap<TGroupId, TArr>{
            {TGroupId{0}, {0}},
            {TGroupId{1}, {0, 1}},
            {TGroupId{2}, {1, 2}}
        });

        me.To(105).CheckByRows(3, 20, 5, TMap<TGroupId, TArr>{
            {TGroupId{0}, {1, 2}},
            {TGroupId{1}, {1, 2, 3, 4}},
            {TGroupId{2}, {3, 4, 5, 6, 7, 8}}
        });
    }
}

}
}
