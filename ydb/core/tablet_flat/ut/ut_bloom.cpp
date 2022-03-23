#include <ydb/core/tablet_flat/flat_bloom_writer.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/model/keys.h>
#include <ydb/core/tablet_flat/test/libs/table/model/small.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(Bloom) {
    using namespace NTest;

    static const NTest::TMass Mass0(new NTest::TModelS3Hash, 24000, 42, 0.5);

    struct TCooker {
        static TIntrusiveConstPtr<NPage::TBloom> Make(const TMass &mass, float rate)
        {
            TCooker cooker(*mass.Model->Scheme, mass.Saved.Size(), rate);

            return cooker.Add(mass.Saved).Flush();
        }

        TCooker(const TRowScheme &scheme, ui64 rows, float rate)
            : Tool(scheme)
            , Writer(rows, rate)
        {

        }

        TCooker& Add(const TRowsHeap &rows)
        {
            for (const auto &one : rows) Add(one);

            return *this;
        }

        TCooker& Add(const NTest::TRow &row)
        {
            const auto key = Tool.KeyCells(row);

            return Writer.Add(key), *this;
        }

        TIntrusiveConstPtr<NPage::TBloom> Flush() noexcept
        {
            return new NPage::TBloom(Writer.Make());
        }

     private:
        const NTest::TRowTool Tool;
        NBloom::TWriter Writer;
    };

    struct TRater {
        TRater(const TRowScheme &scheme, TIntrusiveConstPtr<NPage::TBloom> page)
            : Tool(scheme)
            , Page(std::move(page))
        {

        }

        double Do(const TMass &mass) const
        {
            UNIT_ASSERT_C(Filter(mass.Saved) == 0, "Saved rows weeded");

            const auto size = mass.Holes.Size();

            return size ? double(size - Filter(mass.Holes)) / size : 0.;
        }

        ui64 Filter(const TRowsHeap &rows) const
        {
            ui64 filtered = 0;

            for (const auto &one: rows) {
                const auto key = Tool.KeyCells(one);
                const NBloom::TPrefix prefix(key);

                filtered += Page->MightHave(prefix.Get(key.size())) ? 0 : 1;
            }

            return filtered;
        }

    public:
        const NTest::TRowTool Tool;
        const TIntrusiveConstPtr<NPage::TBloom> Page;
    };

    static TAlter MakeAlter(const ui32 table = 1)
    {
        TAlter alter;

        alter
            .AddTable("me", table)
            .AddColumn(1, "name", 1, ETypes::String, false)
            .AddColumn(1, "salt", 2, ETypes::Uint64, false)
            .AddColumnToKey(1, 1)
            .AddColumnToKey(1, 2);

        return alter;
    }

    Y_UNIT_TEST(Conf)
    {
        NBloom::TEstimator estimator(0.001);
        UNIT_ASSERT(estimator.Hashes() > 0);
        UNIT_ASSERT(estimator.Bits(0) > 0);
    }

    Y_UNIT_TEST(Hashes)
    {
        const NTest::TRowTool tool(*Mass0.Model->Scheme);

        ui64 salt = 0;

        for (const auto &one: Mass0.Saved) {
            const auto key = tool.KeyCells(one);
            const NBloom::TPrefix raw(key);

            ui64 val = NBloom::THash(raw.Get(key.size())).Next();

            salt = MurmurHash<ui64>((void*)&val, sizeof(val), salt);
        }

        UNIT_ASSERT_C(0x3841f2f07c20fc07 == salt, "Hash is broken");
    }

    Y_UNIT_TEST(Rater)
    {
        const auto filter = TCooker::Make(Mass0, 0.001);

        auto rate = TRater(*Mass0.Model->Scheme, filter).Do(Mass0);

        UNIT_ASSERT_C(rate < 0.001 * 1.3, "Bloom filter error " << rate);
    }

    Y_UNIT_TEST(Dipping)
    {
        double edge = 0.13, error = 0.1;

        for (auto num : xrange(4) ) {
            const auto filter = TCooker::Make(Mass0, error);

            auto rate = TRater(*Mass0.Model->Scheme, filter).Do(Mass0);

            UNIT_ASSERT_C(rate < edge, "on " << num << " error " << rate);

            edge = rate * 0.13, error *= 0.1;
        }
    }

    Y_UNIT_TEST(Basics)
    {
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        const auto ru1 = *me.SchemedCookRow(1).Col("ba0_huxu", 01234_u64);
        const auto rw1 = *me.SchemedCookRow(1).Col("ba2_d7mj", 12340_u64);
        const auto ru2 = *me.SchemedCookRow(1).Col("ba4_tfcr", 23401_u64);
        const auto rw2 = *me.SchemedCookRow(1).Col("ba6_fdy4", 34012_u64);
        const auto rw3 = *me.SchemedCookRow(1).Col("ba8_digr", 40123_u64);

        /* These keys are fitted to be filtered in all parts with
            ByKey Bloom filter enabled, thus in Pw and Pz parts. */

        const auto no1 = *me.SchemedCookRow(1).Col("ba1_p9lw", 53543_u64);
        const auto no2 = *me.SchemedCookRow(1).Col("ba3_g0ny", 53442_u64);
        const auto no3 = *me.SchemedCookRow(1).Col("ba5_3hpx", 50894_u64);

        /*_ 10: Filter shouldn't be used when it is not enabled, Pu */

        me.To(11).Begin().Put(1, ru1, ru2).Commit().Snap(1).Compact(1);
        me.To(12).Select(1).Has(ru1, ru2).NoKey(no1, no2, no3);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && !subset->Flatten[0]->ByKey);

            auto &stats = me.Relax().BackLog().Stats;

            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectSieved, 4); /* ba? keys */
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectNoKey, 2); /* no? keys */
        }

        /*_ 20: Make next part Pw with enabled by key bloom filter */

        me.To(20).Begin().Apply(*TAlter().SetByKeyFilter(1, true));
        me.To(21).Put(1, rw1, rw2, rw3).Commit().Snap(1).Compact(1, false);
        me.To(22).Select(1).Has(ru1, ru2, rw1, rw2, rw3).NoKey(no1, no2, no3);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(
                subset->Flatten.at(0)->ByKey || subset->Flatten.at(1)->ByKey);

            auto &stats = me.Relax().BackLog().Stats;

            /* { Weeded 3 } is { Pw : ru no } - { no3, no1 are out of range },
              { Sieved 7 } is { Pu : ru rw no } + { Pw : rw } - ...,
              { NoKey 2 } is { no1, no2 } because no3 is out of range */

            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectSieved, 7);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectNoKey, 2);
        }

        /*_ 30: Recompact all parts to one Pz and repeat Select()'s */

        me.To(30).Compact(1, true /* final compaction */);
        me.To(31).Select(1).Has(ru1, ru2, rw1, rw2, rw3).NoKey(no1, no2, no3);

        {
            const auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && subset->Flatten[0]->ByKey);

            auto &stats = me.Relax().BackLog().Stats;

            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectSieved, 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectNoKey, 0);

            DumpPart(static_cast<const TPartStore&>(*subset->Flatten[0]), 10);
        }

        /*_ 40: Disable filter then check that is really gone */

        me.To(40).Begin().Apply(*TAlter().SetByKeyFilter(1, false));
        me.To(41).Commit().Compact(1, true /* final compaction */);

        {
            const auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && !subset->Flatten[0]->ByKey);
        }
    }

    Y_UNIT_TEST(Stairs)
    {
        const ui32 Height = 99;

        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        const TIntrusivePtr<TGrowHeap> heap = new TGrowHeap(128 * 1024);

        TDeque<NTest::TRow> ladder;

        /* 1xxx: Add rows and parts extending key by one col each time */

        for (auto col : xrange(ui32(3), Height + 3)) {
            TAlter alter;

            auto name = Sprintf("sub_%04u", col);
            alter.AddColumn(1, name, col, ETypes::String, false);
            alter.AddColumnToKey(1, col);
            alter.SetByKeyFilter(1, col > 10); /* first 8 parts w/o filter */

            me.To(col * 10 + 1001).Begin().Apply(*alter).Commit();

            TRow &row = (ladder.emplace_back(heap), ladder.back());

            row.Do(1, "foo").Do(2, 77_u64);

            for (auto sub: xrange(ui32(3), col + 1))
                row.Do(sub, Sprintf("%x_%x", col, sub));

            me.To(col * 10 + 1002).Begin().Put(1, row).Commit().Snap(1);
            me.To(col * 10 + 1003).Compact(1, false).Select(1).Has(row);

            const ui32 limit = 1 + (Min(ui32(10), col) - 2) + 5 /* error */;

            UNIT_ASSERT(me.Relax().BackLog().Stats.SelectSieved < limit);
        }

        {
            const auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT_VALUES_EQUAL(subset->Flatten.size(), Height);
            UNIT_ASSERT_VALUES_EQUAL(subset->Frozen.size(), 0);
        }

        /* 8xxx: Extend incomplete keys with nulls and check it precense */

        for (auto &row : ladder) {
            const ui32 serial = 8000 + (*row).size();

            for (ui32 grow = (*row).size(); grow++ < Height + 2; )
                row.Do(grow, nullptr);

            me.To(serial).Begin().Select(1).Has(row);

            const ui32 limit = 1 + 8 /* w/o filter */ + 5 /* error */;

            UNIT_ASSERT(me.Commit().BackLog().Stats.SelectSieved < limit);
        }
    }
}

}
}
