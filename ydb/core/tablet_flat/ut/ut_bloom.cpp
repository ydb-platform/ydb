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

        TIntrusiveConstPtr<NPage::TBloom> Flush()
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
            .AddColumn(1, "name", 1, ETypes::String, false, false)
            .AddColumn(1, "salt", 2, ETypes::Uint64, false, false)
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

            UNIT_ASSERT(subset->Flatten.size() == 1 && subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;

            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectSieved, 4); /* ba? keys */
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectNoKey, 2); /* no? keys */
        }

        /*_ 20: Make next part Pw with enabled by key bloom filter */

        me.To(20).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2}));
        me.To(21).Put(1, rw1, rw2, rw3).Commit().Snap(1).Compact(1, false);
        me.To(22).Select(1).Has(ru1, ru2, rw1, rw2, rw3).NoKey(no1, no2, no3);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(
                !subset->Flatten.at(0)->ByKeyPrefixes.empty() || !subset->Flatten.at(1)->ByKeyPrefixes.empty());

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

            UNIT_ASSERT(subset->Flatten.size() == 1 && !subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;

            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectSieved, 5);
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectNoKey, 0);

            DumpPart(static_cast<const TPartStore&>(*subset->Flatten[0]), 10);
        }

        /*_ 40: Disable filter then check that is really gone */

        me.To(40).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {}));
        me.To(41).Commit().Compact(1, true /* final compaction */);

        {
            const auto subset = me->Subset(1, TEpoch::Max(), { }, { });

            UNIT_ASSERT(subset->Flatten.size() == 1 && subset->Flatten[0]->ByKeyPrefixes.empty());
        }
    }

    Y_UNIT_TEST(PrefixBloom)
    {
        TDbExec me;

        /* Table with 3-column PK: (name: String, salt: Uint64, sub: String) */
        {
            TAlter alter;
            alter
                .AddTable("me", 1)
                .AddColumn(1, "name", 1, ETypes::String, false, false)
                .AddColumn(1, "salt", 2, ETypes::Uint64, false, false)
                .AddColumn(1, "sub",  3, ETypes::String, false, false)
                .AddColumnToKey(1, 1)
                .AddColumnToKey(1, 2)
                .AddColumnToKey(1, 3);

            me.To(10).Begin().Apply(*alter).Commit();
        }

        /* Rows that share the same first column "aaa" */
        const auto rw1 = *me.SchemedCookRow(1).Col("aaa", 100_u64, "x1");
        const auto rw2 = *me.SchemedCookRow(1).Col("aaa", 200_u64, "x2");

        /* Rows with a different first column "bbb" */
        const auto rw3 = *me.SchemedCookRow(1).Col("bbb", 300_u64, "x3");

        /* Non-existing keys: same first column "aaa" but different rest
           (within range of part, so bloom is checked but passes) */
        const auto noSame = *me.SchemedCookRow(1).Col("aaa", 999_u64, "zz");
        /* Non-existing keys: different first column but within range [aaa..bbb]
           (within range of part, so bloom is checked and rejects) */
        const auto noDiff1 = *me.SchemedCookRow(1).Col("aab", 100_u64, "x1");
        const auto noDiff2 = *me.SchemedCookRow(1).Col("aac", 200_u64, "x2");

        /*_ 10: Enable prefix bloom on first PK column only */

        me.To(11).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {1}));
        me.To(12).Put(1, rw1, rw2, rw3).Commit().Snap(1).Compact(1);
        me.To(13).Select(1).Has(rw1, rw2, rw3).NoKey(noSame, noDiff1, noDiff2);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes[0].first == 1);

            auto &stats = me.Relax().BackLog().Stats;

            /* noDiff1 ("aab") and noDiff2 ("aac") have first column
               not in the bloom {aaa, bbb} → should be Weeded.
               noSame ("aaa") is in the bloom → not weeded (Sieved). */
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 2);
        }

        /*_ 20: Recompact all to one part, verify prefix bloom persists */

        me.To(20).Compact(1, true /* final */);
        me.To(21).Select(1).Has(rw1, rw2, rw3).NoKey(noDiff1, noDiff2);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.size() == 1);

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 2);
        }

        /*_ 30: Disable prefix bloom, recompact, verify it's gone */

        me.To(30).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {}));
        me.To(31).Commit().Compact(1, true /* final */);
        me.To(32).Select(1).Has(rw1);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.empty());
            me.Relax();
        }
    }

    Y_UNIT_TEST(PrefixBloomWithFullKey)
    {
        /* Test that prefix bloom on 1 column and full-key bloom (as prefix=3) coexist.
           Only the longest prefix bloom (prefix=3) is used for lookups. */
        TDbExec me;

        {
            TAlter alter;
            alter
                .AddTable("me", 1)
                .AddColumn(1, "name", 1, ETypes::String, false, false)
                .AddColumn(1, "salt", 2, ETypes::Uint64, false, false)
                .AddColumn(1, "sub",  3, ETypes::String, false, false)
                .AddColumnToKey(1, 1)
                .AddColumnToKey(1, 2)
                .AddColumnToKey(1, 3);

            me.To(10).Begin().Apply(*alter).Commit();
        }

        const auto rw1 = *me.SchemedCookRow(1).Col("aaa", 100_u64, "x1");
        const auto rw2 = *me.SchemedCookRow(1).Col("aaa", 200_u64, "x2");
        const auto rw3 = *me.SchemedCookRow(1).Col("bbb", 300_u64, "x3");

        /* Key with same prefix "aaa" but non-existing rest — full-key bloom (prefix=3)
           rejects (different salt+sub combination) */
        const auto noSamePrefix = *me.SchemedCookRow(1).Col("aaa", 999_u64, "zz");
        /* Key with different prefix — full-key bloom (prefix=3) rejects */
        const auto noDiffPrefix = *me.SchemedCookRow(1).Col("aab", 100_u64, "x1");

        /* Enable prefix bloom on 1 column and full-key bloom (prefix=3) */
        me.To(11).Begin()
            .Apply(*TAlter().SetByKeyFilterPrefixes(1, {1, 3}));
        me.To(12).Put(1, rw1, rw2, rw3).Commit().Snap(1).Compact(1);
        me.To(13).Select(1).Has(rw1, rw2, rw3).NoKey(noSamePrefix, noDiffPrefix);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.size() == 2); /* prefix=1 and prefix=3 */
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes[0].first < subset->Flatten[0]->ByKeyPrefixes[1].first);

            auto &stats = me.Relax().BackLog().Stats;

            /* noSamePrefix: full-key bloom (prefix=3) rejects (aaa,999,zz not in bloom) → Weeded
               noDiffPrefix: full-key bloom (prefix=3) rejects (aab,100,x1 not in bloom) → Weeded */
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 2);
        }
    }

    Y_UNIT_TEST(MultiplePrefixes)
    {
        /* Test two prefix bloom filters on 1 and 2 PK columns.
           Only the longest prefix bloom (prefix=2) is used for lookups. */
        TDbExec me;

        {
            TAlter alter;
            alter
                .AddTable("me", 1)
                .AddColumn(1, "name", 1, ETypes::String, false, false)
                .AddColumn(1, "salt", 2, ETypes::Uint64, false, false)
                .AddColumn(1, "sub",  3, ETypes::String, false, false)
                .AddColumnToKey(1, 1)
                .AddColumnToKey(1, 2)
                .AddColumnToKey(1, 3);

            me.To(10).Begin().Apply(*alter).Commit();
        }

        const auto rw1 = *me.SchemedCookRow(1).Col("aaa", 100_u64, "x1");
        const auto rw2 = *me.SchemedCookRow(1).Col("aaa", 200_u64, "x2");
        const auto rw3 = *me.SchemedCookRow(1).Col("bbb", 300_u64, "x3");

        /* Same 1st column "aaa", different 2nd column — rejected by bloom-on-2 (longest) */
        const auto noPassesFirst = *me.SchemedCookRow(1).Col("aaa", 999_u64, "zz");
        /* Different 1st column — rejected by bloom-on-2 (longest) */
        const auto noDiff = *me.SchemedCookRow(1).Col("aab", 100_u64, "x1");

        me.To(11).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2, 1})); /* deliberately reversed */
        me.To(12).Put(1, rw1, rw2, rw3).Commit().Snap(1).Compact(1);
        me.To(13).Select(1).Has(rw1, rw2, rw3).NoKey(noPassesFirst, noDiff);

        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.size() == 2);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes[0].first < subset->Flatten[0]->ByKeyPrefixes[1].first);

            auto &stats = me.Relax().BackLog().Stats;

            /* noPassesFirst (aaa,999,...): bloom-on-2 rejects (aaa,999 not in bloom) → Weeded.
               noDiff (aab,...): bloom-on-2 rejects (aab,100 not in bloom) → Weeded. */
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 2);
        }
    }

    Y_UNIT_TEST(LegacyByKeyFilterAlter)
    {
        /* Use case: a tablet boots from old schema log containing legacy
           ByKeyFilter=1 records. Bloom filtering should work identically
           to the new prefix-based API. Legacy disable should remove filtering. */
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        const auto rw1 = *me.SchemedCookRow(1).Col("ba0_huxu", 01234_u64);
        const auto rw2 = *me.SchemedCookRow(1).Col("ba2_d7mj", 12340_u64);
        const auto no1 = *me.SchemedCookRow(1).Col("ba1_p9lw", 53543_u64);

        /* Enable bloom via legacy ByKeyFilter=1 alter record (raw proto) */
        {
            NTabletFlatScheme::TSchemeChanges changes;
            auto* delta = changes.AddDelta();
            delta->SetDeltaType(NTabletFlatScheme::TAlterRecord::SetTable);
            delta->SetTableId(1);
            delta->SetByKeyFilter(1); /* legacy enable */
            me.To(20).Begin().Apply(changes);
        }
        me.To(21).Put(1, rw1, rw2).Commit().Snap(1).Compact(1);

        /* Bloom must filter absent keys — same behavior as new API */
        me.To(22).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes[0].first == 2);

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT(stats.SelectWeeded > 0);
        }

        /* Disable bloom via legacy ByKeyFilter=0, recompact */
        {
            NTabletFlatScheme::TSchemeChanges changes;
            auto* delta = changes.AddDelta();
            delta->SetDeltaType(NTabletFlatScheme::TAlterRecord::SetTable);
            delta->SetTableId(1);
            delta->SetByKeyFilter(0); /* legacy disable */
            me.To(30).Begin().Apply(changes);
        }
        me.To(31).Commit().Compact(1, true);

        /* After disable + recompact, no filtering should occur */
        me.To(32).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
        }
    }

    Y_UNIT_TEST(SnapshotRoundTrip)
    {
        /* Use case: tablet restarts (Boot) or follower syncs (Redo).
           Bloom filter settings must survive and continue filtering. */
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        const auto rw1 = *me.SchemedCookRow(1).Col("ba0_huxu", 01234_u64);
        const auto rw2 = *me.SchemedCookRow(1).Col("ba2_d7mj", 12340_u64);
        const auto no1 = *me.SchemedCookRow(1).Col("ba1_p9lw", 53543_u64);

        /* Enable bloom, write data, compact */
        me.To(20).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2}));
        me.To(21).Put(1, rw1, rw2).Commit().Snap(1).Compact(1);

        /* Bloom filters before restart */
        me.To(22).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT(stats.SelectWeeded > 0);
        }

        /* Restart via Boot — bloom must still filter after recompaction */
        me.Replay(EPlay::Boot);
        me.To(30).Snap(1).Compact(1, true);
        me.To(31).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(!subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT(stats.SelectWeeded > 0);
        }

        /* Restart via Redo (follower sync) — same */
        me.Replay(EPlay::Redo);
        me.To(40).Snap(1).Compact(1, true);
        me.To(41).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(!subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT(stats.SelectWeeded > 0);
        }
    }

    Y_UNIT_TEST(ClearBloomSnapshotRoundTrip)
    {
        /* Use case: bloom is enabled then disabled, tablet restarts.
           The disabled state must survive — no filtering after restart. */
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        const auto rw1 = *me.SchemedCookRow(1).Col("ba0_huxu", 01234_u64);
        const auto rw2 = *me.SchemedCookRow(1).Col("ba2_d7mj", 12340_u64);
        const auto no1 = *me.SchemedCookRow(1).Col("ba1_p9lw", 53543_u64);

        /* Enable bloom, write data, compact */
        me.To(20).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2}));
        me.To(21).Put(1, rw1, rw2).Commit().Snap(1).Compact(1);

        /* Bloom filters initially */
        me.To(22).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT(stats.SelectWeeded > 0);
        }

        /* Disable bloom, recompact */
        me.To(30).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {}));
        me.To(31).Commit().Compact(1, true);

        /* No filtering after disable */
        me.To(32).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
        }

        /* Restart via Boot — disabled state survives */
        me.Replay(EPlay::Boot);
        me.To(40).Snap(1).Compact(1, true);
        me.To(41).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
        }

        /* Restart via Redo — disabled state survives */
        me.Replay(EPlay::Redo);
        me.To(50).Snap(1).Compact(1, true);
        me.To(51).Select(1).Has(rw1, rw2).NoKey(no1);
        {
            auto subset = me->Subset(1, TEpoch::Max(), { }, { });
            UNIT_ASSERT(subset->Flatten.size() == 1);
            UNIT_ASSERT(subset->Flatten[0]->ByKeyPrefixes.empty());

            auto &stats = me.Relax().BackLog().Stats;
            UNIT_ASSERT_VALUES_EQUAL(stats.SelectWeeded, 0);
        }
    }

    Y_UNIT_TEST(SnapshotBackwardCompatibility)
    {
        /* Regression test: when snapshot is created with ByKeyFilterPrefixes for full key,
           GetSnapshot() must also set the legacy ByKeyFilter field.
           This ensures older versions understand that bloom filtering is active
           and don't disable it during their own snapshots. */
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        /* Enable full-key (2-column) bloom filter using new API */
        me.To(20).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2})).Commit();

        /* GetSnapshot() must include both legacy ByKeyFilter and new ByKeyFilterPrefixes */
        {
            auto snapshot = me->GetScheme().GetSnapshot();
            bool foundLegacy = false;
            bool foundNew = false;
            for (const auto& delta : snapshot->GetDelta()) {
                if (delta.GetDeltaType() == NTabletFlatScheme::TAlterRecord::SetTable &&
                    delta.GetTableId() == 1) {
                    if (delta.HasByKeyFilter() && delta.GetByKeyFilter() != 0) {
                        foundLegacy = true;
                    }
                    if (delta.ByKeyFilterPrefixesSize() > 0) {
                        foundNew = true;
                    }
                }
            }
            UNIT_ASSERT_C(foundLegacy, "GetSnapshot() must set legacy ByKeyFilter for full-key bloom");
            UNIT_ASSERT_C(foundNew, "GetSnapshot() must set ByKeyFilterPrefixes");
        }
    }

    Y_UNIT_TEST(PrefixExceedsKeyCount)
    {
        /* Bloom filter prefix must not exceed the number of key columns.
           The table has a 2-column PK (name, salt):
             prefix=1 and prefix=2 are valid prefixes and must be accepted;
             prefix=3 exceeds the key count and must be rejected. */
        TDbExec me;

        me.To(10).Begin().Apply(*MakeAlter()).Commit();

        // prefix=1 (first key column): valid
        me.To(11).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {1})).Commit();

        // prefix=2 (full key): valid
        me.To(12).Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {2})).Commit();

        // prefix=3: exceeds key column count — must be rejected
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            me.Begin().Apply(*TAlter().SetByKeyFilterPrefixes(1, {3})),
            yexception,
            "exceeds key column count"
        );
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
            alter.AddColumn(1, name, col, ETypes::String, false, false);
            alter.AddColumnToKey(1, col);
            if (col > 10) {
                alter.SetByKeyFilterPrefixes(1, {col}); /* enable bloom on full key */
            } else {
                alter.SetByKeyFilterPrefixes(1, {}); /* no bloom for first 8 parts */
            }

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
