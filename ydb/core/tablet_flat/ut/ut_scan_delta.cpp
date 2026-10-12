#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <ydb/core/tablet_flat/flat_scan_delta.h>
#include <ydb/core/tablet_flat/flat_scan_feed.h>
#include <ydb/core/tablet_flat/flat_table_subset.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

using namespace NTest;

namespace {

    constexpr ui32 TagKey = 1;
    constexpr ui32 TagValue = 2;

    NTest::TLayoutCook MakeLayout() {
        NTest::TLayoutCook lay;
        lay
            .Col(0, TagKey,   NScheme::NTypeIds::Uint64)
            .Col(0, TagValue, NScheme::NTypeIds::Uint64)
            .Key({ TagKey });
        return lay;
    }

    TSubset MakeSubset(const TPartEggs& eggs) {
        TVector<TPartView> partView;
        for (const auto& part : eggs.Parts) {
            Y_ENSURE(part->Slices, "Missing part slices");
            partView.push_back({ part, nullptr, part->Slices });
        }
        return TSubset(TEpoch::Zero(), eggs.Scheme, std::move(partView));
    }

    /* Builds a subset with a single memtable holding key 1 written at the
       specified versions (in ascending order). */
    TSubset MakeMemTableSubset(const TLayoutCook& lay, const TVector<std::pair<TRowVersion, ui64>>& rows) {
        TIntrusiveConstPtr<TRowScheme> scheme = lay.RowScheme();
        TIntrusivePtr<TMemTable> memTable = new TMemTable(scheme, TEpoch::Zero(), 0);
        NTest::TRowTool tool(*scheme);

        for (const auto& [version, value] : rows) {
            auto row = *NTest::TSchemedCookRow(*scheme).Col(1_u64, value);
            auto split = tool.Split(row, true, true);
            memTable->Update(ERowOp::Upsert, split.Key, split.Ops, { }, version, /* committed */ nullptr);
        }

        TVector<TMemTableSnapshot> frozen;
        frozen.emplace_back(TMemTableSnapshot{ memTable, memTable->Immediate() });
        return TSubset(TEpoch::Zero(), std::move(scheme), std::move(frozen));
    }

    struct TDelta {
        TOwnedCellVec Key;
        ERowOp OldOp = ERowOp::Absent;
        ERowOp NewOp = ERowOp::Absent;
        ui64 OldValue = 0;
        ui64 NewValue = 0;
    };

    ui64 ValueOf(const TRowState& state) {
        const auto& cell = state.Get(1 /* value column */);
        return cell.IsNull() ? 0 : cell.AsValue<ui64>();
    }

    /* Consumer of TDeltaScan that just records the deltas it is given. */
    class TTestDeltaScan: public TDeltaScan {
    public:
        TTestDeltaScan(TRowVersion from, TRowVersion to)
            : TDeltaScan(from, to)
        {}

        TVector<TDelta> Deltas;
        TVector<ui32> Tags;

    private:
        EScan Seek(TLead& lead, ui64 seq) override {
            lead.To(Tags, { }, ESeek::Lower);
            return seq == 0 ? EScan::Feed : EScan::Final;
        }

        EScan FeedDelta(TArrayRef<const TCell> key, const TRowState& oldState, const TRowState& newState) override {
            Deltas.push_back({
                .Key = TOwnedCellVec::Make(key),
                .OldOp = oldState.GetRowState(),
                .NewOp = newState.GetRowState(),
                .OldValue = ValueOf(oldState),
                .NewValue = ValueOf(newState),
            });
            return EScan::Feed;
        }

        TAutoPtr<IDestructable> Finish(EStatus) override {
            Y_ABORT("TDeltaScan::Finish is not used in this test");
        }

        void Describe(IOutputStream& out) const override {
            out << "DeltaScan{test}";
        }
    };

    class TTestDriver: public TFeed {
    public:
        TTestDriver(IScan* scan, const TSubset& subset, IPages* env)
            : TFeed(scan, subset)
            , Env(env)
        {
            Conf.NoErased = false;
        }

        IPages* MakeEnv() override { return Env; }

        TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>&) override {
            Y_ABORT("Cold parts are not supported in this test");
        }

        IPages* const Env = nullptr;
    };

    TVector<TDelta> RunDeltaScan(const TSubset& subset, TRowVersion from, TRowVersion to) {
        TTestDeltaScan scan(from, to);
        scan.Tags = subset.Scheme->Tags();

        NTest::TTestEnv env;
        TTestDriver driver(&scan, subset, &env);

        while (true) {
            const auto ready = driver.Process();
            if (ready == EReady::Gone) {
                break;
            }
            UNIT_ASSERT_C(ready == EReady::Data, "Unexpected scan result " << (int)ready);
        }

        return std::move(scan.Deltas);
    }

    /* Returns true when the scan was finished without iterating any rows. */
    bool WasDeltaScanSkipped(const TSubset& subset, TRowVersion from, TRowVersion to) {
        TTestDeltaScan scan(from, to);
        scan.Tags = subset.Scheme->Tags();

        NTest::TTestEnv env;
        TTestDriver driver(&scan, subset, &env);

        const auto ready = driver.Process();
        if (ready != EReady::Gone) {
            return false;
        }

        UNIT_ASSERT_C(scan.Deltas.empty(), "Skipped scan must not report any deltas");
        return true;
    }

    /* Four keys:
        key 1: updated inside (From, To]        -> old 100, new 300
        key 2: untouched inside (From, To]      -> no delta
        key 3: created inside (From, To]        -> old absent, new 100
        key 4: erased inside (From, To]         -> old 100, new absent
     */

} // anonymous

Y_UNIT_TEST_SUITE(TDeltaScanTest) {

    Y_UNIT_TEST(ChangedKeysOnly) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Ver({5, 1}).AddN(2_u64, 100_u64)
            .Ver({15, 1}).AddN(3_u64, 100_u64)
            .Ver({15, 1}).AddOpN(ERowOp::Erase, 4_u64)
            .Ver({5, 1}).AddN(4_u64, 100_u64)
            .Finish();

        auto subset = MakeSubset(part);
        auto deltas = RunDeltaScan(subset, TRowVersion(10, 1), TRowVersion(25, 1));

        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(deltas[0].Key[0].AsValue<ui64>(), 1u);
        UNIT_ASSERT(deltas[0].OldOp == ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0].OldValue, 100u);
        UNIT_ASSERT(deltas[0].NewOp == ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0].NewValue, 300u);

        UNIT_ASSERT_VALUES_EQUAL(deltas[1].Key[0].AsValue<ui64>(), 3u);
        UNIT_ASSERT(deltas[1].OldOp == ERowOp::Absent);
        UNIT_ASSERT(deltas[1].NewOp == ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(deltas[1].NewValue, 100u);

        UNIT_ASSERT_VALUES_EQUAL(deltas[2].Key[0].AsValue<ui64>(), 4u);
        UNIT_ASSERT(deltas[2].OldOp == ERowOp::Upsert);
        UNIT_ASSERT_VALUES_EQUAL(deltas[2].OldValue, 100u);
        UNIT_ASSERT(deltas[2].NewOp == ERowOp::Absent);
    }

    Y_UNIT_TEST(SameVersionYieldsNothing) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Finish();

        auto subset = MakeSubset(part);
        auto deltas = RunDeltaScan(subset, TRowVersion(25, 1), TRowVersion(25, 1));

        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 0);
    }

    Y_UNIT_TEST(RangeWithoutChangesYieldsNothing) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Finish();

        // Both From and To are older than any update, so the image is stable
        auto subset = MakeSubset(part);
        auto deltas = RunDeltaScan(subset, TRowVersion(10, 1), TRowVersion(15, 1));

        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 0);
    }

    Y_UNIT_TEST(SkipsRangeNewerThanData) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Finish();

        // The part ends at version 20, both snapshots see the same image
        auto subset = MakeSubset(part);

        UNIT_ASSERT(WasDeltaScanSkipped(subset, TRowVersion(20, 1), TRowVersion(30, 1)));
    }

    Y_UNIT_TEST(SkipsRangeOlderThanData) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Finish();

        // The part starts at version 5, both snapshots see no rows at all
        auto subset = MakeSubset(part);

        UNIT_ASSERT(WasDeltaScanSkipped(subset, TRowVersion(1, 1), TRowVersion(4, 1)));
    }

    Y_UNIT_TEST(ScansRangeWithChanges) {
        auto lay = MakeLayout();

        auto part = NTest::TPartCook(lay, { false, 4096 })
            .Ver({20, 1}).AddN(1_u64, 300_u64)
            .Ver({5, 1}).AddN(1_u64, 100_u64)
            .Finish();

        // (10, 15] overlaps the part bounds [5, 20], so the subset cannot be
        // proven empty and has to be scanned
        auto subset = MakeSubset(part);

        UNIT_ASSERT(!WasDeltaScanSkipped(subset, TRowVersion(10, 1), TRowVersion(15, 1)));
        UNIT_ASSERT_VALUES_EQUAL(RunDeltaScan(subset, TRowVersion(10, 1), TRowVersion(15, 1)).size(), 0);
    }

    Y_UNIT_TEST(SkipsMemTableWithoutChanges) {
        auto lay = MakeLayout();

        auto subset = MakeMemTableSubset(lay, {
            { TRowVersion(5, 1), 100 },
            { TRowVersion(20, 1), 300 },
        });

        UNIT_ASSERT(WasDeltaScanSkipped(subset, TRowVersion(20, 1), TRowVersion(30, 1)));
        UNIT_ASSERT(WasDeltaScanSkipped(subset, TRowVersion(1, 1), TRowVersion(4, 1)));
        UNIT_ASSERT(!WasDeltaScanSkipped(subset, TRowVersion(10, 1), TRowVersion(15, 1)));
    }

    Y_UNIT_TEST(SkipsEmptySubset) {
        auto lay = MakeLayout();

        auto subset = TSubset(TEpoch::Zero(), lay.RowScheme(), TVector<TPartView>{ });

        UNIT_ASSERT(WasDeltaScanSkipped(subset, TRowVersion(1, 1), TRowVersion(2, 1)));
    }

}

}
}
