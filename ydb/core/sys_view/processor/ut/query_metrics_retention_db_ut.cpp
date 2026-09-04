#include <ydb/core/sys_view/common/query_metrics_limits.h>
#include <ydb/core/sys_view/processor/query_metrics_retention.h>
#include <ydb/core/sys_view/processor/query_metrics_retention_db.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {
namespace {

class TQueryMetricsTestDb {
public:
    template <typename TCallback>
    void Transaction(TCallback&& callback) {
        NTable::TDummyEnv env;
        Database.Begin(++Stamp, env);
        NIceDb::TNiceDb db(Database);
        callback(db);
        Database.Commit(Stamp, true);
    }

private:
    NTable::TDatabase Database;
    ui64 Stamp = 0;
};

void WriteRow(
    NIceDb::TNiceDb& db,
    ui64 hourEnd,
    ui32 rank,
    size_t textBytes,
    size_t dataBytes)
{
    db.Table<TProcessorSchema::MetricsOneHour>().Key(hourEnd, rank).Update(
        NIceDb::TUpdate<TProcessorSchema::MetricsOneHour::Text>(
            TString(textBytes, 't')),
        NIceDb::TUpdate<TProcessorSchema::MetricsOneHour::Data>(
            TString(dataBytes, 'm')));
}

ui64 ReadCutoff(NIceDb::TNiceDb& db) {
    auto row = db.Table<TProcessorSchema::SysParams>()
        .Key(TProcessorSchema::SysParam_MetricsOneHourEvictBeforeHourEnd)
        .Select<TProcessorSchema::SysParams::Value>();
    UNIT_ASSERT(row.IsReady());
    UNIT_ASSERT(row.IsValid());
    return FromString<ui64>(row.GetValue<TProcessorSchema::SysParams::Value>());
}

void WriteCutoff(NIceDb::TNiceDb& db, ui64 cutoff) {
    db.Table<TProcessorSchema::SysParams>()
        .Key(TProcessorSchema::SysParam_MetricsOneHourEvictBeforeHourEnd)
        .Update(NIceDb::TUpdate<TProcessorSchema::SysParams::Value>(
            ToString(cutoff)));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryMetricsRetentionDbTest) {
    Y_UNIT_TEST(EvictsWholeOldestClosedBuckets) {
        const TMap<ui64, ui64> bucketBytes = {
            {100, 40},
            {200, 30},
            {300, 50},
        };

        auto plan = PlanQueryMetricsRetention(
            bucketBytes, /* activeHourEnd */ 300, /* byteLimit */ 80);

        UNIT_ASSERT_VALUES_EQUAL(plan.RetainedBytes, 80);
        UNIT_ASSERT_VALUES_EQUAL(plan.BucketsToEvict.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan.BucketsToEvict[0], 100);
        UNIT_ASSERT_VALUES_EQUAL(plan.EvictBeforeHourEnd, 200);
    }

    Y_UNIT_TEST(NeverEvictsActiveBucket) {
        const TMap<ui64, ui64> bucketBytes = {
            {100, 10},
            {200, 100},
        };

        auto plan = PlanQueryMetricsRetention(
            bucketBytes, /* activeHourEnd */ 200, /* byteLimit */ 50);

        UNIT_ASSERT_VALUES_EQUAL(plan.RetainedBytes, 100);
        UNIT_ASSERT_VALUES_EQUAL(plan.BucketsToEvict.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan.BucketsToEvict[0], 100);
        UNIT_ASSERT_VALUES_EQUAL(plan.EvictBeforeHourEnd, 200);
    }

    Y_UNIT_TEST(LoadsNewestWholeBucketsWithinByteLimitAfterReboot) {
        TQueryMetricsTestDb testDb;
        testDb.Transaction([](NIceDb::TNiceDb& db) {
            db.Materialize<TProcessorSchema>();

            // 40 bytes in the oldest closed bucket.
            WriteRow(db, 100, 1, 10, 10);
            WriteRow(db, 100, 2, 10, 10);
            // 30 bytes in the next closed bucket.
            WriteRow(db, 200, 1, 5, 10);
            WriteRow(db, 200, 2, 5, 10);
            // 50 bytes in the active bucket.
            WriteRow(db, 300, 1, 15, 10);
            WriteRow(db, 300, 2, 15, 10);
        });

        testDb.Transaction([](NIceDb::TNiceDb& db) {
            TQueryMetricsOneHourLoadResult loaded;
            UNIT_ASSERT(LoadQueryMetricsOneHour(
                db, /* activeHourEnd */ 300, /* byteLimit */ 80,
                /* persistentCutoff */ 0, loaded));

            UNIT_ASSERT_VALUES_EQUAL(loaded.Rows.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(loaded.RetainedBytes, 80);
            UNIT_ASSERT_VALUES_EQUAL(loaded.EvictBeforeHourEnd, 200);
            for (const auto& row : loaded.Rows) {
                UNIT_ASSERT_GE(row.HourEnd, 200);
            }
            WriteCutoff(db, loaded.EvictBeforeHourEnd);
        });

        // Simulate a reboot before physical cleanup: the persistent cutoff
        // keeps the oldest bucket invisible even though its rows still exist.
        testDb.Transaction([](NIceDb::TNiceDb& db) {
            const ui64 cutoff = ReadCutoff(db);
            TQueryMetricsOneHourLoadResult loaded;
            UNIT_ASSERT(LoadQueryMetricsOneHour(
                db, /* activeHourEnd */ 300, /* byteLimit */ 80,
                cutoff, loaded));

            UNIT_ASSERT_VALUES_EQUAL(cutoff, 200);
            UNIT_ASSERT_VALUES_EQUAL(loaded.Rows.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(loaded.RetainedBytes, 80);
            for (const auto& row : loaded.Rows) {
                UNIT_ASSERT_GE(row.HourEnd, cutoff);
            }
        });
    }

    Y_UNIT_TEST(ResumesBatchedCleanupAfterReboot) {
        constexpr ui32 rowsPerClosedBucket =
            NQueryMetricsLimits::OneHourCleanupBatchSize + 1;
        constexpr size_t totalRowsToDelete = 2 * rowsPerClosedBucket;

        TQueryMetricsTestDb testDb;
        testDb.Transaction([&](NIceDb::TNiceDb& db) {
            db.Materialize<TProcessorSchema>();
            for (ui32 rank = 1; rank <= rowsPerClosedBucket; ++rank) {
                WriteRow(db, 100, rank, 1, 1);
                WriteRow(db, 200, rank, 1, 1);
            }
            for (ui32 rank = 1; rank <= 10; ++rank) {
                WriteRow(db, 300, rank, 1, 1);
            }
            WriteCutoff(db, 300);
        });

        size_t totalDeleted = 0;
        ui64 totalEvictedBuckets = 0;
        size_t transactions = 0;
        for (;;) {
            bool more = false;
            testDb.Transaction([&](NIceDb::TNiceDb& db) {
                const ui64 cutoff = ReadCutoff(db);
                TQueryMetricsOneHourCleanupResult cleanup;
                UNIT_ASSERT(CleanupQueryMetricsOneHour(
                    db, cutoff,
                    NQueryMetricsLimits::OneHourCleanupBatchSize,
                    cleanup));
                totalDeleted += cleanup.Deleted;
                totalEvictedBuckets += cleanup.EvictedBuckets;
                more = cleanup.More;
                WriteCutoff(db, cleanup.NewCutoff);
            });
            ++transactions;

            if (transactions == 1) {
                // This is the crash point: a new loader sees only the active
                // bucket while hundreds of old rows are still in LocalDB.
                testDb.Transaction([](NIceDb::TNiceDb& db) {
                    const ui64 cutoff = ReadCutoff(db);
                    TQueryMetricsOneHourLoadResult loaded;
                    UNIT_ASSERT(LoadQueryMetricsOneHour(
                        db, /* activeHourEnd */ 300, Max<ui64>(), cutoff, loaded));
                    UNIT_ASSERT_VALUES_EQUAL(loaded.Rows.size(), 10);
                    for (const auto& row : loaded.Rows) {
                        UNIT_ASSERT_VALUES_EQUAL(row.HourEnd, 300);
                    }
                });
            }

            if (!more) {
                break;
            }
        }

        const size_t expectedTransactions =
            (totalRowsToDelete + NQueryMetricsLimits::OneHourCleanupBatchSize - 1)
            / NQueryMetricsLimits::OneHourCleanupBatchSize;
        UNIT_ASSERT_VALUES_EQUAL(transactions, expectedTransactions);
        UNIT_ASSERT_VALUES_EQUAL(totalDeleted, totalRowsToDelete);
        UNIT_ASSERT_VALUES_EQUAL(totalEvictedBuckets, 2);

        testDb.Transaction([](NIceDb::TNiceDb& db) {
            UNIT_ASSERT_VALUES_EQUAL(ReadCutoff(db), 0);
            auto rows = db.Table<TProcessorSchema::MetricsOneHour>().Range().Select();
            UNIT_ASSERT(rows.IsReady());
            size_t count = 0;
            while (!rows.EndOfSet()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    rows.GetValue<TProcessorSchema::MetricsOneHour::IntervalEnd>(), 300);
                ++count;
                UNIT_ASSERT(rows.Next());
            }
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
        });
    }
}

} // namespace NKikimr::NSysView
