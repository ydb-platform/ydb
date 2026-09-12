#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(ScanLimit) {
    namespace {

    void RunDdl(TSession& session, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TExecuteQueryResult RunQuery(TSession& session, const TString& query) {
        auto result = session.ExecuteQuery(Q_(query), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    TKikimrSettings MakeSettings(const TString& readerClassName, const std::optional<ui64> sortedStartInFlight = {}) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.SetColumnShardReaderClassName(readerClassName);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        if (sortedStartInFlight) {
            settings.AppConfig.MutableColumnShardConfig()->SetLimitSortedStartInFlight(*sortedStartInFlight);
        }
        return settings;
    }

    i64 GetScanCounter(TKikimrRunner& kikimr, const TString& name) {
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        return GetServiceCounters(runtime->GetAppData().Counters, "tablets")
            ->GetSubgroup("subsystem", "columnshard")
            ->GetSubgroup("module_id", "Scan")
            ->GetCounter(name, true)
            ->Val();
    }

    // Both counters are bumped once per source, in IDataSource::OnStartProcessing.
    i64 GetStartedSourceCount(TKikimrRunner& kikimr) {
        return GetScanCounter(kikimr, "Deriviative/StartedSource/Nonconflicting/Count");
    }

    i64 GetConflictingSourceCount(TKikimrRunner& kikimr) {
        return GetScanCounter(kikimr, "Deriviative/StartedSource/Conflicting/Count");
    }

    // Bumped per page of results, in TBuildResultStep, so only for portions that produced rows.
    i64 GetProcessedSourceCount(TKikimrRunner& kikimr) {
        return GetScanCounter(kikimr, "Deriviative/ProcessedSource/Count");
    }

    // Creates the table and fills it with portionsCount portions, one row each, ids 0, 1, 2, ...
    void PrepareTable(TSession& session, const ui32 portionsCount) {
        RunDdl(session, R"(
            CREATE TABLE `/Root/KV` (
                id Uint64 NOT NULL,
                vn Int32,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            );
        )");

        // Each INSERT commits separately, so each one writes its own portion.
        for (ui32 i = 0; i < portionsCount; ++i) {
            RunQuery(session, Sprintf("INSERT INTO `/Root/KV` (id, vn) VALUES (%uu, %u);", i, i * 10));
        }

        // Compaction is disabled above, but check anyway: with fewer portions the tests prove nothing.
        auto result = RunQuery(session, R"(
            SELECT COUNT(*) AS Portions
            FROM `/Root/KV/.sys/primary_index_portion_stats`
            WHERE Activity == 1;
        )");
        auto portions = result.GetResultSetParser(0);
        UNIT_ASSERT(portions.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(portions.ColumnParser("Portions").GetUint64(), portionsCount);
    }

    // SELECT ... LIMIT n without ORDER BY must read only as many portions as the limit needs. It used
    // to read all of them: deduplication set an ASC sorting, which sent the scan to a sources
    // collection that has no limit logic.
    void CheckLimitStopsScan(const TString& readerClassName) {
        constexpr ui32 portionsCount = 10;

        TKikimrRunner kikimr(MakeSettings(readerClassName));

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        PrepareTable(session, portionsCount);

        const i64 startedBefore = GetStartedSourceCount(kikimr);

        auto result = RunQuery(session, R"(
            SELECT vn FROM `/Root/KV` LIMIT 1;
        )");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);

        // Only the portion holding that row, not the other nine.
        UNIT_ASSERT_VALUES_EQUAL(GetStartedSourceCount(kikimr) - startedBefore, 1);
    }

    // A portion written by a transaction that has not committed is "conflicting". The scan reads such
    // portions only to detect the conflict, and reads all of them before any ordered portion, with source
    // indexes outside the key order. So they must add no rows to the result and must not use up the
    // limit. Here they hold the same keys as the committed data, so a leaked row changes the answer.
    void CheckConflictingPortionsKeepOrderAndLimit(const TString& readerClassName) {
        // One committed and one conflicting portion per key.
        constexpr ui32 portionsCount = 100;

        // ORDER BY + LIMIT starts with LimitSortedStartInFlight portions in flight, 16 by default, which
        // would let the scan read more portions than the limit needs. Set to 1 to make the count exact.
        TKikimrRunner kikimr(MakeSettings(readerClassName, 1));

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto client = kikimr.GetQueryClient();
        auto reader = client.GetSession().GetValueSync().GetSession();
        auto writer = client.GetSession().GetValueSync().GetSession();

        PrepareTable(reader, portionsCount);

        // One UPSERT per key in a single transaction, never committed: one conflicting portion per key.
        std::optional<TTransaction> tx;
        for (ui32 i = 0; i < portionsCount; ++i) {
            const auto control = tx ? TTxControl::Tx(*tx) : TTxControl::BeginTx(TTxSettings::SerializableRW());
            auto written = writer.ExecuteQuery(
                Q_(Sprintf("UPSERT INTO `/Root/KV` (id, vn) VALUES (%uu, -1);", i)), control).GetValueSync();
            UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
            tx = written.GetTransaction();
        }

        const i64 startedBefore = GetStartedSourceCount(kikimr);
        const i64 conflictingBefore = GetConflictingSourceCount(kikimr);
        const i64 processedBefore = GetProcessedSourceCount(kikimr);

        // The scan reads conflicting portions only when the transaction holds a lock, so the read has to
        // be inside an interactive transaction (see TReadDescription::readConflictingPortions).
        auto result = reader.ExecuteQuery(Q_(R"(
            SELECT id, vn FROM `/Root/KV` ORDER BY id LIMIT 3;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // The committed values, ordered by id. None of the uncommitted -1 values.
        CompareYson(R"([[0u;[0]];[1u;[10]];[2u;[20]]])", FormatResultSetYson(result.GetResultSet(0)));

        // All conflicting portions are read, because the conflict has to be detected in each of them.
        // The limit still works: only 3 of the 100 committed portions are read.
        UNIT_ASSERT_VALUES_EQUAL(GetConflictingSourceCount(kikimr) - conflictingBefore, portionsCount);
        UNIT_ASSERT_VALUES_EQUAL(GetStartedSourceCount(kikimr) - startedBefore, 3);

        // Those same 3 portions are the only ones that produced rows: no conflicting portion did.
        UNIT_ASSERT_VALUES_EQUAL(GetProcessedSourceCount(kikimr) - processedBefore, 3);
    }

    }   // namespace

    Y_UNIT_TEST(TrivialReaderStopsOnLimit) {
        CheckLimitStopsScan("TRIVIAL");
    }

    Y_UNIT_TEST(SimpleReaderStopsOnLimit) {
        CheckLimitStopsScan("SIMPLE");
    }

    Y_UNIT_TEST(TrivialReaderConflictingPortionsKeepOrderAndLimit) {
        CheckConflictingPortionsKeepOrderAndLimit("TRIVIAL");
    }

    Y_UNIT_TEST(SimpleReaderConflictingPortionsKeepOrderAndLimit) {
        CheckConflictingPortionsKeepOrderAndLimit("SIMPLE");
    }
}

}   // namespace NKikimr::NKqp
