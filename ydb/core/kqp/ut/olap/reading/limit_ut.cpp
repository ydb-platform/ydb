#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(ScanLimit) {
    namespace {

    constexpr ui32 PortionsCount = 10;

    void RunDdl(TSession& session, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TExecuteQueryResult RunQuery(TSession& session, const TString& query) {
        auto result = session.ExecuteQuery(Q_(query), TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    // Number of portions the shard has walked so far, over every scan in this runner.
    // TScanCounters::OnSourceFinished bumps it once per portion, so the difference across a
    // single query is exactly how many portions that query read.
    i64 GetProcessedSourceCount(TKikimrRunner& kikimr) {
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        return GetServiceCounters(runtime->GetAppData().Counters, "tablets")
            ->GetSubgroup("subsystem", "columnshard")
            ->GetSubgroup("module_id", "Scan")
            ->GetCounter("Deriviative/ProcessedSource/Count", true)
            ->Val();
    }

    // `SELECT ... LIMIT n` without ORDER BY must stop as soon as the limit is satisfied. It used to
    // walk every portion instead, because deduplication forged an ASC sorting for the scan and that
    // routed it into the one sources collection with no limit logic.
    void CheckLimitStopsScan(const TString& readerClassName) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.SetColumnShardReaderClassName(readerClassName);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

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

        // Every INSERT commits on its own, so each one becomes a separate portion.
        for (ui32 i = 0; i < PortionsCount; ++i) {
            RunQuery(session, Sprintf("INSERT INTO `/Root/KV` (id, vn) VALUES (%uu, %u);", i, i * 10));
        }

        // Without several portions the test proves nothing: one portion is read either way.
        {
            auto result = RunQuery(session, R"(
                SELECT COUNT(*) AS Portions
                FROM `/Root/KV/.sys/primary_index_portion_stats`
                WHERE Activity == 1;
            )");
            auto portions = result.GetResultSetParser(0);
            UNIT_ASSERT(portions.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(portions.ColumnParser("Portions").GetUint64(), PortionsCount);
        }

        const i64 processedBefore = GetProcessedSourceCount(kikimr);

        auto result = RunQuery(session, R"(
            SELECT vn FROM `/Root/KV` LIMIT 1;
        )");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);

        // The one portion holding the row the limit needs, and not one of the other nine.
        UNIT_ASSERT_VALUES_EQUAL(GetProcessedSourceCount(kikimr) - processedBefore, 1);
    }

    }   // namespace

    Y_UNIT_TEST(TrivialReaderStopsOnLimit) {
        CheckLimitStopsScan("TRIVIAL");
    }

    Y_UNIT_TEST(SimpleReaderStopsOnLimit) {
        CheckLimitStopsScan("SIMPLE");
    }
}

}   // namespace NKikimr::NKqp
