#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <format>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

NKikimrConfig::TAppConfig GetAppConfig(size_t maxBatchSize = 10000) {
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableOlapSink(true);
    app.MutableTableServiceConfig()->SetEnableOltpSink(true);
    app.MutableTableServiceConfig()->SetEnableBatchUpdates(true);
    app.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetMaxBatchSize(maxBatchSize);
    return app;
}

TExecuteQuerySettings GetQuerySettings() {
    TExecuteQuerySettings execSettings;
    execSettings.StatsMode(EStatsMode::Basic);
    return execSettings;
}

void CreateTuplePrimaryTable(TSession& session) {
    UNIT_ASSERT(session.ExecuteQuery(R"(
        CREATE TABLE TuplePrimary (
            Col1 Uint32,
            Col2 Uint64,
            Col3 Int64,
            Col4 Int64,
            PRIMARY KEY (Col2, Col1, Col3)
        ) WITH (
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2,
            PARTITION_AT_KEYS = (2, 3)
        );)", TTxControl::NoTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteQuery(R"(
        REPLACE INTO TuplePrimary (Col1, Col2, Col3, Col4) VALUES
                (0, 1, 0, 0),
                (1, 1, 0, 0),
                (1, 1, 1, 0),
                (1, 1, 2, 0),
                (2, 1, 0, 0),
                (1, 2, 0, 0),
                (1, 2, 1, 0),
                (2, 2, 0, 0),
                (3, 2, 0, 0),
                (0, 3, 0, 0),
                (1, 3, 0, 0),
                (2, 3, 0, 0),
                (0, 3, 0, 0),
                (1, 3, 0, 0),
                (2, 3, 0, 0),
                (3, 3, 0, 0);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

void ExecQueryAndTestEmpty(TSession& session, const TString& query) {
    auto txControl = TTxControl::NoTx();
    auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    CompareYson("[[0u]]", FormatResultSetYson(result.GetResultSet(0)));
}

void TestSimple(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH DELETE FROM KeyValue;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValue;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM KeyValueLargePartition;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValueLargePartition;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM Test
                WHERE Amount >= 5000ul;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Amount >= 5000ul;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM Test
                WHERE Amount < 5000ul AND Group < 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Amount < 5000ul AND Group < 2;
        )");
    }
}

void TestPartitionTables(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH DELETE FROM TwoShard
                WHERE Key >= 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TwoShard
                WHERE Key >= 2;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM EightShard
                WHERE Key > 300 AND Data = 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM EightShard
                WHERE Key > 300 AND Data = 2;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM Logs
                WHERE App = "kikimr-db";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Logs
                WHERE App = "kikimr-db";
        )");
    }
    {
        auto query = Q_(R"(
            DELETE FROM Join2
                WHERE Key1 = 102 AND Key2 = "One";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Join2
                WHERE Key1 = 102 AND Key2 = "One";
        )");
    }
    {
        CreateTuplePrimaryTable(session);

        auto query = Q_(R"(
            BATCH DELETE FROM TuplePrimary
                WHERE Col3 = 0;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TuplePrimary
                WHERE Col3 = 0;
        )");
    }
}

void TestLargeTable(size_t maxBatchSize, size_t rowsPerShard) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateLargeTable(kikimr, rowsPerShard, 4, 4, 10000);

    {
        auto query = Q_(R"(
            BATCH DELETE FROM LargeTable
                WHERE Key >= 2000;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM LargeTable
                WHERE Key >= 2000;
        )");
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpBatchDelete) {
    Y_UNIT_TEST(Simple) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestSimple(size);
        }
    }

    Y_UNIT_TEST(PartitionTables) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestPartitionTables(size);
        }
    }

    Y_UNIT_TEST(LargeTable_1) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestLargeTable(size, 100);
        }
    }

    Y_UNIT_TEST(LargeTable_2) {
        for (size_t size = 100; size <= 10000; size *= 10) {
            TestLargeTable(size, 10000);
        }
    }

    Y_UNIT_TEST(LargeTable_3) {
        for (size_t size = 1000; size <= 100000; size *= 10) {
            TestLargeTable(size, 100000);
        }
    }

    Y_UNIT_TEST(MultiTable) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test
                    WHERE Comment IN (
                        SELECT Comment FROM Test WHERE Group >= 1;
                    );
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue;
                BATCH DELETE FROM KeyValue2;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
    }

    Y_UNIT_TEST(MultiStatement) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test WHERE Amount > 100;
                SELECT 42;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test WHERE Amount > 100;
                SELECT * FROM Test;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue WHERE Key >= 3;
                UPSERT INTO KeyValue (Key, Value)
                    VALUES (10, "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue;
                UPSERT INTO KeyValue2 (Key, Value)
                    VALUES ("Key10", "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
