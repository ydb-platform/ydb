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
            BATCH UPDATE KeyValue
                SET Value = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValue
                WHERE Value != "None";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE KeyValue2
                SET Value = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValue
                WHERE Value != "None";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE KeyValueLargePartition
                SET Value = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValueLargePartition
                WHERE Value != "None";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE Test
                SET Amount = 0
                WHERE Comment = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Comment != "None" AND Amount != 0;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE Test
                SET Amount = 100, Comment = "Yes"
                WHERE Comment = "None" AND Group < 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Group < 2 AND Comment != "Yes" AND Amount != 100;
        )");
    }
}

void TestPartitionTables(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH UPDATE TwoShard
                SET Value2 = 3;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TwoShard
                WHERE Value2 != 3;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE TwoShard
                SET Value2 = 5
                WHERE Key >= 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TwoShard
                WHERE Key >= 2 AND Value2 != 5;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE EightShard
                SET Text = "None"
                WHERE Key > 300 AND Data = 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM EightShard
                WHERE Key > 300 AND Data = 2 AND Text != "None";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE Logs
                SET Message = ""
                WHERE Host = "kikimr-db";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Logs
                WHERE Host = "kikimr-db" AND Message != "";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE Join2
                SET Name = "None", Value2 = "ValueN"
                WHERE Key1 = 102 AND Key2 = "One";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Join2
                WHERE Key1 = 102 AND Key2 = "One" AND Name != "None" AND Value2 != "ValueN";
        )");
    }
    {
        CreateTuplePrimaryTable(session);

        auto query = Q_(R"(
            BATCH UPDATE TuplePrimary
                SET Col4 = 2
                WHERE Col3 = 0;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TuplePrimary
                WHERE Col3 = 0 AND Col4 != 2;
        )");
    }
}

void TestLargeTable(size_t maxBatchSize, size_t rowsPerShard) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateLargeTable(kikimr, rowsPerShard, 4, 8);

    {
        auto query = Q_(R"(
            BATCH UPDATE LargeTable
                SET Data = -1, DataText = "Updated";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM LargeTable
                WHERE Data != -1 AND DataText != "Updated";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE LargeTable
                SET Data = 2
                WHERE Key >= 2000;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM LargeTable
                WHERE Key >= 2000 AND Data != 2;
        )");
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpBatchUpdate) {
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
        // for (size_t size = 100; size <= 10000; size *= 10) {
        //     TestLargeTable(size, 10000);
        // }
        TestLargeTable(1000, 10000);
    }

    Y_UNIT_TEST(LargeTable_3) {
        for (size_t size = 1000; size <= 100000; size *= 10) {
            TestLargeTable(size, 1000000);
        }
    }

    Y_UNIT_TEST(NotIdempotent) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = Amount * 10;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = Amount * Group;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Name = Comment, Comment = Name;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
    }

    Y_UNIT_TEST(MultiTable) {
        TKikimrRunner kikimr(GetAppConfig());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = 1000
                    WHERE Comment IN (
                        SELECT Comment FROM Test WHERE Group >= 1;
                    );
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None"
                    WHERE Value IN (
                        SELECT Value FROM KeyValue2;
                    );
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                BATCH UPDATE KeyValue2
                    SET Value = "None";
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
                BATCH UPDATE Test
                    SET Amount = 1000;
                SELECT 42;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = 1000;
                SELECT * FROM Test;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                UPSERT INTO KeyValue (Key, Value)
                    VALUES (10, "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                UPSERT INTO KeyValue2 (Key, Value)
                    VALUES ("Key10", "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), GetQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
    }

    // Y_UNIT_TEST(DeleteTwoPartitionsByPrimaryPoint) {
    //     TKikimrRunner kikimr(GetAppConfig());
    //     auto db = kikimr.GetQueryClient();
    //     auto session = db.GetSession().GetValueSync().GetSession();

    //     CreateTwoPartitionsTable(session);

    //     {
    //         auto query = Q_(R"(
    //             BATCH DELETE FROM TestTable WHERE Group = 1u;
    //         )");

    //         auto txControl = NYdb::NQuery::TTxControl::NoTx();
    //         auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
    //         UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), /* EStatus::SUCCESS */ EStatus::BAD_REQUEST); // todo: isBatch does not match in SA
    //     }
    //     {
    //         auto query = Q_(R"(
    //             SELECT * FROM TestTable ORDER BY Group;
    //         )");

    //         auto txControl = NYdb::NQuery::TTxControl::NoTx();
    //         auto result = session.ExecuteQuery(query, txControl, GetQuerySettings()).ExtractValueSync();
    //         UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

    //         CompareYson(R"([
    //             [[36u];[300u];[2u];["Paul"]];
    //             [[81u];[7200u];[3u];["Tony"]];
    //             [[11u];[10u];[4u];["John"]];
    //             [[3u];[0u];[5u];["Lena"]];
    //             [[48u];[730u];[6u];["Mary"]]
    //         ])", FormatResultSetYson(result.GetResultSet(0)));
    //     }
    // }
}

} // namespace NKqp
} // namespace NKikimr
