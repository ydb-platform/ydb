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

void TestSimple(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH UPDATE KeyValue
                SET Value = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValue2
                WHERE Value != "None";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE KeyValueLargePartition
                SET Value = "None";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Group < 2 AND Comment != "Yes" AND Amount != 100;
        )");
    }
}

void TestSimplePartitions(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH UPDATE TwoShard
                SET Value2 = 3;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
                WHERE App = "kikimr-db";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Logs
                WHERE App = "kikimr-db" AND Message != "";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE Join2
                SET Name = "None", Value2 = "ValueN"
                WHERE Key1 = 102 AND Key2 = "One";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Join2
                WHERE Key1 = 102 AND Key2 = "One" AND Name != "None" AND Value2 != "ValueN";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE TuplePrimaryDescending
                SET Col4 = 2
                WHERE Col3 = 0;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM TuplePrimaryDescending
                WHERE Col3 = 0 AND Col4 != 2;
        )");
    }
}

void TestManyPartitions(size_t maxBatchSize, size_t totalRows, size_t shards) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateManyShardsTable(kikimr, totalRows, shards, 1000);

    {
        auto query = Q_(R"(
            BATCH UPDATE ManyShardsTable
                SET Data = -10;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ManyShardsTable
                WHERE Data != -10;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH UPDATE ManyShardsTable
                SET Data = 2
                WHERE Key >= 2000;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ManyShardsTable
                WHERE Key >= 2000 AND Data != 2;
        )");
    }
}

void TestLarge(size_t maxBatchSize, size_t rowsPerShard) {
    TKikimrRunner kikimr(GetAppConfig(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateLargeTable(kikimr, rowsPerShard, 4, 4, 10000);

    {
        auto query = Q_(R"(
            BATCH UPDATE LargeTable
                SET Data = -1, DataText = "Updated";
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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

    Y_UNIT_TEST(SimplePartitions) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestSimplePartitions(size);
        }
    }

    Y_UNIT_TEST(ManyPartitions_1) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestManyPartitions(size, 1000, 10);
        }
    }

    Y_UNIT_TEST(ManyPartitions_2) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestManyPartitions(size, 5000, 50);
        }
    }

    Y_UNIT_TEST(ManyPartitions_3) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestManyPartitions(size, 10000, 100);
        }
    }

    Y_UNIT_TEST(Large_1) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestLarge(size, 100);
        }
    }

    Y_UNIT_TEST(Large_2) {
        for (size_t size = 100; size <= 10000; size *= 10) {
            TestLarge(size, 10000);
        }
    }

    Y_UNIT_TEST(Large_3) {
        for (size_t size = 1000; size <= 100000; size *= 10) {
            TestLarge(size, 100000);
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

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = Amount * Group;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Name = Comment, Comment = Name;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                BATCH UPDATE KeyValue2
                    SET Value = "None";
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
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

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE Test
                    SET Amount = 1000;
                SELECT * FROM Test;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                UPSERT INTO KeyValue (Key, Value)
                    VALUES (10, "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
        {
            auto query = Q_(R"(
                BATCH UPDATE KeyValue
                    SET Value = "None";
                UPSERT INTO KeyValue2 (Key, Value)
                    VALUES ("Key10", "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
