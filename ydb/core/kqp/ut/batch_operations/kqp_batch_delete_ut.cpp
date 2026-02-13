#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrSettings GetTestSettings(size_t maxBatchSize = 10000, size_t partitionLimit = 10,
    bool enableOltpSink = true, bool enableBatchUpdates = true, bool enableIndexStreamWrite = true)
{
    auto app = NKikimrConfig::TAppConfig();
    app.MutableTableServiceConfig()->SetEnableOlapSink(true);
    app.MutableTableServiceConfig()->SetEnableOltpSink(enableOltpSink);
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    app.MutableTableServiceConfig()->SetEnableBatchUpdates(enableBatchUpdates);
    app.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetMaxBatchSize(maxBatchSize);
    app.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetPartitionExecutionLimit(partitionLimit);

    auto logConfig = TTestLogSettings()
        .AddLogPriority(NKikimrServices::EServiceKikimr::KQP_EXECUTER, NLog::EPriority::PRI_TRACE)
        .AddLogPriority(NKikimrServices::EServiceKikimr::KQP_COMPUTE, NLog::EPriority::PRI_INFO);

    logConfig.DefaultLogPriority = NLog::EPriority::PRI_CRIT;
    return TKikimrSettings(std::move(app)).SetLogSettings(std::move(logConfig));
}

void TestSimpleOnePartition(size_t maxBatchSize) {
    TKikimrRunner kikimr(GetTestSettings(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH DELETE FROM KeyValue;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValue;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM KeyValueLargePartition;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM KeyValueLargePartition;
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM Test
                WHERE Amount >= 5000ul;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Test
                WHERE Amount < 5000ul AND Group < 2;
        )");
    }
}

void TestSimplePartitions(size_t maxBatchSize, size_t partitionLimit) {
    TKikimrRunner kikimr(GetTestSettings(maxBatchSize, partitionLimit));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        auto query = Q_(R"(
            BATCH DELETE FROM TwoShard
                WHERE Key >= 2;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

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
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM Join2
                WHERE Key1 = 102 AND Key2 = "One";
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM ReorderKey
                WHERE Col3 = 0;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ReorderKey
                WHERE Col3 = 0;
        )");
    }
    {
        // With literal tx
        auto query = Q_(R"(
            BATCH DELETE FROM ReorderOptionalKey
                WHERE k1 IN [0, 1, 2, 3];
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ReorderOptionalKey
                WHERE k1 IN [0, 1, 2, 3];
        )");
    }
    {
        auto query = Q_(R"(
            BATCH DELETE FROM ReorderOptionalKey
                WHERE id = 2 AND (k2 IS NULL OR k2 >= 0);
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ReorderOptionalKey
                WHERE id = 2 AND (k2 IS NULL OR k2 >= 0);
        )");
    }
}

void TestManyPartitions(size_t maxBatchSize, size_t totalRows, size_t shards, size_t partitionLimit) {
    TKikimrRunner kikimr(GetTestSettings(maxBatchSize, partitionLimit));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateManyShardsTable(kikimr, totalRows, shards, 1000);

    {
        auto query = Q_(R"(
            BATCH DELETE FROM ManyShardsTable
                WHERE Data >= 200;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM ManyShardsTable
                WHERE Data >= 200;
        )");
    }
}

void TestLarge(size_t maxBatchSize, size_t rowsPerShard) {
    TKikimrRunner kikimr(GetTestSettings(maxBatchSize));
    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    CreateLargeTable(kikimr, rowsPerShard, 4, 4, 10000);

    {
        auto query = Q_(R"(
            BATCH DELETE FROM LargeTable
                WHERE Key >= 2000;
        )");
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM LargeTable
                WHERE Key >= 2000;
        )");
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpBatchDelete) {
    Y_UNIT_TEST(SimpleOnePartition) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            TestSimpleOnePartition(size);
        }
    }

    Y_UNIT_TEST(SimplePartitions) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            for (size_t partitionLimit = 1; partitionLimit <= 10; partitionLimit *= 2) {
                TestSimplePartitions(size, partitionLimit);
            }
        }
    }

    Y_UNIT_TEST(ManyPartitions_1) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            for (size_t partitionLimit = 1; partitionLimit <= 16; partitionLimit *= 2) {
                TestManyPartitions(size, 100, 10, partitionLimit);
            }
        }
    }

    Y_UNIT_TEST(ManyPartitions_2) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            for (size_t partitionLimit = 1; partitionLimit <= 52; partitionLimit *= 4) {
                TestManyPartitions(size, 500, 50, partitionLimit);
            }
        }
    }

    Y_UNIT_TEST(ManyPartitions_3) {
        for (size_t size = 1; size <= 1000; size *= 10) {
            for (size_t partitionLimit = 1; partitionLimit <= 128; partitionLimit *= 8) {
                TestManyPartitions(size, 1000, 100, partitionLimit);
            }
        }
    }

    Y_UNIT_TEST(Large_1) {
        ui32 sizeLimit = NSan::PlainOrUnderSanitizer(1000, 100);
        for (size_t size = 1; size <= sizeLimit; size *= 10) {
            TestLarge(size, 100);
        }
    }

    Y_UNIT_TEST(Large_2) {
        ui32 sizeLimit = NSan::PlainOrUnderSanitizer(10000, 1000);
        ui32 shardRows = NSan::PlainOrUnderSanitizer(10000, 5000);
        for (size_t size = 100; size <= sizeLimit; size *= 10) {
            TestLarge(size, shardRows);
        }
    }

    Y_UNIT_TEST(Large_3) {
        ui32 sizeLimit = NSan::PlainOrUnderSanitizer(100000, 10000);
        ui32 shardRows = NSan::PlainOrUnderSanitizer(50000, 25000);
        for (size_t size = 1000; size <= sizeLimit; size *= 10) {
            TestLarge(size, shardRows);
        }
    }

    Y_UNIT_TEST(TableWithIndex) {
        TKikimrRunner kikimr(GetTestSettings());

        {
            auto db = kikimr.GetTableClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            CreateSampleTablesWithIndex(session, true, false);
        }

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            session.ExecuteQuery(R"(
                --!syntax_v1
                CREATE TABLE `/Root/TimestampTable` (
                    key Uint64 NOT NULL,
                    `timestamp` Uint64 NOT NULL,
                    value Utf8 FAMILY lz4_family NOT NULL,
                    PRIMARY KEY (key),
                    FAMILY lz4_family (
                        COMPRESSION = "lz4"
                    ),
                    INDEX by_timestamp GLOBAL ON (`timestamp`)
                )
                WITH (
                    STORE = ROW,
                    TTL = Interval("PT240S") ON `timestamp` AS SECONDS,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_BY_LOAD = ENABLED,
                    AUTO_PARTITIONING_PARTITION_SIZE_MB = 128,
                    READ_REPLICAS_SETTINGS = "PER_AZ:1",
                    KEY_BLOOM_FILTER = ENABLED
                );
            )", TTxControl::NoTx()).ExtractValueSync();

            auto result = session.ExecuteQuery(R"(
                REPLACE INTO `/Root/TimestampTable` (key, `timestamp`, value) VALUES
                    (1, 1, "1"),
                    (2, 2, "2"),
                    (3, 3, "3"),
                    (4, 4, "4"),
                    (5, 5, "5"),
                    (6, 6, "6"),
                    (7, 7, "7"),
                    (8, 8, "8"),
                    (9, 9, "9"),
                    (10, 10, "10");
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM `/Root/SecondaryKeys`
                    WHERE Fk IS NULL;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            ExecQueryAndTestEmpty(session, R"(
                SELECT count(*) FROM `/Root/SecondaryKeys`
                    WHERE Fk IS NULL;
            )");
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM `/Root/SecondaryComplexKeys`
                    WHERE Fk1 IS NOT NULL AND Fk2 >= "Fk2";
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            ExecQueryAndTestEmpty(session, R"(
                SELECT count(*) FROM `/Root/SecondaryComplexKeys`
                    WHERE Fk1 IS NOT NULL AND Fk2 >= "Fk2";
            )");
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM `/Root/TimestampTable`
                    WHERE `timestamp` > 0;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            ExecQueryAndTestEmpty(session, R"(
                SELECT count(*) FROM `/Root/TimestampTable`
                    WHERE `timestamp` > 0;
            )");
        }
    }

    Y_UNIT_TEST(MultiStatement) {
        TKikimrRunner kikimr(GetTestSettings());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test
                    WHERE Amount IN (
                        SELECT Amount FROM Test
                    );
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH can't be used with multiple writes or reads.", result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test
                    WHERE Amount > 100;
                SELECT 42;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH can't be used with multiple writes or reads.", result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test
                    WHERE Amount > 100;
                SELECT * FROM Test;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH can't be used with multiple writes or reads.", result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue
                    WHERE Key >= 3;
                UPSERT INTO KeyValue (Key, Value)
                    VALUES (10, "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH can't be used with multiple writes or reads.", result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue;
                UPSERT INTO KeyValue2 (Key, Value)
                    VALUES ("Key10", "Value10");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH can't be used with multiple writes or reads.", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(Returning) {
        TKikimrRunner kikimr(GetTestSettings());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue
                    WHERE Key >= 3
                    RETURNING *;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH DELETE is unsupported with RETURNING", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DeleteOn) {
        TKikimrRunner kikimr(GetTestSettings());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM Test ON (Group, Name)
                    VALUES (1u, "Anna");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH DELETE is unsupported with ON", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ColumnTable) {
        TKikimrRunner kikimr(GetTestSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                CREATE TABLE TestOlap (
                    Key Int32 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = COLUMN
                );
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Q_(R"(
                INSERT INTO TestOlap (Key, Value) VALUES
                    (1, "1"),
                    (2, "2"),
                    (3, "3");
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestOlap
                    WHERE Key < 3;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "BATCH operations are not supported for column tables at the current time.", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TableNotExists) {
        TKikimrRunner kikimr(GetTestSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestBatchNotExists;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Cannot find table 'db.[/Root/TestBatchNotExists]'", result.GetIssues().ToString());
        }
        {
            auto query = Q_(R"(
                BATCH DELETE FROM TestBatchNotExists
                    WHERE Key IN [1, 3, 5];
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Cannot find table 'db.[/Root/TestBatchNotExists]'", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(UnknownColumn) {
        TKikimrRunner kikimr(GetTestSettings());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            BATCH DELETE FROM Test
                WHERE UnknownColumn = 123;
        )");

        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Member not found: UnknownColumn", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(HasTxControl) {
        TKikimrRunner kikimr(GetTestSettings());
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue
                    WHERE Key >= 3;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH operation can be executed only in the implicit transaction mode.", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_QUAD(DisableFlags, UseSink, UseBatchUpdates) {
        TKikimrRunner kikimr(GetTestSettings(10000, 10, UseSink, UseBatchUpdates));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Q_(R"(
                BATCH DELETE FROM KeyValue
                    WHERE Key >= 3;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            if (UseSink && UseBatchUpdates) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH operations are not supported at the current time.", result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST_TWIN(TableWithSyncIndex, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(GetTestSettings(10000, 10, true, true, EnableIndexStreamWrite).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE global_sync_idx (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC ON (v1) COVER (v2)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                UPSERT INTO global_sync_idx (k, v1, v2, v3) VALUES
                    (1, "123", "456", "789"),
                    (2, "123", "456", "789"),
                    (3, "123", "456", "789"),
                    (4, "123", "456", "789"),
                    (5, "123", "456", "789");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                BATCH DELETE FROM global_sync_idx
                    WHERE v1 = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_sync_idx
                WHERE v1 = "123";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM `/Root/global_sync_idx/idx/indexImplTable`
                WHERE v1 = "123";
        )");
    }

    Y_UNIT_TEST_TWIN(TableWithUniqueSyncIndex, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(GetTestSettings(10000, 10, true, true, EnableIndexStreamWrite).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE global_unique_sync_idx (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL UNIQUE SYNC ON (v1) COVER (v2)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                UPSERT INTO global_unique_sync_idx (k, v1, v2, v3) VALUES
                    (1, "123", "456", "787"),
                    (2, "124", "456", "789"),
                    (3, "125", "457", "787"),
                    (4, "126", "457", "789"),
                    (5, "127", "458", "787");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                BATCH DELETE FROM global_unique_sync_idx WHERE v1 = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            if (EnableIndexStreamWrite) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "BATCH operations are not supported for tables with global sync unique secondary indexes (index: `idx`)", result.GetIssues().ToString());
            }
        }

        if (!EnableIndexStreamWrite) {
            return;
        }

        {
            const auto query = R"(
                BATCH DELETE FROM global_unique_sync_idx WHERE v2 = "456";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                BATCH DELETE FROM global_unique_sync_idx WHERE v3 = "787";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_unique_sync_idx WHERE v1 = "123";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_unique_sync_idx WHERE v2 = "456";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_unique_sync_idx WHERE v3 = "787";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM `/Root/global_unique_sync_idx/idx/indexImplTable` WHERE v1 = "123";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM `/Root/global_unique_sync_idx/idx/indexImplTable` WHERE v2 = "456";
        )");

        {
            const auto query = R"(
                BATCH DELETE FROM global_unique_sync_idx;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_unique_sync_idx;
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM `/Root/global_unique_sync_idx/idx/indexImplTable`;
        )");
    }

    Y_UNIT_TEST_TWIN(TableWithAsyncIndex, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(GetTestSettings(10000, 10, true, true, EnableIndexStreamWrite).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE global_async_idx (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL ASYNC ON (v1) COVER (v2)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                UPSERT INTO global_async_idx (k, v1, v2, v3) VALUES
                    (1, "123", "456", "789"),
                    (2, "123", "456", "789"),
                    (3, "123", "456", "789"),
                    (4, "123", "456", "789"),
                    (5, "123", "456", "789");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                BATCH DELETE FROM global_async_idx WHERE v1 = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM global_async_idx WHERE v1 = "123";
        )");

        ExecQueryAndTestEmpty(session, R"(
            SELECT count(*) FROM `/Root/global_async_idx/idx/indexImplTable` WHERE v1 = "123";
        )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx());
    }

    Y_UNIT_TEST(TableWithVectorIndex) {
        TKikimrRunner kikimr(GetTestSettings().SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE vector_idx (
                    k Int32 NOT NULL,
                    v String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC USING vector_kmeans_tree ON (v) WITH (
                        distance="cosine",
                        vector_type="uint8",
                        vector_dimension=3,
                        clusters=2,
                        levels=3
                    )
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                BATCH DELETE FROM vector_idx WHERE v = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "BATCH operations are not supported for tables with global sync vector_kmeans_tree indexes (index: `idx`)");
        }
    }

    Y_UNIT_TEST(TableWithFullTextIndex) {
        auto settings = GetTestSettings().SetWithSampleTables(false);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        settings.SetFeatureFlags(featureFlags);

        auto kikimr = TKikimrRunner(settings);

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE fulltext_plain_idx (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC USING fulltext_plain ON (v1) COVER (v2) WITH (
                        tokenizer=standard,
                        use_filter_lowercase=true
                    )
                );

                CREATE TABLE fulltext_relevance_idx (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC USING fulltext_relevance ON (v1) COVER (v2) WITH (
                        tokenizer=standard,
                        use_filter_lowercase=true
                    )
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = R"(
                BATCH DELETE FROM fulltext_plain_idx WHERE v1 = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "BATCH operations are not supported for tables with global sync fulltext_plain indexes (index: `idx`)");
        }
        {
            const auto query = R"(
                BATCH DELETE FROM fulltext_relevance_idx WHERE v1 = "123";
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "BATCH operations are not supported for tables with global sync fulltext_relevance indexes (index: `idx`)");
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
