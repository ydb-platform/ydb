#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/testing/unittest/registar.h>

#include <set>

namespace NKikimr::NKqp {

// The SchemaVersionCleaner repair (ColumnShardConfig.Repairs) removes schema versions that no portion uses. After it,
// the tablet must start and keep the schema, the data and the TTL of its tables.
Y_UNIT_TEST_SUITE(KqpOlapSchemaVersionCleaner) {

namespace {

class TCleanerTest {
private:
    TTestHelper TestHelper;

public:
    TCleanerTest()
        : TestHelper(TKikimrSettings().SetWithSampleTables(false)) {
    }

    TTestHelper& GetTestHelper() {
        return TestHelper;
    }

    NYdb::NQuery::TExecuteQueryResult Execute(const TString& query) {
        return TestHelper.GetKikimr().GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    }

    void ExecuteOk(const TString& query) {
        const auto result = Execute(query);
        UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
    }

    // A rebooted tablet needs some time before it answers again.
    TString WaitSelect(const TString& query) {
        TString lastError;
        for (ui32 i = 0; i < 60; ++i) {
            const auto result = Execute(query);
            if (result.IsSuccess()) {
                return NYdb::FormatResultSetYson(result.GetResultSet(0));
            }
            lastError = result.GetIssues().ToString();
            Sleep(TDuration::Seconds(1));
        }
        UNIT_FAIL(query << ": " << lastError);
        return {};
    }

    std::set<ui64> GetPortionIds(const TString& tablePath) {
        auto tableClient = TestHelper.GetKikimr().GetTableClient();
        auto rows = ExecuteScanQuery(tableClient,
            TStringBuilder() << "SELECT PortionId FROM `" << tablePath << "/.sys/primary_index_portion_stats`");
        std::set<ui64> result;
        for (auto&& row : rows) {
            result.emplace(GetUint64(row.at("PortionId")));
        }
        return result;
    }

    THashMap<ui64, ui64> GetColumnShardGenerations() {
        auto tableClient = TestHelper.GetKikimr().GetTableClient();
        auto rows = ExecuteScanQuery(tableClient,
            R"(SELECT TabletId, Generation FROM `/Root/.sys/hive_tablets` WHERE Type = "ColumnShard" AND State = "ReadyToWork")", false);
        THashMap<ui64, ui64> result;
        for (auto&& row : rows) {
            result.emplace(GetUint64(row.at("TabletId")), GetUint32(row.at("Generation")));
        }
        return result;
    }

    // Returns when every column shard runs in a new generation: its normalizers, the repair among them, are done
    // before the shard starts to answer queries.
    void Reboot(const TString& tablePath) {
        const auto before = GetColumnShardGenerations();
        UNIT_ASSERT(!before.empty());
        TestHelper.RebootTablets(tablePath);
        for (ui32 i = 0; i < 60; ++i) {
            const auto after = GetColumnShardGenerations();
            bool rebooted = after.size() == before.size();
            for (auto&& [tabletId, generation] : before) {
                const auto it = after.find(tabletId);
                rebooted = rebooted && it != after.end() && it->second > generation;
            }
            if (rebooted) {
                return;
            }
            Sleep(TDuration::Seconds(1));
        }
        UNIT_FAIL("column shards of " << tablePath << " were not rebooted");
    }

    void EnableCleaner() {
        auto* repair = TestHelper.GetRuntime().GetAppData().ColumnShardConfig.AddRepairs();
        repair->SetClassName("SchemaVersionCleaner");
        repair->SetDescription("Removing unused schema versions");
    }

    void RebootWithCleaner(const TString& tablePath) {
        EnableCleaner();
        Reboot(tablePath);
    }
};

}   // namespace

// V1 (id, a) is used by data, V2 adds b and has no data, V3 adds c. The repair removes V2 while V3 is stored as a diff
// against V2.
Y_UNIT_TEST(MiddleVersionRemovedKeepsColumns) {
    TCleanerTest test;
    const TString path = "/Root/ColumnTable";
    test.ExecuteOk(TStringBuilder() << R"(
        CREATE TABLE `)" << path << R"(` (
            id Uint64 NOT NULL,
            a Int32,
            PRIMARY KEY (id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, a) VALUES (1, 10), (2, 20);");
    std::set<ui64> portionsV1;
    for (ui32 i = 0; i < 30 && portionsV1.empty(); ++i) {
        portionsV1 = test.GetPortionIds(path);
        if (portionsV1.empty()) {
            Sleep(TDuration::Seconds(1));
        }
    }
    UNIT_ASSERT_C(!portionsV1.empty(), "the first schema version has to be used by data");

    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` ADD COLUMN b Int32;");
    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` ADD COLUMN c Int32;");

    // Control: a plain reboot keeps everything, and the data is still in the portions of the first version.
    test.Reboot(path);
    CompareYson(R"([[1u;[10];#;#];[2u;[20];#;#]])", test.WaitSelect(TStringBuilder() << "SELECT id, a, b, c FROM `" << path << "` ORDER BY id;"));
    UNIT_ASSERT_C(test.GetPortionIds(path) == portionsV1, "the portions of the first version have to stay");

    test.RebootWithCleaner(path);
    test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;");
    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, a, b, c) VALUES (3, 30, 300, 3000);");
    CompareYson(R"([[1u;[10];#;#];[2u;[20];#;#];[3u;[30];[300];[3000]]])",
        test.WaitSelect(TStringBuilder() << "SELECT id, a, b, c FROM `" << path << "` ORDER BY id;"));
}

// A TTL-only ALTER of a standalone table stores a table version without a schema version at the same key.
Y_UNIT_TEST(StandaloneTtlOnlyAlter) {
    TCleanerTest test;
    const TString path = "/Root/ColumnTable";
    test.ExecuteOk(TStringBuilder() << R"(
        CREATE TABLE `)" << path << R"(` (
            id Uint64 NOT NULL,
            ts Timestamp NOT NULL,
            PRIMARY KEY (ts, id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, ts) VALUES (1, CurrentUtcTimestamp()), (2, CurrentUtcTimestamp());");
    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` SET (TTL = Interval(\"P3650D\") ON ts);");

    test.Reboot(path);
    CompareYson(R"([[2u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;"));

    test.RebootWithCleaner(path);
    CompareYson(R"([[2u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;"));
}

// An ALTER TABLE of a table in a table store sends the unchanged preset again: the shard stores a second row with the
// same schema version.
Y_UNIT_TEST(InStoreTableAlter) {
    TCleanerTest test;
    const TString path = "/Root/TableStore/InStoreTable";
    test.ExecuteOk(R"(
        CREATE TABLESTORE `/Root/TableStore` (
            id Uint64 NOT NULL,
            ts Timestamp NOT NULL,
            PRIMARY KEY (ts, id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    test.ExecuteOk(TStringBuilder() << R"(
        CREATE TABLE `)" << path << R"(` (
            id Uint64 NOT NULL,
            ts Timestamp NOT NULL,
            PRIMARY KEY (ts, id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, ts) VALUES (1, CurrentUtcTimestamp()), (2, CurrentUtcTimestamp());");
    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` SET (TTL = Interval(\"P3650D\") ON ts);");

    test.Reboot(path);
    CompareYson(R"([[2u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;"));

    test.RebootWithCleaner(path);
    CompareYson(R"([[2u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;"));
}

// A second table of a table store created on a shard that already has the preset stores a table version without a
// schema version at the same key.
Y_UNIT_TEST(InStoreSecondTable) {
    TCleanerTest test;
    test.ExecuteOk(R"(
        CREATE TABLESTORE `/Root/TableStore` (
            id Uint64 NOT NULL,
            ts Timestamp NOT NULL,
            PRIMARY KEY (ts, id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    for (const TString name : { "First", "Second" }) {
        test.ExecuteOk(TStringBuilder() << R"(
            CREATE TABLE `/Root/TableStore/)" << name << R"(` (
                id Uint64 NOT NULL,
                ts Timestamp NOT NULL,
                PRIMARY KEY (ts, id)
            ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )");
        test.ExecuteOk(TStringBuilder() << "UPSERT INTO `/Root/TableStore/" << name << "` (id, ts) VALUES (1, CurrentUtcTimestamp());");
    }

    test.Reboot("/Root/TableStore/First");
    CompareYson(R"([[1u]])", test.WaitSelect("SELECT COUNT(*) FROM `/Root/TableStore/Second`;"));

    test.RebootWithCleaner("/Root/TableStore/First");
    CompareYson(R"([[1u]])", test.WaitSelect("SELECT COUNT(*) FROM `/Root/TableStore/First`;"));
    CompareYson(R"([[1u]])", test.WaitSelect("SELECT COUNT(*) FROM `/Root/TableStore/Second`;"));
}

// TTL is set together with a schema change (V2, no data), then another schema change without TTL (V3). The repair
// removes V2 together with the table version that holds the TTL.
Y_UNIT_TEST_TWIN(TtlSetWithRemovedVersionStillApplies, WithCleaner) {
    auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
    csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    csController->SetOverrideTasksActualizationLag(TDuration::Seconds(1));

    TCleanerTest test;
    const TString path = "/Root/ColumnTable";
    test.ExecuteOk(TStringBuilder() << R"(
        CREATE TABLE `)" << path << R"(` (
            id Uint64 NOT NULL,
            ts Timestamp NOT NULL,
            PRIMARY KEY (ts, id)
        ) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
    )");
    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, ts) VALUES (1, CurrentUtcTimestamp());");
    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` ADD COLUMN b Int32, SET (TTL = Interval(\"P1D\") ON ts);");
    test.ExecuteOk(TStringBuilder() << "ALTER TABLE `" << path << "` ADD COLUMN c Int32;");

    if (WithCleaner) {
        test.RebootWithCleaner(path);
    } else {
        test.Reboot(path);
    }
    CompareYson(R"([[1u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "`;"));

    test.ExecuteOk(TStringBuilder() << "UPSERT INTO `" << path << "` (id, ts) VALUES (2, Timestamp(\"2000-01-01T00:00:00Z\"));");
    csController->WaitCondition(TDuration::Seconds(120), [&]() {
        return test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "` WHERE id = 2;") == "[[0u]]";
    });
    CompareYson(R"([[0u]])", test.WaitSelect(TStringBuilder() << "SELECT COUNT(*) FROM `" << path << "` WHERE id = 2;"),
        "the expired row has to be deleted by TTL");
}

}

}   // namespace NKikimr::NKqp
