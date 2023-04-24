#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/core/tx/datashard/datashard_failpoints.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/json/json_prettifier.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NScripting;

Y_UNIT_TEST_SUITE(KqpScripting) {
    Y_UNIT_TEST(EndOfQueryCommit) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            CREATE TABLE `/Root/ScriptingTest` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);

        result = client.ExecuteYqlScript(R"(
            SELECT COUNT(*) FROM `/Root/ScriptingTest`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 1);
        TResultSetParser rs0(result.GetResultSets()[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rs0.ColumnParser(0).GetUint64(), 2u);
    }

    Y_UNIT_TEST(UnsafeTimestampCast) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpYqlSyntaxVersion");
        setting.SetValue("0");

        TKikimrRunner kikimr({setting});
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(Q_(R"(
            CREATE TABLE `/Root/TsTest` (
                Key Timestamp,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            UPSERT INTO `/Root/TsTest`
            SELECT * FROM `/Root/KeyValue`;
        )")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = client.ExecuteYqlScript(Q1_(R"(
            UPSERT INTO `/Root/TsTest`
            SELECT * FROM `/Root/KeyValue`;
        )")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ScanQuery) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto params = client.GetParamsBuilder()
            .AddParam("$text").String("Value1").Build()
            .Build();

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA db.ScanQuery = "true";
            DECLARE $text AS String;
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = $text;
        )", params).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        TResultSetParser rs0(result.GetResultSets()[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rs0.ColumnParser(0).GetUint64(), 8u);
    }

    Y_UNIT_TEST(ScanQueryInvalid) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/EightShard`;
            SELECT COUNT(*) FROM `/Root/TwoShard`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = client.ExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            UPSERT INTO `/Root/KeyValue`
            SELECT Key, Text AS Value FROM `/Root/EightShard`;

            SELECT * FROM `/Root/EightShard`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ScanQueryDisable) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA ydb.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value1";
            COMMIT;

            PRAGMA kikimr.ScanQuery = default;
            SELECT COUNT(*) FROM `/Root/EightShard`;
            SELECT COUNT(*) FROM `/Root/TwoShard`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
    }

    Y_UNIT_TEST(ScanQueryTruncate) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_ResultRowsLimit");
        setting.SetValue("5");

        TKikimrRunner kikimr({setting});
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA ydb.ScanQuery = "true";
            SELECT * FROM `/Root/EightShard`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT(result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
    }

    Y_UNIT_TEST(LimitOnShard) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        NYdb::NScripting::TExecuteYqlRequestSettings execSettings;
        execSettings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/KeyValue`  WHERE Key > 0 ORDER BY Key LIMIT 1;
        )", execSettings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/KeyValue");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
    }

    Y_UNIT_TEST(QueryStats) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        NYdb::NScripting::TExecuteYqlRequestSettings execSettings;
        execSettings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "false";
            SELECT COUNT(*) FROM `/Root/EightShard`;
            COMMIT;
            SELECT COUNT(*) FROM `/Root/TwoShard`;
            COMMIT;
            PRAGMA kikimr.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/KeyValue`;
        )", execSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);

        UNIT_ASSERT(result.GetStats());
        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        uint64_t totalCpuTimeUs = 0;

        UNIT_ASSERT(stats.process_cpu_time_us() > 0);
        UNIT_ASSERT(stats.total_duration_us() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 4);
        ui32 phaseNo = 0;

        totalCpuTimeUs += stats.process_cpu_time_us();

        for (auto& phase : stats.query_phases()) {
            if (phaseNo++ == 3) {
                UNIT_ASSERT_VALUES_EQUAL(phase.table_access().size(), 0);
                UNIT_ASSERT(phase.cpu_time_us() > 0);
                UNIT_ASSERT(phase.affected_shards() == 0);
                totalCpuTimeUs += phase.cpu_time_us();
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(phase.table_access().size(), 1);
            UNIT_ASSERT(phase.table_access(0).partitions_count() > 0);
            UNIT_ASSERT(phase.table_access(0).reads().rows() > 0);
            UNIT_ASSERT(phase.table_access(0).reads().bytes() > 0);
            UNIT_ASSERT(phase.cpu_time_us() > 0);
            UNIT_ASSERT(phase.affected_shards() > 0);
            totalCpuTimeUs += phase.cpu_time_us();
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.total_cpu_time_us(), totalCpuTimeUs);
    }

    Y_UNIT_TEST(SystemTables) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/.sys/partition_stats`;
            COMMIT;
            SELECT * FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);

        result = client.ExecuteYqlScript(R"(
            SELECT *
            FROM `/Root/TwoShard` AS ts
            JOIN `/Root/.sys/partition_stats`AS ps
            ON ts.Key = ps.PartIdx;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Pure) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            SELECT 1 + 1;
        )").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        TResultSetParser rs0(result.GetResultSets()[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rs0.ColumnParser(0).GetInt32(), 2u);
    }

    Y_UNIT_TEST(NoAstSizeLimit) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetQueryLimitBytes(200);

        TKikimrRunner kikimr(TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false));
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());


        result = client.ExecuteYqlScript(R"(
            REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                (1u, "One"),
                (2u, "Two");
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ScriptExplainCreatedTable) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        TExplainYqlRequestSettings settings;
        settings.Mode(ExplainYqlRequestMode::Plan);

        {
            auto result = client.ExecuteYqlScript(R"(
                CREATE TABLE `/Root/ScriptingTest` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                COMMIT;
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto result = client.ExplainYqlScript(R"(
            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (3, "Three"),
                (4, "Four");
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT 1*2*3*4*5;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT 1*2*3*4*5;
            COMMIT;
        )", settings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto planJson = result.GetPlan();

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(planJson, &plan, true);
        UNIT_ASSERT_EQUAL(plan.GetMapSafe().at("queries").GetArraySafe().size(), 8);
    }


    Y_UNIT_TEST(ScriptExplain) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        TExplainYqlRequestSettings settings;
        settings.Mode(ExplainYqlRequestMode::Plan);

        auto result = client.ExplainYqlScript(R"(
            CREATE TABLE `/Root/ScriptingTest` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (3, "Three"),
                (4, "Four");
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT 1*2*3*4*5;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT 1*2*3*4*5;
            COMMIT;
        )", settings).GetValueSync();
        // KIKIMR-15083
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());

        /*
        auto planJson = result.GetPlan();

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(planJson, &plan, true);
        UNIT_ASSERT_EQUAL(plan.GetMapSafe().at("queries").GetArraySafe().size(), 8);
        */
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto res = session.DescribeTable("/Root/ScriptingTest").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(ScriptValidate) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        TExplainYqlRequestSettings settings;
        settings.Mode(ExplainYqlRequestMode::Validate);

        auto result = client.ExplainYqlScript(R"(
            DECLARE $value1 as Utf8;
            DECLARE $value2 as UInt32;
            SELECT $value1 as value1, $value2 as value2;
        )", settings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto paramTypes = result.GetParameterTypes();
        UNIT_ASSERT_VALUES_EQUAL(paramTypes.size(), 2);

        UNIT_ASSERT(paramTypes.find("$value1") != paramTypes.end());
        TType type1 = paramTypes.at("$value1");
        TTypeParser parser1(type1);
        UNIT_ASSERT_EQUAL(parser1.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_EQUAL(parser1.GetPrimitive(), EPrimitiveType::Utf8);

        UNIT_ASSERT(paramTypes.find("$value2") != paramTypes.end());
        TType type2 = paramTypes.at("$value2");
        TTypeParser parser2(type2);
        UNIT_ASSERT_EQUAL(parser2.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_EQUAL(parser2.GetPrimitive(), EPrimitiveType::Uint32);

        UNIT_ASSERT_EQUAL(result.GetPlan(), "");
    }

    Y_UNIT_TEST(ScriptStats) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        TExecuteYqlRequestSettings settings;
        settings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Full);

        auto result = client.ExecuteYqlScript(R"(
            CREATE TABLE `/Root/ScriptingTest` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            REPLACE INTO `/Root/ScriptingTest` (Key, Value) VALUES
                (3, "Three"),
                (4, "Four");
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT count(*) FROM `/Root/ScriptingTest`;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT 1*2*3*4*5;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT 1*2*3*4*5;
            COMMIT;
        )", settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 6);

        auto stats = result.GetStats().Get();
        auto planJson = NYdb::TProtoAccessor::GetProto(*stats).query_plan();

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(planJson, &plan, true);
        auto node = FindPlanNodeByKv(plan.GetMap().at("queries").GetArray()[2], "Node Type", "Aggregate-TableFullScan");
        UNIT_ASSERT_EQUAL(node.GetMap().at("Stats").GetMapSafe().at("TotalTasks").GetIntegerSafe(), 1);
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptScan) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value1";
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[8u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptScanCancelation) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);

        {
            auto it = client.StreamExecuteYqlScript(R"(
                PRAGMA kikimr.ScanQuery = "true";
                SELECT * FROM `/Root/EightShard` WHERE Text = "Value1";
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            // We must wait execution to be started
            int count = 60;
            while (counters.GetActiveSessionActors()->Val() != 2 && count) {
                count--;
                Sleep(TDuration::Seconds(1));
            }

            UNIT_ASSERT_C(count, "Unable to wait second session actor (executing compiled program) start");
        }

        NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        int count = 60;
        while (counters.GetActiveSessionActors()->Val() != 1 && count) {
            count--;
            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_C(count, "Unable to wait for proper active session count, it looks like cancelation doesn`t work");
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptScanScalar) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            $key1 = (SELECT Fk21 FROM `/Root/Join1` WHERE Key = 1);
            $key2 = (SELECT Fk21 FROM `/Root/Join1` WHERE Key = 2);
            $limit = (SELECT Key FROM `/Root/Join1` WHERE Fk21 = 105);

            SELECT Data FROM `/Root/EightShard` WHERE Key = $key1 OR Key = $key2 LIMIT COALESCE($limit, 1u);
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[[1]];[[3]]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(ExecuteYqlScriptScanScalar) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        NYdb::NScripting::TExecuteYqlRequestSettings execSettings;
        execSettings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            $key1 = (SELECT Fk21 FROM `/Root/Join1` WHERE Key = 1);
            $key2 = (SELECT Fk21 FROM `/Root/Join1` WHERE Key = 2);
            $limit = (SELECT Key FROM `/Root/Join1` WHERE Fk21 = 105);

            SELECT Data FROM `/Root/EightShard` WHERE Key = $key1 OR Key = $key2 LIMIT COALESCE($limit, 1u);
        )", execSettings).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[[1]];[[3]]])", FormatResultSetYson(result.GetResultSet(0)));

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        for (const auto& phase : stats.query_phases()) {
            if (phase.table_access().size()) {
                if (phase.table_access(0).name() == "/Root/EightShard") {
                    UNIT_ASSERT_VALUES_EQUAL(phase.table_access(0).reads().rows(), 2);
                }
            }
        }
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptData) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value1";
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[8u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptMixed) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            SELECT Key FROM `/Root/EightShard` WHERE Text = "Value1"
            ORDER BY Key;
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value1";
            COMMIT;

            PRAGMA kikimr.ScanQuery = "false";
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value2";
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value3";
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([
            [[[101u]];[[201u]];[[301u]];[[401u]];[[501u]];[[601u]];[[701u]];[[801u]]];
            [[8u]];
            [[8u]];
            [[8u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptLeadingEmptyScan) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "true";
            SELECT Key FROM `/Root/EightShard` WHERE Text = "Value123";
            COMMIT;

            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Text = "Value1";
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[];[[8u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptSeveralQueries) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            --!syntax_v1
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 1;
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 2;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[[101u]]];[[[102u]]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptSeveralQueriesComplex) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            --!syntax_v1
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 1;
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 2;
            COMMIT;

            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 3;
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 4;
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 5;

        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        TString res = StreamResultToYson(it);
        Cerr << "Result: " << res << Endl;
        CompareYson(R"([[[[101u]]];[[[102u]]];[[[103u]]];[[[104u]]];[[[105u]]]])", res);
    }

    Y_UNIT_TEST(SyncExecuteYqlScriptSeveralQueries) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 1;
            SELECT Fk21 FROM `/Root/Join1` WHERE Key = 2;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 1);
        TResultSetParser rs0(result.GetResultSets()[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*rs0.ColumnParser(0).GetOptionalUint32().Get(), 101u);
        TResultSetParser rs1(result.GetResultSets()[1]);
        UNIT_ASSERT(rs1.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*rs1.ColumnParser(0).GetOptionalUint32().Get(), 102u);
    }

    Y_UNIT_TEST(StreamExecuteYqlScriptEmptyResults) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            SELECT Key FROM `/Root/EightShard` WHERE Text = "Value";
            COMMIT;

            PRAGMA kikimr.ScanQuery = "true";
            SELECT Key FROM `/Root/EightShard` WHERE Text = "Value5";
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[];[]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamDdlAndDml) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());
        {
            auto it = client.StreamExecuteYqlScript(R"(
                CREATE TABLE `/Root/StreamScriptingTest` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                COMMIT;

                REPLACE INTO `/Root/StreamScriptingTest` (Key, Value) VALUES
                    (1, "One"),
                    (2, "Two");
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            {
                auto streamPart = it.ReadNext().GetValueSync();
                UNIT_ASSERT_C(streamPart.IsSuccess(), streamPart.GetIssues().ToString());
                UNIT_ASSERT_C(!streamPart.HasPartialResult(), streamPart.GetIssues().ToString());
            }
            {
                auto streamPart = it.ReadNext().GetValueSync();
                UNIT_ASSERT_C(!streamPart.IsSuccess(), streamPart.GetIssues().ToString());
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            }
        }

        auto it = client.StreamExecuteYqlScript(R"(
            SELECT COUNT(*) FROM `/Root/StreamScriptingTest`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[2u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamOperationTimeout) {
        TKikimrRunner kikimr;
        TScriptingClient client(kikimr.GetDriver());

        TExecuteYqlRequestSettings settings;
        settings.OperationTimeout(TDuration::MilliSeconds(1));
        auto it = client.StreamExecuteYqlScript(R"(
            SELECT Key FROM `/Root/EightShard` WHERE Text = "Value1";
        )", settings).GetValueSync();

        auto streamPart = it.ReadNext().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), EStatus::TIMEOUT, it.GetIssues().ToString());
    }

    Y_UNIT_TEST(SecondaryIndexes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        TScriptingClient client(kikimr.GetDriver());
        auto it = client.StreamExecuteYqlScript(R"(
            --!syntax_v1

            SELECT Value
            FROM `/Root/SecondaryKeys` VIEW Index
            WHERE Fk = 5;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[["Payload5"]]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(JoinIndexLookup) {
        TKikimrRunner kikimr;

        auto settings = TExecuteYqlRequestSettings()
            .CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1

            SELECT Key, Value2 FROM Join1 AS j1
            JOIN Join2 AS j2
            ON j1.Fk21 = j2.Key1 AND j1.Fk22 = j2.Key2
            WHERE Key = 1;
        )", settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        UNIT_ASSERT(result.GetStats());
        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        for (auto& phase : stats.query_phases()) {
            for (auto& table : phase.table_access()) {
                if (table.has_reads()) {
                    UNIT_ASSERT_VALUES_EQUAL(table.reads().rows(), 1);
                }
            }
        }

        CompareYson(R"([
            [[1];["Value21"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
