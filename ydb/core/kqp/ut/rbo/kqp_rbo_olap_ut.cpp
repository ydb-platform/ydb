#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/aggregation.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <util/system/env.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <ctime>
#include <regex>
#include <fstream>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NStat;
}

namespace NKikimr {
namespace NKqp {
using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRboOlap) {

    void CreateTableOfAllTypes(TKikimrRunner& kikimr) {
        auto& legacyClient = kikimr.GetTestClient();

        legacyClient.CreateOlapStore("/Root", R"(
                                     Name: "olapStore"
                                     ColumnShardCount: 1
                                     SchemaPresets {
                                         Name: "default"
                                         Schema {
                                             Columns { Name: "key" Type: "Int32" NotNull: true }
                                             #Columns { Name: "Bool_column" Type: "Bool" }
                                             # Int8, Int16, UInt8, UInt16 is not supported by engine
                                             Columns { Name: "Int8_column" Type: "Int32" }
                                             Columns { Name: "Int16_column" Type: "Int32" }
                                             Columns { Name: "Int32_column" Type: "Int32" }
                                             Columns { Name: "Int64_column" Type: "Int64" }
                                             Columns { Name: "UInt8_column" Type: "Uint32" }
                                             Columns { Name: "UInt16_column" Type: "Uint32" }
                                             Columns { Name: "UInt32_column" Type: "Uint32" }
                                             Columns { Name: "UInt64_column" Type: "Uint64" }
                                             Columns { Name: "Double_column" Type: "Double" }
                                             Columns { Name: "Float_column" Type: "Float" }
                                             #Columns { Name: "Decimal_column" Type: "Decimal" }
                                             Columns { Name: "String_column" Type: "String" }
                                             Columns { Name: "Utf8_column" Type: "Utf8" }
                                             Columns { Name: "Json_column" Type: "Json" }
                                             Columns { Name: "Yson_column" Type: "Yson" }
                                             Columns { Name: "Timestamp_column" Type: "Timestamp" }
                                             Columns { Name: "Date_column" Type: "Date" }
                                             Columns { Name: "Datetime_column" Type: "Datetime" }
                                             #Columns { Name: "Interval_column" Type: "Interval" }
                                             KeyColumnNames: "key"
                                         }
                                     }
        )");

        legacyClient.CreateColumnTable("/Root/olapStore", R"(
            Name: "OlapParametersTable"
            ColumnShardCount: 1
        )");
        legacyClient.Ls("/Root");
        legacyClient.Ls("/Root/olapStore");
        legacyClient.Ls("/Root/olapStore/OlapParametersTable");
    }

    std::map<std::string, TParams> CreateParametersOfAllTypes(NYdb::NTable::TTableClient& tableClient) {
         return {
#if 0
            {
                "Bool",
                tableClient.GetParamsBuilder().AddParam("$in_value").Bool(false).Build().Build()
            },
#endif
            {
                "Int8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int8(0).Build().Build()
            },
            {
                "Int16",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int16(0).Build().Build()
            },
            {
                "Int32",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int32(0).Build().Build()
            },
            {
                "Int64",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int64(0).Build().Build()
            },
            {
                "UInt8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint8(0).Build().Build()
            },
            {
                "UInt16",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint16(0).Build().Build()
            },
            {
                "UInt32",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint32(0).Build().Build()
            },
            {
                "UInt64",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint64(0).Build().Build()
            },
            {
                "Float",
                tableClient.GetParamsBuilder().AddParam("$in_value").Float(0).Build().Build()
            },
            {
                "Double",
                tableClient.GetParamsBuilder().AddParam("$in_value").Double(0).Build().Build()
            },
            {
                "String",
                tableClient.GetParamsBuilder().AddParam("$in_value").String("XX").Build().Build()
            },
            {
                "Utf8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Utf8("XX").Build().Build()
            },
            {
                "Timestamp",
                tableClient.GetParamsBuilder().AddParam("$in_value").Timestamp(TInstant::Now()).Build().Build()
            },
            {
                "Date",
                tableClient.GetParamsBuilder().AddParam("$in_value").Date(TInstant::Now()).Build().Build()
            },
            {
                "Datetime",
                tableClient.GetParamsBuilder().AddParam("$in_value").Datetime(TInstant::Now()).Build().Build()
            },
#if 0
            {
                "Interval",
                tableClient.GetParamsBuilder().AddParam("$in_value").Interval(1010).Build().Build()
            },
            {
                "Decimal(12,9)",
                tableClient.GetParamsBuilder().AddParam("$in_value").Decimal(TDecimalValue("10.123456789", 12, 9)).Build().Build()
            },
            {
                "Json",
                tableClient.GetParamsBuilder().AddParam("$in_value").Json(R"({"XX":"YY"})").Build().Build()
            },
            {
                "Yson",
                tableClient.GetParamsBuilder().AddParam("$in_value").Yson("[[[]]]").Build().Build()
            },
#endif
        };
    }

    TString BuildQuery(const TString& predicate, bool pushEnabled) {
        TStringBuilder qBuilder;
        qBuilder << "PRAGMA Kikimr.OptEnableOlapPushdown = '" << (pushEnabled ? "true" : "false") << "';" << Endl;
        qBuilder << "SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE ";
        qBuilder << predicate;
        qBuilder << " ORDER BY `timestamp`";
        return qBuilder;
    };

    Y_UNIT_TEST(PredicatePushdown) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, true);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> testData = {
            R"(`resource_id` != "10001" XOR "XXX" == "YYY")",
            R"(`resource_id` = `uid`)",
            R"(`resource_id` != `uid`)",
            R"(`resource_id` = "10001")",
            R"(`resource_id` != "10001")",
            R"("XXX" == "YYY" OR `resource_id` != "10001")",
            R"(`level` = 1)",
            R"(`level` = Int8("1"))",
            R"(`level` = Int16("1"))",
            R"(`level` = Int32("1"))",
            R"(`level` > Int32("3"))",
            R"(`level` < Int32("1"))",
            R"(`level` >= Int32("4"))",
            R"(`level` <= Int32("0"))",
            R"(`level` != Int32("0"))",
            R"(`level` + `level` <= Int32("0"))",
            R"(`level` <= `level`)",
            R"((`level`, `uid`, `resource_id`) = (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) > (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) > (Int32("1"), "uid_3000000", "10001"))",
            R"((`level`, `uid`, `resource_id`) < (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("2"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000001", "10002"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("2"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000001", "10002"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) != (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) != (Int32("0"), "uid_3000001", "10011"))",
            R"(`level` = 0 OR `level` = 2 OR `level` = 1)",
            R"(`level` = 0 OR (`level` = 2 AND `uid` = "uid_3000002"))",
            R"(`level` = 0 OR NOT(`level` = 2 AND `uid` = "uid_3000002"))",
            R"(`level` = 0 AND (`uid` = "uid_3000000" OR `uid` = "uid_3000002"))",
            R"(`level` = 0 AND NOT(`uid` = "uid_3000000" OR `uid` = "uid_3000002"))",
            R"(`level` = 0 OR `uid` = "uid_3000003")",
            R"(`level` = 0 AND `uid` = "uid_3000003")",
            R"(`level` = 0 AND `uid` = "uid_3000000")",
            R"((`level`, `uid`) > (Int32("2"), "uid_3000004") OR (`level`, `uid`) < (Int32("1"), "uid_3000002"))",
            R"(Int32("3") > `level`)",
            R"((Int32("1"), "uid_3000001", "10001") = (`level`, `uid`, `resource_id`))",
            //R"((Int32("1"), `uid`, "10001") = (`level`, "uid_3000001", `resource_id`))",
            R"(`level` = 0 AND "uid_3000000" = `uid`)",
            R"(`uid` > `resource_id`)",
            R"(`level` IS NULL)",
            R"(`level` IS NOT NULL)",
            R"(`message` IS NULL)",
            R"(`message` IS NOT NULL)",
            R"((`level`, `uid`) > (Int32("1"), NULL))",
            R"((`level`, `uid`) != (Int32("1"), NULL))",
            R"(`level` >= CAST("2" As Int32))",
            R"(CAST("2" As Int32) >= `level`)",
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
            R"(`level` + 2 < 5)",
            R"(`level` - 2 >= 1)",
            R"(`level` * 3 > 4)",
            R"(`level` / 2 <= 1)",
            R"(`level` % 3 != 1)",
            R"(-`level` < -2)",
            R"(Abs(`level` - 3) >= 1)",
            R"(LENGTH(`message`) > 1037)",
            R"(LENGTH(`uid`) > 1 OR `resource_id` = "10001")",
            R"((LENGTH(`uid`) > 2 AND `resource_id` = "10001") OR `resource_id` = "10002")",
            R"((LENGTH(`uid`) > 3 OR `resource_id` = "10002") AND (LENGTH(`uid`) < 15 OR `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 AND `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 OR `resource_id` = "10001"))",
            R"(`level` IS NULL OR `message` IS NULL)",
            R"(`level` IS NOT NULL AND `message` IS NULL)",
            R"(`level` IS NULL AND `message` IS NOT NULL)",
            R"(`level` IS NOT NULL AND `message` IS NOT NULL)",
            R"(`level` IS NULL XOR `message` IS NOT NULL)",
            R"(`level` IS NULL XOR `message` IS NULL)",
            R"(`level` + 2. < 5.f)",
            R"(`level` - 2.f >= 1.)",
            R"(`level` * 3. > 4.f)",
            R"(`level` / 2.f <= 1.)",
            R"(`level` % 3. != 1.f)",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:03.000001Z") AND `level` < 4)",
            R"(IF(`level` > 3, -`level`, +`level`) < 2)",
            R"(StartsWith(`message` ?? `resource_id`, "10000"))",
            R"(NOT EndsWith(`message` ?? `resource_id`, "xxx"))",
            // Do not work even without olap pushdown.
            //R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) == <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
            //R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) != <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
            R"(`uid` LIKE "_id%000_")",
            R"(`uid` ILIKE "UID%002")",

            R"(Udf(String::_yql_AsciiEqualsIgnoreCase)(`uid`,  "UI"))",
            R"(Udf(String::Contains)(`uid`,  "UI"))",
            R"(Udf(String::_yql_AsciiContainsIgnoreCase)(`uid`,  "UI"))",
            R"(Udf(String::StartsWith)(`uid`,  "UI"))",
            R"(Udf(String::_yql_AsciiStartsWithIgnoreCase)(`uid`,  "UI"))",
            R"(Udf(String::EndsWith)(`uid`,  "UI"))",
            R"(Udf(String::_yql_AsciiEndsWithIgnoreCase)(`uid`,  "UI"))",
        };

        for (const auto& predicate: testData) {
            auto normalQuery = BuildQuery(predicate, false);
            auto pushQuery = BuildQuery(predicate, true);

            Cerr << "--- Run normal query ---\n";
            Cerr << normalQuery << Endl;
            auto it = tableClient.StreamExecuteScanQuery(normalQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto goodResult = CollectStreamResult(it);

            Cerr << "--- Run pushed down query ---\n";
            Cerr << pushQuery << Endl;
            it = tableClient.StreamExecuteScanQuery(pushQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto pushResult = CollectStreamResult(it);

            if (logQueries) {
                Cerr << "Query: " << normalQuery << Endl;
                Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
                Cerr << "Received: " << pushResult.ResultSetYson << Endl;
            }

            CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

            it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);
        }
    }

    // Unit tests for datetime pushdowns in query service
    Y_UNIT_TEST(PredicatePushdown_Datetime_QS) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session2 = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,
                dt      Date,
                dt32    Date32,
                dtm     DateTime,
                dtm64   DateTime64,
                ts      Timestamp,
                ts64    Timestamp64,
                --inter  Interval, -- NOT SUPPORTED?
                inter64  Interval64,
                primary key(id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        std::vector<TString> testData = {
            // TPC-H Datetime predicates. Commented out predicates currently fail, need to be fixed
            // TPCH Q1:
            R"(CAST(dt AS Timestamp) <= (CAST('1998-12-01' AS Date) - Interval("P100D")))",
            R"(CAST(dt AS Timestamp64) <= (CAST('1998-12-01' AS Date) - Interval("P100D")))",

            R"(CAST(dt32 AS Timestamp) <= (CAST('1998-12-01' AS Date) - Interval("P100D")))",
            R"(CAST(dt32 AS Timestamp) <= (CAST('1998-12-01' AS Date32) - Interval("P100D")))",
            R"(CAST(dt32 AS Timestamp) <= (CAST('1998-12-01' AS Date32) - Interval64("P100D")))",
            R"(CAST(dt32 AS Timestamp64) <= (CAST('1998-12-01' AS Date32) - Interval64("P100D")))",

            // TPCH Q6:
            R"(cast(dt as Timestamp) < (Date("1995-01-01") + Interval("P365D")))",

            // Other tests:

            R"(dt <= (CAST('1998-12-01' AS Date) - Interval("P100D")))",
            R"(dt32 <= (CAST('1998-12-01' AS Date) - Interval("P100D")))",
            R"(dt <= (CAST('1998-12-01' AS Date32) - Interval64("P100D")))",

            R"(CAST(dt as Timestamp) <= dt - inter64)",
            R"(CAST(dt as Timestamp64) <= dt - inter64)",
            R"(CAST(dt as Timestamp64) <= dt32 - inter64)",
            R"(dt <= dt - inter64)",
            R"(dt32 <= dt - inter64)",
            R"(CAST(dt32 as Date) <= dt - inter64)",
            R"(dt <= dt - CAST(inter64 as Interval))",
            R"(dt32 <= dt32 - inter64)",
            R"(dt32 <= ts64 - inter64)",

            R"(dt <= CAST('2001-01-01' as Date))",
            R"(dt <= Date('2001-01-01'))",

            R"((`dt`, `id`) >= (CAST('1998-12-01' AS Date) - Interval64("P100D"), 3))",
            R"((`ts`, `id`) >= (Timestamp("1970-01-01T00:00:03.000001Z"), 3))",
            R"((`dt32`, `id`) >= (CAST('1998-12-01' AS Date32), 3))",
            R"((`dtm`, `id`) >= (CAST('1998-12-01' AS DateTime), 3))",
            R"((`dtm64`, `id`) >= (CAST('1998-12-01' AS DateTime64), 3))",
            R"((`ts64`, `id`) >= (Timestamp("1970-01-01T00:00:03.000001Z"), 3))",
            R"(dt >= dt)",
        };

        std::vector<TString> testDataBlocks = {
            // 1. Compare
            // 1.1 Column and Constant expr
            R"(dt <= Date('2001-01-01'))",
            R"(dt32 <= Date32('2001-01-01'))",
            R"(dtm >= DateTime('1998-12-01T15:30:00Z'))",
            R"(dtm64 >= DateTime64('1998-12-01T15:30:00Z'))",
            R"(ts64 >= Timestamp64("1970-01-01T00:00:03.000001Z"))",
            R"(ts >= Timestamp("1970-01-01T00:00:03.000001Z"))",
            R"(inter64 >= Interval64("P100D"))",

            // Right side simplified and `just` added.
            R"(dt <= (Date('2001-01-01') - Interval("P100D")))",
            R"(dt32 <= (Date32('2001-01-01') - Interval("P100D")))",
            R"(dtm <= (DateTime('1998-12-01T15:30:00Z') - Interval("P100D")))",
            R"(dtm64 <= (DateTime64('1998-12-01T15:30:00Z') - Interval("P100D")))",
            R"(ts64 >= (Timestamp64("1970-01-01T00:00:03.000001Z") - Interval("P100D")))",
            // R"(ts >= (Timestamp("1970-01-01T00:00:03.000001Z") - Interval("P100D")))", Olap apply?
            R"(inter64 >= (Interval("P100D") - Interval("P20D")))",

            // 1.2 Column and Column
            R"(dtm >= dt)",
            R"(dtm >= dt32)",
            R"(dtm64 >= dtm)",
            R"(dt >= ts)",
            R"(dt >= ts64)",

            // 2. Arithmetic
            R"(dt <= dt - inter64)",
            R"(dt <= dt - Interval("P100D"))",
            R"(dt32 <= dt32 - inter64)",
            R"(dt32 <= dt32 - Interval("P100D"))",
            R"(dtm <= dtm - inter64)",
            R"(dtm <= dtm - Interval("P100D"))",
            R"(dtm64 <= dtm64 - inter64)",
            R"(dtm64 <= dtm64 - Interval("P100D"))",
            R"(ts <= ts - inter64)",
            R"(ts <= ts - Interval("P100D"))",
            R"(ts64 <= ts64 - inter64)",
            R"(ts64 <= ts64 - Interval("P100D"))",

            R"(inter64 <= dt - Date('2001-01-01'))",
            R"(inter64 <= dt32 - Date32('2001-01-01'))",
            R"(inter64 <= dtm - DateTime('1998-12-01T15:30:00Z'))",
            R"(inter64 <= dtm64 - DateTime64('1998-12-01T15:30:00Z'))",
            R"(inter64 <= ts - Timestamp("1970-01-01T00:00:03.000001Z"))",
            R"(inter64 <= ts64 - Timestamp64("1970-01-01T00:00:03.000001Z"))",
        };

        auto queryPrefix = R"(
                SELECT * FROM `/Root/foo`
                WHERE
            )";

        for (const auto& predicate : testData) {
            auto query = queryPrefix + predicate + ";";
            Cerr << "Running query: " << query << "\n";
            auto result =
                session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            TString plan = *result.GetStats()->GetPlan();
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Predicate not pushed down. Query: " << query);
            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        for (const auto& predicate : testDataBlocks) {
            auto query = queryPrefix + predicate + ";";
            Cerr << "Running query: " << query << "\n";
            auto result =
                session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            TString plan = *result.GetStats()->GetPlan();
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Predicate not pushed down. Query: " << query);
            UNIT_ASSERT_C(ast.find("KqpOlapApply") == std::string::npos, TStringBuilder() << "Predicate pushed by scalar apply. Query: " << query);
            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(SimpleLookupOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        auto client = kikimr.GetTableClient();
        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` == CAST(1000000 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }
    }

    Y_UNIT_TEST(SimpleRangeOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                  AND `timestamp` <= CAST(2000000 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }
    }

    Y_UNIT_TEST(CompositeRangeOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `timestamp` < CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `timestamp` <= CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` > CAST(1000000 AS Timestamp)
                    AND `timestamp` <= CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `resource_id` == "0"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` <= CAST(1000001 AS Timestamp)
                    AND `resource_id` == "1"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` > CAST(1000000 AS Timestamp)
                    AND `resource_id` == "1"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` < CAST(1000001 AS Timestamp)
                    AND `resource_id` == "0"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }
    }

   Y_UNIT_TEST(EmptyRange) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto it = tableClient.StreamExecuteScanQuery(R"(
            --!syntax_v1

            SELECT *
            FROM `/Root/olapStore/olapTable`
            WHERE `timestamp` < CAST(3000001 AS Timestamp) AND `timestamp` > CAST(3000005 AS Timestamp)
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(StreamResultToYson(it), "[]");
    }

    Y_UNIT_TEST(PushdownFilter) {
        static bool enableLog = false;

        auto doTest = [](std::optional<bool> viaPragma, bool pushdownPresent) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
            auto settings = TKikimrSettings(appConfig)
                .SetWithSampleTables(false);

            if (enableLog) {
                Cerr << "Run test:" << Endl;
                Cerr << "viaPragma is " << (viaPragma.has_value() ? "" : "not ") << "present.";
                if (viaPragma.has_value()) {
                    Cerr << " Value: " << viaPragma.value();
                }
                Cerr << Endl;
                Cerr << "Expected result: " << pushdownPresent << Endl;
            }

            TKikimrRunner kikimr(settings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

            auto client = kikimr.GetTableClient();

            TLocalHelper(kikimr).CreateTestOlapTable();
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 10);

            TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);

            {
                TString query = TString(R"(
                    --!syntax_v1
                    SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id = "5"u;
                )");

                if (viaPragma.has_value() && !viaPragma.value()) {
                    TString pragma = TString(R"(
                        PRAGMA Kikimr.OptEnableOlapPushdown = "false";
                    )");
                    query = pragma + query;
                }

                auto it = client.StreamExecuteScanQuery(query).GetValueSync();

                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);

                /* FIXME: wrong column order.
                CompareYson(result, R"([[
                    [0];
                    ["some prefix xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"];
                    [5000u];
                    ["5"];
                    1000005u;
                    "uid_1000005"
                    ]])");
                */
            }
        };

        TVector<std::tuple<std::optional<bool>, bool>> testData = {
            {std::nullopt, true},
            {false, false},
            {true, true},
        };

        for (auto &data: testData) {
            doTest(std::get<0>(data), std::get<1>(data));
        }
    }

    Y_UNIT_TEST(CheckEarlyFilterOnEmptySelect) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        ui32 rowsCount = 0;
        {
            ui32 i = 0;
            const ui32 rowsPack = 20;
            const TInstant start = Now();
            while (!csController->GetCompactionFinishedCounter().Val() && Now() - start < TDuration::Seconds(100)) {
                WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * rowsPack, rowsPack);
                ++i;
                rowsCount += rowsPack;
            }
        }
        Sleep(TDuration::Seconds(10));
        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT * FROM `/Root/olapStore/olapTable`
            WHERE uid='dsfdfsd'
            LIMIT 10;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        Cerr << csController->GetFilteredRecordsCount().Val() << Endl;
        Y_ABORT_UNLESS(csController->GetFilteredRecordsCount().Val() * 10 <= rowsCount);
        UNIT_ASSERT(rows.size() == 0);
    }

    Y_UNIT_TEST(ExtractRangesSimple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideMemoryLimitForPortionReading(10000000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        auto tableClient = kikimr.GetTableClient();
        {
            auto alterQuery = TStringBuilder() <<
                              R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp))
                    AND (`uid` != 'uuu')
                ORDER BY `timestamp`
                LIMIT 1000;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(1000000);

        std::set<ui64> results = { 1000096, 1000097, 1000098, 1000099, 1000999, 1001000 };
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_GE_C(ts, tsPrev, "result is not sorted in ASC order");
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == 4);
    }

    Y_UNIT_TEST(ExtractRangesSimpleLimit) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);
        auto tableClient = kikimr.GetTableClient();
        {
            auto alterQuery = TStringBuilder() <<
                              R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`)
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp`
                LIMIT 1;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(1000000);

        std::set<ui64> results = { 1000096 };
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_GE_C(ts, tsPrev, "result is not sorted in ASC order");
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == 1);
    }

    Y_UNIT_TEST(ExtractRanges) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2001);

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp`
                LIMIT 1000;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(1000000);

        std::set<ui64> results = { 1000096, 1000097, 1000098, 1000099, 1000999, 1001000 };
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_GE_C(ts, tsPrev, "result is not sorted in ASC order");
            UNIT_ASSERT_C(results.erase(ts.GetValue()), Sprintf("%d", ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == 6);
    }

    Y_UNIT_TEST(ExtractRangesReverse) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` < CAST(1000300 AS Timestamp) AND `timestamp` >= CAST(1000295 AS Timestamp)) OR
                    (`timestamp` <= CAST(1000400 AS Timestamp) AND `timestamp` > CAST(1000395 AS Timestamp)) OR

                    (`timestamp` <= CAST(1000500 AS Timestamp) AND `timestamp` >= CAST(1000495 AS Timestamp)) OR
                    (`timestamp` <= CAST(1000505 AS Timestamp) AND `timestamp` >= CAST(1000499 AS Timestamp)) OR
                    (`timestamp` < CAST(1000510 AS Timestamp) AND `timestamp` >= CAST(1000505 AS Timestamp)) OR

                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp` DESC
                LIMIT 1000;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(2000000);
        std::set<ui64> results = { 1000096, 1000097, 1000098, 1000099,
            1000999, 1001000,
            1000295, 1000296, 1000297, 1000298, 1000299,
            1000396, 1000397, 1000398, 1000399, 1000400,
            1000495, 1000496, 1000497, 1000498, 1000499, 1000500, 1000501, 1000502, 1000503, 1000504, 1000505, 1000506, 1000507, 1000508, 1000509 };
        const ui32 expectedCount = results.size();
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_LE_C(ts, tsPrev, "result is not sorted in DESC order");
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == expectedCount);
    }

    Y_UNIT_TEST(PredicateDoNotPushdown) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> testDataNoPush = {
            R"(`level` != NULL)",
            R"(`level` > NULL)",
            R"(`timestamp` >= CAST(3000001U AS Timestamp))",
            R"(`level` = NULL)",
            R"(`level` > NULL)",
            R"(Re2::Match('uid.*')(`uid`))",
        };

        for (const auto& predicate: testDataNoPush) {
            auto pushQuery = BuildQuery(predicate, true);

            if (logQueries) {
                Cerr << "Query: " << pushQuery << Endl;
            }

            auto it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                          TStringBuilder() << "Predicate pushed down. Query: " << pushQuery);
        }
    }

    Y_UNIT_TEST(PredicatePushdownPartial) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> testDataPartialPush = {
            R"(LENGTH(`uid`) > 0 AND `resource_id` = "10001")",
            R"(`resource_id` = "10001" AND `level` > 1 AND LENGTH(`uid`) > 0)",
            R"(`resource_id` >= "10001" AND LENGTH(`uid`) > 0 AND `level` >= 1 AND `level` < 3)",
            R"(LENGTH(`uid`) > 0 AND (`resource_id` >= "10001" OR `level`>= 1 AND `level` <= 3))",
            R"(NOT(`resource_id` = "10001" OR `level` >= 1) AND LENGTH(`uid`) > 0)",
            R"(NOT(`resource_id` = "10001" AND `level` != 1) AND LENGTH(`uid`) > 0)",
            R"(`resource_id` = "10001" AND Unwrap(`level`/1) = `level`)",
            R"(`resource_id` = "10001" AND Unwrap(`level`/1) = `level` AND `level` > 1)",
        };

        for (const auto& predicate: testDataPartialPush) {
            auto normalQuery = BuildQuery(predicate, false);
            auto pushQuery = BuildQuery(predicate, true);

            Cerr << "--- Run normal query ---\n";
            Cerr << normalQuery << Endl;
            auto it = tableClient.StreamExecuteScanQuery(normalQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto goodResult = CollectStreamResult(it);

            Cerr << "--- Run pushed down query ---\n";
            Cerr << pushQuery << Endl;
            it = tableClient.StreamExecuteScanQuery(pushQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto pushResult = CollectStreamResult(it);

            if (logQueries) {
                Cerr << "Query: " << normalQuery << Endl;
                Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
                Cerr << "Received: " << pushResult.ResultSetYson << Endl;
            }

            CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

            it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);
            UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << pushQuery);
        }
    }

    Y_UNIT_TEST(PredicatePushdown_DifferentLvlOfFilters) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector< std::pair<TString, TString> > secondLvlFilters = {
            { R"(`uid` LIKE "%30000%")", "Filter-TableFullScan" },
            { R"(`uid` NOT LIKE "%30000%")", "Filter-TableFullScan" },
            { R"(`uid` LIKE "uid%")", "Filter-TableFullScan" },
            { R"(`uid` LIKE "%001")", "Filter-TableFullScan" },
            { R"(`uid` LIKE "uid%001")", "Filter-TableFullScan" },
        };
        std::string query = R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                `level` >= 1 AND
        )";

        for (auto filter : secondLvlFilters) {
            auto it = tableClient.StreamExecuteScanQuery(query + filter.first, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*result.PlanJson, &plan, true);

            /* FIXME: Does not look like new rbo has the same nodes names.
            auto readNode = FindPlanNodeByKv(plan, "Node Type", filter.second);
            UNIT_ASSERT(readNode.IsDefined());

            auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
            for (auto& op : operators) {
                if (op.GetMapSafe().at("Name") == "TableFullScan") {
                    UNIT_ASSERT(op.GetMapSafe().at("SsaProgram").IsDefined());
                    const auto ssa = op.GetMapSafe().at("SsaProgram").GetStringRobust();
                    UNIT_ASSERT(ssa.Contains(R"("Filter":{)"));
                }
            }
            */
        }
    }

    Y_UNIT_TEST(PredicatePushdown_LikePushedDownForStringType) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(SELECT id, binary_str FROM `/Root/tableWithNulls` WHERE binary_str LIKE "5%")";
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                        TStringBuilder() << "Predicate wasn't pushed down. Query: " << query);
    }

    Y_UNIT_TEST(PredicatePushdown_SimpleAsciiILike) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        // For insert.
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);

        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            const auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/foo` (
                    id Int64 NOT NULL,
                    str String,
                    u_str Utf8,
                    json JsonDocument,
                    PRIMARY KEY(id)
                )
                WITH (STORE = COLUMN);
            )").GetValueSync();
            UNIT_ASSERT(res.IsSuccess());
        }

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        {
            const auto res = session.ExecuteQuery(R"(
                INSERT INTO `/Root/foo` (id, str, u_str) VALUES
                    (1, "hello", "hello")
            )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues());
        }

        std::vector<TString> predicates = {
            "str ILIKE 'hell%'",
            "str ILIKE 'Hell%'",
            "str ILIKE 'Hello%'",
            "str ILIKE '%lo'",
            "str ILIKE '%Lo'",
            "str ILIKE '%Lo%'",
            "str ILIKE '%eLLo%'",
            "str ILIKE '%HeLLo%'",
            "str ILIKE '%LLL%'",
            "u_str ILIKE 'hell%'",
            "u_str ILIKE 'Hell%'",
            "u_str ILIKE 'Hello%'",
            "u_str ILIKE '%lo'",
            "u_str ILIKE '%Lo'",
            "u_str ILIKE '%Lo%'",
            "u_str ILIKE '%eLLo%'",
            "u_str ILIKE '%HeLLo%'",
            "u_str ILIKE '%LLL%'",
        };

        std::vector<TString> expectedResults = {
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[[1]]",
            "[]",
        };

        UNIT_ASSERT_EQUAL(expectedResults.size(), predicates.size());

        for (ui32 i = 0; i < predicates.size(); ++i) {
            const auto& query = TString(R"(
                PRAGMA OptimizeSimpleILike;
                PRAGMA AnsiLike;
                SELECT id FROM `/Root/foo` WHERE
                )") + predicates[i];
            Cerr
                << "QUERY " << i << Endl
                << query << Endl;

            auto result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            const auto ast = result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast->find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "ILIKE not pushed down. Query: " << query);
            result =
                session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Execute))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), expectedResults[i]);
        }
    }

    Y_UNIT_TEST(PredicatePushdown_MixStrictAndNotStrict) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                `resource_id` = "10001" AND Unwrap(`level`/1) = `level` AND `level` > 1;
        )";

        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find(R"(('eq '"resource_id")") != std::string::npos,
                          TStringBuilder() << "Subpredicate is not pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find(R"(('gt '"level")") != std::string::npos,
                          TStringBuilder() << "Subpredicate is not pushed down. Query: " << query);
        //This check is based on an assumpltion, that for pushed down predicates column names are preserved in AST
        //But for non-pushed down predicates column names are (usually) replaced with a label, started with $. It' not a rule, but a heuristic
        //So this check may require a correction when some ast optimization rules are changed
        UNIT_ASSERT_C(ast.find(R"((Unwrap (/ $)") != std::string::npos,
                          TStringBuilder() << "Unsafe subpredicate is pushed down. Query: " << query);

        UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << query);
    }

    Y_UNIT_TEST(PredicatePushdownWithParametersILike) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);

        constexpr bool logQueries = true;
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto buildQuery = [](bool pushEnabled) {
            TStringBuilder builder;

            builder << "--!syntax_v1" << Endl;

            if (!pushEnabled) {
                builder << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }

            builder << R"(
                DECLARE $in_uid AS Utf8;
                DECLARE $in_level AS Int32;

                SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                    uid ILIKE "uid_%" || $in_uid || "%" AND level > $in_level
                ORDER BY `timestamp`;
            )" << Endl;

            return builder;
        };

        auto normalQuery = buildQuery(false);
        auto pushQuery = buildQuery(true);

        auto params = tableClient.GetParamsBuilder()
            .AddParam("$in_uid")
                .Utf8("3000")
                .Build()
            .AddParam("$in_level")
                .Int32(2)
                .Build()
            .Build();

        auto it = tableClient.StreamExecuteScanQuery(normalQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto goodResult = CollectStreamResult(it);

        it = tableClient.StreamExecuteScanQuery(pushQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto pushResult = CollectStreamResult(it);

        if (logQueries) {
            Cerr << "Query: " << normalQuery << Endl;
            Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
            Cerr << "Received: " << pushResult.ResultSetYson << Endl;
        }

        CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

        it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();

        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                      TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);

        NJson::TJsonValue plan, readRange;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);

        Cerr << result.PlanJson << Endl;

        readRange = FindPlanNodeByKv(plan, "Name", "TableFullScan");
        UNIT_ASSERT(readRange.IsDefined());
    }

    Y_UNIT_TEST(PredicatePushdownWithParameters) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);

        constexpr bool logQueries = true;
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto buildQuery = [](bool pushEnabled) {
            TStringBuilder builder;

            builder << "--!syntax_v1" << Endl;

            if (!pushEnabled) {
                builder << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }

            builder << R"(
                DECLARE $in_uid AS Utf8;
                DECLARE $in_level AS Int32;

                SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                    uid > $in_uid AND level > $in_level
                ORDER BY `timestamp`;
            )" << Endl;

            return builder;
        };

        auto normalQuery = buildQuery(false);
        auto pushQuery = buildQuery(true);

        auto params = tableClient.GetParamsBuilder()
            .AddParam("$in_uid")
                .Utf8("uid_3000980")
                .Build()
            .AddParam("$in_level")
                .Int32(2)
                .Build()
            .Build();

        auto it = tableClient.StreamExecuteScanQuery(normalQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto goodResult = CollectStreamResult(it);

        it = tableClient.StreamExecuteScanQuery(pushQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto pushResult = CollectStreamResult(it);

        if (logQueries) {
            Cerr << "Query: " << normalQuery << Endl;
            Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
            Cerr << "Received: " << pushResult.ResultSetYson << Endl;
        }

        CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

        it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();

        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                      TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);

        NJson::TJsonValue plan, readRange;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);

        readRange = FindPlanNodeByKv(plan, "Name", "TableFullScan");
        UNIT_ASSERT(readRange.IsDefined());
    }

    Y_UNIT_TEST(PredicatePushdownParameterTypesValidation) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);

        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

        std::map<std::string, TParams> testData = CreateParametersOfAllTypes(tableClient);

        const TString queryTemplate = R"(
            --!syntax_v1
            DECLARE $in_value AS <--TYPE-->;
            SELECT `key` FROM `/Root/olapStore/OlapParametersTable` WHERE <--NAME-->_column > $in_value;
        )";

        for (auto& [type, parameter]: testData) {
            TString query(queryTemplate);
            std::string clearType = type;

            size_t pos = clearType.find('(');

            if (std::string::npos != pos) {
                clearType = clearType.substr(0, pos);
            }

            SubstGlobal(query, "<--TYPE-->", type);
            SubstGlobal(query, "<--NAME-->", clearType);

            TStringBuilder b;

            b << "----------------------------" << Endl;
            b << query << Endl;
            b << "----------------------------" << Endl;

            auto it = tableClient.StreamExecuteScanQuery(query, parameter).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString() << Endl << b);
            auto goodResult = CollectStreamResult(it);

            it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString() << Endl << b);

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << query);
        }
    }

     // Unit test for https://github.com/ydb-platform/ydb/issues/7967
    Y_UNIT_TEST(PredicatePushdownNulls) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 10);

        auto tableClient = kikimr.GetTableClient();

        TString query = R"(
                SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                    (case when level > 0
	                    then level
	                    else null
	                end) > 0;
        )";

        auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();
        // Check for successful execution
        auto streamPart = it.ReadNext().GetValueSync();

        UNIT_ASSERT(streamPart.IsSuccess());
    }

    Y_UNIT_TEST(PredicatePushdownCastErrors) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

        const std::set<std::string> numerics = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double"};
        const std::set<std::string> datetimes = {"Timestamp","Date","Datetime"};
        const std::map<std::string, std::set<std::string>> exceptions = {
            {"Int8", numerics},
            {"Int16", numerics},
            {"Int32", numerics},
            {"Int64", numerics},
            {"UInt8", numerics},
            {"UInt16", numerics},
            {"UInt32", numerics},
            {"UInt64", numerics},
            {"Float", numerics},
            {"Double", numerics},
            {"String", {"Utf8"}},
            {"Utf8", {"String"}},
            {"Timestamp", datetimes},
            {"Date", datetimes},
            {"Datetime", datetimes}
        };

        std::vector<std::string> allTypes = {
            //"Bool",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Double",
            "Float",
            //"Decimal(12,9)",
            "String",
            "Utf8",
            "Timestamp",
            "Date",
            "Datetime",
            //"Interval"
        };

        std::map<std::string, TParams> parameters = CreateParametersOfAllTypes(tableClient);

        const std::vector<std::string> predicates = {
            "<--NAME-->_column > $in_value",
            "<--NAME-->_column = $in_value",
            "$in_value > <--NAME-->_column",
            "$in_value = <--NAME-->_column",
        };

        const TString queryBegin = R"(
            --!syntax_v1
            DECLARE $in_value AS <--TYPE-->;
            SELECT `key` FROM `/Root/olapStore/OlapParametersTable` WHERE
        )";

        std::vector<std::string> falsePositive;
        std::vector<std::string> falseNegative;

        for (const auto& predicateTemplate: predicates) {
            for (const auto& type: allTypes) {
                for (const auto& checkType: allTypes) {
                    bool error = true;

                    auto exc = exceptions.find(checkType);

                    if (exc != exceptions.end() && exc->second.contains(type)) {
                        error = false;
                    } else if (type == checkType) {
                        error = false;
                    }

                    std::string clearType = type;

                    size_t pos = clearType.find('(');

                    if (std::string::npos != pos) {
                        clearType = clearType.substr(0, pos);
                    }

                    TString query(queryBegin);
                    TString predicate(predicateTemplate);
                    SubstGlobal(query, "<--TYPE-->", checkType);
                    SubstGlobal(predicate, "<--NAME-->", clearType);

                    auto parameter = parameters.find(checkType);

                    UNIT_ASSERT_C(parameter != parameters.end(), "No type " << checkType << " in parameters");

                    Cerr << "Test query:\n" << query + predicate << Endl;

                    auto it = tableClient.StreamExecuteScanQuery(query + predicate, parameter->second).GetValueSync();
                    // Check for successful execution
                    auto streamPart = it.ReadNext().GetValueSync();

                    bool pushdown;

                    if (streamPart.IsSuccess()) {
                        it = tableClient.StreamExecuteScanQuery(
                            query + predicate, parameter->second, scanSettings
                        ).GetValueSync();

                        auto result = CollectStreamResult(it);
                        auto ast = result.QueryStats->Getquery_ast();

                        pushdown = ast.find("KqpOlapFilter") != std::string::npos;
                    } else {
                        // Error means that predicate not pushed down
                        pushdown = false;
                    }

                    if (error && pushdown) {
                        falsePositive.emplace_back(
                            TStringBuilder() << type << " vs " << checkType << " at " << predicate
                        );
                        continue;
                    }

                    if (!error && !pushdown) {
                        falseNegative.emplace_back(
                            TStringBuilder() << type << " vs " << checkType << " at " << predicate
                        );
                    }
                }
            }
        }

        TStringBuilder b;
        b << "Errors found:" << Endl;
        b << "------------------------------------------------" << Endl;
        b << "False positive" << Endl;

        for (const auto& txt: falsePositive) {
            b << txt << Endl;
        }

        b << "False negative" << Endl;
        for (const auto& txt: falseNegative) {
            b << txt << Endl;
        }

        b << "------------------------------------------------" << Endl;
        UNIT_ASSERT_C(falsePositive.empty() && falseNegative.empty(), b);
    }

    void RunBlockChannelTest(auto blockChannelsMode) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        settings.AppConfig.MutableTableServiceConfig()->SetBlockChannelsMode(blockChannelsMode);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingNodes("None");
        settings.AppConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        {
            const TString query = R"(
                CREATE TABLE `/Root/ColumnShard` (
                    a Uint64 NOT NULL,
                    b Int32,
                    c Int64,
                    PRIMARY KEY (a)
                )
                PARTITION BY HASH(a)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();
        // Inserts are not supported in new rbo.
        /*
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (a, b, c) VALUES
                    (1u, 1, 5),
                    (2u, 2, 5),
                    (3u, 2, 0),
                    (4u, 2, 5);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(prepareResult.IsSuccess());;
        }
        */

        {
            NYdb::NQuery::TExecuteQuerySettings scanSettings;
            scanSettings.ExecMode(NYdb::NQuery::EExecMode::Explain);
            auto it = client.StreamExecuteQuery(R"(
                SELECT
                    b, COUNT(*), SUM(a)
                FROM `/Root/ColumnShard`
                WHERE c = 5
                GROUP BY b
                ORDER BY b;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), scanSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            auto plan = CollectStreamResult(it);

            switch (blockChannelsMode) {
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(ToFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideToBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_EQUAL_C(plan.QueryStats->Getquery_ast().find("WideFromBlocks"), plan.QueryStats->Getquery_ast().rfind("WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(WideSortBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(ToFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
            }
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT
                    b, COUNT(*), SUM(a)
                FROM `/Root/ColumnShard`
                WHERE c = 5
                GROUP BY b
                ORDER BY b;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            //CompareYson(output, R"([[[1];1u;1u];[[2];2u;6u]])");
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/ColumnIsHard` (
                    a Uint64 NOT NULL,
                    b Int32,
                    c Int64,
                    PRIMARY KEY (a)
                )
                PARTITION BY HASH(a)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Inserts are not support in new rbo.
        /*
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnIsHard` (a, b, c) VALUES
                    (1u, 1, 5),
                    (2u, 2, 5),
                    (3u, 2, 0),
                    (4u, 2, 5);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(prepareResult.IsSuccess());
        }
        */

        {
            NYdb::NQuery::TExecuteQuerySettings scanSettings;
            scanSettings.ExecMode(NYdb::NQuery::EExecMode::Explain);
            auto it = client.StreamExecuteQuery(R"(
    	    	PRAGMA ydb.UseBlockHashJoin = "false";
                PRAGMA ydb.OptimizerHints =
                '
                    JoinType(T1 T2 Shuffle)
                ';

                SELECT T1.a
                    FROM (SELECT Just(a) as a FROM `/Root/ColumnShard`) as T1
                    LEFT JOIN `/Root/ColumnIsHard` as T2
                    ON T1.a = T2.a
                ;

            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), scanSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            auto plan = CollectStreamResult(it);

            auto CountSubstr = [](const TString& str, const TString& sub) -> ui64 {
                ui64 count = 0;
                for (auto pos = str.find(sub); pos != TString::npos; pos = str.find(sub, pos + sub.size())) {
                    ++count;
                }
                return count;
            };

            switch (blockChannelsMode) {
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR:
                    // TODO: implement checks?
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(GraceJoinCore (ToFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_EQUAL_C(CountSubstr(plan.QueryStats->Getquery_ast(), "(ToFlow (WideFromBlocks"), 2, plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideToBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
                    // TODO: implement checks?
                    break;
            }
        }
    }

    Y_UNIT_TEST(BlockChannelScalar) {
        RunBlockChannelTest(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR);
    }

    Y_UNIT_TEST(BlockChannelAuto) {
        RunBlockChannelTest(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);
    }

    Y_UNIT_TEST(BlockChannelForce) {
        RunBlockChannelTest(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE);
    }

    Y_UNIT_TEST(DisableBlockExecutionPerQuery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto client = kikimr.GetQueryClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10, 1'000'000, 10);
        }

        const TString query = R"(
            PRAGMA ydb.DisableBlockExecution;

            SELECT timestamp, resource_id, uid, level
            FROM `/Root/olapStore/olapTable`
            WHERE level >= 1
            ORDER BY level, resource_id
            LIMIT 5
        )";

        {
            auto res = StreamExplainQuery(query, client);
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto plan = CollectStreamResult(res);
            Cerr << plan.QueryStats->query_ast() << Endl;

            UNIT_ASSERT_C(!plan.QueryStats->query_ast().Contains("BlockAsStruct"), plan.QueryStats->Getquery_ast());
        }

        {
            auto it = client.StreamExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), R"([
                [1000001u;["11"];"uid_1000001";[1]];
                [1000006u;["16"];"uid_1000006";[1]];
                [1000002u;["12"];"uid_1000002";[2]];
                [1000007u;["17"];"uid_1000007";[2]];
                [1000003u;["13"];"uid_1000003";[3]]
            ])");
        }
    }

    Y_UNIT_TEST(SimpleCountNoFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 300, true);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            const std::vector<std::string> queries = {
                R"(
                    SELECT count(level)
                    FROM `/Root/olapStore/olapTable`;
                )",
                R"(
                    SELECT sum(level)
                    FROM `/Root/olapStore/olapTable`;
                )",
                R"(
                    SELECT avg(level)
                    FROM `/Root/olapStore/olapTable`;
                )",
                R"(
                    SELECT min(level)
                    FROM `/Root/olapStore/olapTable`;
                )",
                R"(
                    SELECT max(level)
                    FROM `/Root/olapStore/olapTable`;
                )",
                R"(
                    SELECT count(*)
                    FROM `/Root/olapStore/olapTable`;
                )",
            };

            const std::vector<TString> results = {
                R"([[200u]])",
                R"([[[400]]])",
                R"([[[2.]]])",
                R"([[[0]]])",
                R"([[[4]]])",
                R"([[300u]])",
            };

            for (ui32 i = 0; i < queries.size(); ++i) {
                auto it = client.StreamExecuteScanQuery(queries[i]).GetValueSync();

                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);
                //Cout << result << Endl;
                CompareYson(results[i], result);
            }
        }
    }

    Y_UNIT_TEST(CountWhereColumnIsNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 300, true);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT(*), COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level IS NULL
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson("[[100u;0u]]", result);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT(*), COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level IS NULL AND uid IS NOT NULL
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson("[[100u;0u]]", result);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT(*), COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level IS NULL
                GROUP BY level
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson("[[100u;0u]]", result);
        }
    }

    Y_UNIT_TEST(SimpleCount) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 300, true);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE StartsWith(uid, "uid_")
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson("[[200u]]", result);
        }
    }

    Y_UNIT_TEST(DoubleOutOfRangeInJson) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetColumnShardDoubleOutOfRangeHandling(
            NKikimrConfig::TColumnShardConfig_EJsonDoubleOutOfRangeHandlingPolicy_CAST_TO_INFINITY);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                CREATE TABLE `/Root/olapTable` (
                    k Uint32 NOT NULL,
                    v JsonDocument NOT NULL,
                    PRIMARY KEY (k)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4
                )
            )",
                                  NQuery::TTxControl::NoTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        {
            TValueBuilder rowsBuilder;
            rowsBuilder.BeginList();
            for (ui32 i = 0; i < 10; ++i) {
                rowsBuilder.AddListItem().BeginStruct().AddMember("k").Uint32(i * 4 + 0).AddMember("v").JsonDocument("-1.797693135e+308").EndStruct();
                rowsBuilder.AddListItem().BeginStruct().AddMember("k").Uint32(i * 4 + 1).AddMember("v").JsonDocument("1.797693135e+308").EndStruct();
                rowsBuilder.AddListItem().BeginStruct().AddMember("k").Uint32(i * 4 + 2).AddMember("v").JsonDocument("1e1000000000000").EndStruct();
                rowsBuilder.AddListItem().BeginStruct().AddMember("k").Uint32(i * 4 + 3).AddMember("v").JsonDocument("-1e1000000000000").EndStruct();
            }
            rowsBuilder.EndList();
            auto result = client.BulkUpsert("/Root/olapTable", rowsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        {
            auto it = client.StreamExecuteScanQuery("SELECT * FROM `/Root/olapTable` WHERE k < 4 ORDER BY k").ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[0u;"\"-inf\""];[1u;"\"inf\""];[2u;"\"inf\""];[3u;"\"-inf\""]])");
        }
    }

    Y_UNIT_TEST(SimpleRequestHasProjections) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 20);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT 1
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);

            CompareYson(result, R"([[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT count(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);

            CompareYson(result, R"([[20u]])");
        }
    }

    Y_UNIT_TEST(PushdownFilterKnownIssuies) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session2 = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Uint64 NOT NULL,
                b Uint32 NOT NULL,
                c Timestamp NOT NULL,
                d Utf8,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        std::vector<TString> queries = {
            R"(
                $some_time = Timestamp("2000-01-10T08:00:00.000000Z");
                SELECT
                    *
                FROM
                    `/Root/t1`
                WHERE
                    (cast(c as Timestamp?) > $some_time)
            )",
            R"(
                $some_time = Timestamp("2000-01-10T08:00:00.000000Z");
                SELECT
                    *
                FROM
                    `/Root/t1`
                WHERE
                    (just(c) > $some_time)
            )",
            /* Bug in RBO type ann with sublink.
            R"(
                $sub = (select distinct (b) from `/Root/t1` where b > 10);

                select count(*) from `/Root/t1` as t1
                where t1.b = $sub;
            )",
            */
            /* Unsupported in YqlSelect group by alias.
            R"(
                select a, res, count(*) as cnt
                from `/Root/t1`
                where (b % 128) == 0
                group by a, cast(bitcast(Digest::IntHash64(a) as UInt32)/((Math::Pow(2, 32)/240) + 1) as UInt32) as res
                order by cnt desc;
            )",
            */
            R"(
                SELECT
                    d,
                FROM
                    `/Root/t1` as t1
                WHERE
                    t1.d is not distinct from "some_str";
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            Cout << query << Endl;
            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Olap filter not pushed down. Query: " << query);

            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    // This test triggers "heap-use-after-free" error if we call `PeepholeOptimizeNode()`
    // on expression with `free args`.
    Y_UNIT_TEST(OlapFilterPeephole) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
 
        auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/test` (
                D Utf8 not null,
                S Utf8 not null,
                V Utf8,
            PRIMARY KEY (D, S)
        ) WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session2 = result.GetSession();
        std::string query =
        R"(SELECT DISTINCT `t1`.`S` AS `result` FROM
           (SELECT D, S, V FROM `/Root/test`) AS `t1` WHERE
            Unicode::ReplaceAll(CAST(`t1`.`V` AS UTF8),
                                coalesce(CAST(Unicode::Substring(CAST(`t1`.`V` AS UTF8),
                                Unicode::GetLength(CAST(`t1`.`V` AS UTF8)) - 8) AS UTF8), ''),
            coalesce(CAST('' AS UTF8), '')) IN ('m') AND
            CASE WHEN String::Contains(`t1`.`D`, 'l') THEN 'c'
                 WHEN String::Contains(`t1`.`D`, 's') THEN 's'
                 WHEN String::Contains(`t1`.`D`, 'r') THEN 'r'
                 ELSE 'o'
            END IN ('c');
        )";

         auto resultQuery =
              session2
                  .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                NYdb::NQuery::TExecuteQuerySettings().ExecMode(
                                    NQuery::EExecMode::Explain))
                  .ExtractValueSync();
          UNIT_ASSERT_VALUES_EQUAL(resultQuery.GetStatus(), EStatus::SUCCESS);
          auto ast = *resultQuery.GetStats()->GetAst();
          // Filter not pushed because map before filter.
          /*
          UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                      TStringBuilder() << "Predicate not pushed down. Query: " << query);
          */
   }

}
} // namespace NKqp
} // namespace NKikimr
