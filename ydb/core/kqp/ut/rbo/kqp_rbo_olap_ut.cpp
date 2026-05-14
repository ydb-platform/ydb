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
            //R"((Int32("1"), "uid_3000001", "10001") = (`level`, `uid`, `resource_id`))",
            //R"((Int32("1"), `uid`, "10001") = (`level`, "uid_3000001", `resource_id`))",
            R"(`level` = 0 AND "uid_3000000" = `uid`)",
            R"(`uid` > `resource_id`)",
            //R"(`level` IS NULL)",
            //R"(`level` IS NOT NULL)",
            //R"(`message` IS NULL)",
            //R"(`message` IS NOT NULL)",
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
            //R"(`level` IS NULL OR `message` IS NULL)",
            //R"(`level` IS NOT NULL AND `message` IS NULL)",
            //R"(`level` IS NULL AND `message` IS NOT NULL)",
            //R"(`level` IS NOT NULL AND `message` IS NOT NULL)",
            //R"(`level` IS NULL XOR `message` IS NOT NULL)",
            //R"(`level` IS NULL XOR `message` IS NULL)",
            R"(`level` + 2. < 5.f)",
            R"(`level` - 2.f >= 1.)",
            R"(`level` * 3. > 4.f)",
            R"(`level` / 2.f <= 1.)",
            R"(`level` % 3. != 1.f)",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:03.000001Z") AND `level` < 4)",
            //R"(`resource_id` != "10001" XOR "XXX" == "YYY")",
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

}
} // namespace NKqp
} // namespace NKikimr
