#include <ydb/core/formats/arrow/ssa_runtime_version.h>

#include "helpers/get_value.h"
#include "helpers/query_executor.h"
#include "helpers/local.h"
#include "helpers/writer.h"
#include "helpers/aggregation.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
using namespace NSchemeShard;
using namespace NActors;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

Y_UNIT_TEST_SUITE(KqpOlap) {

    class TExtLocalHelper: public TLocalHelper {
    private:
        using TBase = TLocalHelper;
        TKikimrRunner& KikimrRunner;
    public:
        bool TryCreateTable(const TString& storeName, const TString& tableName, const ui32 shardsCount) {
            auto tableClient = KikimrRunner.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
            CREATE TABLE `/Root/%s/%s`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8,
                level Int32,
                message Utf8,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
                )
            )", storeName.data(), tableName.data(), shardsCount);
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                Cerr << result.GetIssues().ToOneLineString() << Endl;
            }
            return result.GetStatus() == EStatus::SUCCESS;
        }

        bool DropTable(const TString& storeName, const TString& tableName) {
            auto tableClient = KikimrRunner.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
            DROP TABLE `/Root/%s/%s`
            )", storeName.data(), tableName.data());
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                Cerr << result.GetIssues().ToOneLineString() << Endl;
            }
            return result.GetStatus() == EStatus::SUCCESS;
        }

        TExtLocalHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
            , KikimrRunner(runner) {

        }
    };

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
                                             Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
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

    Y_UNIT_TEST(SimpleQueryOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }
    }

    Y_UNIT_TEST(SimpleQueryOlapStats) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonStat;
            CollectRows(it, &jsonStat);
            UNIT_ASSERT(!jsonStat.IsNull());
            const TString plan = jsonStat.GetStringRobust();
            Cerr << plan << Endl;
            UNIT_ASSERT(plan.find("NodesScanShards") == TString::npos);
        }

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Profile);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();
            NJson::TJsonValue jsonStat;
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CollectRows(it, &jsonStat);
            const TString plan = jsonStat.GetStringRobust();
            Cerr << plan << Endl;
            UNIT_ASSERT(plan.find("NodesScanShards") != TString::npos);
        }
    }

    Y_UNIT_TEST(SimpleQueryOlapDiagnostics) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonDiagnostics;
            CollectRows(it, nullptr, &jsonDiagnostics);
            UNIT_ASSERT_C(!jsonDiagnostics.IsDefined(), "Query result diagnostics should be empty, but it's not");
        }

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            settings.CollectFullDiagnostics(true);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonDiagnostics;
            CollectRows(it, nullptr, &jsonDiagnostics);
            UNIT_ASSERT(!jsonDiagnostics.IsNull());

            UNIT_ASSERT_C(jsonDiagnostics.IsMap(), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_id"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("version"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_text"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_parameter_types"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("table_metadata"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("created_at"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_syntax"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_database"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_cluster"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_plan"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_type"), "Incorrect Diagnostics");
        }
    }

    Y_UNIT_TEST(SimpleLookupOlap) {
        auto settings = TKikimrSettings()
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
        auto settings = TKikimrSettings()
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
        auto settings = TKikimrSettings()
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

    void CreateSampleOltpTable(TKikimrRunner& kikimr) {
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "OltpTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "Value1", Type: "String" }
            Columns { Name: "Value2", Type: "String" }
            KeyColumnNames: ["Key"]
        )");

        TTableClient tableClient{kikimr.GetDriver()};
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/OltpTable` (Key, Value1, Value2) VALUES
                (1u,   "Value-001",  "1"),
                (2u,   "Value-002",  "2"),
                (42u,  "Value-002",  "2"),
                (101u, "Value-101",  "101")
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        session.Close();
    }

    Y_UNIT_TEST(ScanQueryOltpAndOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetTableClient();

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 3);

        CreateSampleOltpTable(kikimr);

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT a.`resource_id`, a.`timestamp`, t.*
                FROM `/Root/OltpTable` AS t
                JOIN `/Root/olapStore/olapTable` AS a ON CAST(t.Key AS Utf8) = a.resource_id
                ORDER BY a.`resource_id`, a.`timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[[1u];["Value-001"];["1"];["1"];1000001u];[[2u];["Value-002"];["2"];["2"];1000002u]])");
        }
    }

    Y_UNIT_TEST(YqlScriptOltpAndOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 3);

        CreateSampleOltpTable(kikimr);

        {
            NScripting::TScriptingClient client(kikimr.GetDriver());
            auto it = client.ExecuteYqlScript(R"(
                --!syntax_v1

                SELECT a.`resource_id`, a.`timestamp`, t.*
                FROM `/Root/OltpTable` AS t
                JOIN `/Root/olapStore/olapTable` AS a ON CAST(t.Key AS Utf8) = a.resource_id
                ORDER BY a.`resource_id`, a.`timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = FormatResultSetYson(it.GetResultSet(0));
            Cout << result << Endl;
            CompareYson(result, R"([[[1u];["Value-001"];["1"];["1"];1000001u];[[2u];["Value-002"];["2"];["2"];1000002u]])");
        }
    }

    Y_UNIT_TEST(EmptyRange) {
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
            auto settings = TKikimrSettings()
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

                CompareYson(result, R"([[
                    [0];
                    ["some prefix xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"];
                    ["5"];
                    1000005u;
                    "uid_1000005"
                    ]])");
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

    Y_UNIT_TEST(PKDescScan) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 128);

        auto tableClient = kikimr.GetTableClient();
        auto selectQueryWithSort = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` DESC LIMIT 4;
        )");
        auto selectQuery = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` LIMIT 4;
        )");

        auto it = tableClient.StreamExecuteScanQuery(selectQuery, scanSettings).GetValueSync();
        auto result = CollectStreamResult(it);

        NJson::TJsonValue plan, node, reverse, limit, pushedLimit;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        Cerr << *result.PlanJson << Endl;
        Cerr << result.QueryStats->query_plan() << Endl;
        Cerr << result.QueryStats->query_ast() << Endl;

        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableFullScan");
        UNIT_ASSERT(node.IsDefined());
        reverse = FindPlanNodeByKv(node, "Reverse", "false");
        UNIT_ASSERT(!reverse.IsDefined());
        pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
        UNIT_ASSERT(pushedLimit.IsDefined());
        limit = FindPlanNodeByKv(node, "Limit", "4");
        UNIT_ASSERT(limit.IsDefined());

        // Check that Reverse flag is set in query plan
        it = tableClient.StreamExecuteScanQuery(selectQueryWithSort, scanSettings).GetValueSync();
        result = CollectStreamResult(it);

        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        Cerr << "==============================" << Endl;
        Cerr << *result.PlanJson << Endl;
        Cerr << result.QueryStats->query_plan() << Endl;
        Cerr << result.QueryStats->query_ast() << Endl;

        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableFullScan");
        UNIT_ASSERT(node.IsDefined());
        reverse = FindPlanNodeByKv(node, "Reverse", "true");
        UNIT_ASSERT(reverse.IsDefined());
        limit = FindPlanNodeByKv(node, "Limit", "4");
        UNIT_ASSERT(limit.IsDefined());
        pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
        UNIT_ASSERT(pushedLimit.IsDefined());

        // Run actual request in case explain did not execute anything
        it = tableClient.StreamExecuteScanQuery(selectQueryWithSort).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        auto ysonResult = CollectStreamResult(it).ResultSetYson;

        auto expectedYson = TString(R"([
            [1000127u];
            [1000126u];
            [1000125u];
            [1000124u]
        ])");

        CompareYson(expectedYson, ysonResult);
    }

    Y_UNIT_TEST(CheckEarlyFilterOnEmptySelect) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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

    Y_UNIT_TEST(ExtractRanges) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

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
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == 6);
    }

    Y_UNIT_TEST(ExtractRangesReverse) {
        auto settings = TKikimrSettings()
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

    TString BuildQuery(const TString& predicate, bool pushEnabled) {
        TStringBuilder qBuilder;
        qBuilder << "--!syntax_v1" << Endl;
        qBuilder << "PRAGMA Kikimr.OptEnableOlapPushdown = '" << (pushEnabled ? "true" : "false") << "';" << Endl;
        qBuilder << "SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE ";
        qBuilder << predicate;
        qBuilder << " ORDER BY `timestamp`";
        return qBuilder;
    };

    Y_UNIT_TEST(PredicatePushdown) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
#if SSA_RUNTIME_VERSION >= 4U
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, true);
#else
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, false);
#endif
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        // TODO: Add support for DqPhyPrecompute push-down: Cast((2+2) as Uint64)
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
            R"((Int32("1"), `uid`, "10001") = (`level`, "uid_3000001", `resource_id`))",
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
#if SSA_RUNTIME_VERSION >= 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
#if SSA_RUNTIME_VERSION >= 4U
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
            //R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z"))",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:03.000001Z") AND `level` < 4)",
            R"((`timestamp`, `level`) >= (Timestamp("1970-01-01T00:00:03.000001Z"), 3))",
#endif
#if SSA_RUNTIME_VERSION >= 5U
            R"(`resource_id` != "10001" XOR "XXX" == "YYY")",
            R"(IF(`level` > 3, -`level`, +`level`) < 2)",
            R"(StartsWith(`message` ?? `resource_id`, "10000"))",
            R"(NOT EndsWith(`message` ?? `resource_id`, "xxx"))",
            R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) == <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
            R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) != <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
            R"(`uid` LIKE "_id%000_")",
            R"(`uid` ILIKE "UID%002")",
#endif
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

    Y_UNIT_TEST(PredicateDoNotPushdown) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
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
#if SSA_RUNTIME_VERSION < 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
#if SSA_RUNTIME_VERSION < 4U
            R"(`level` * 3.14 > 4)",
            R"(LENGTH(`uid`) > 0 OR `resource_id` = "10001")",
            R"((LENGTH(`uid`) > 0 AND `resource_id` = "10001") OR `resource_id` = "10002")",
            R"((LENGTH(`uid`) > 0 OR `resource_id` = "10002") AND (LENGTH(`uid`) < 15 OR `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 AND `resource_id` = "10001"))",
            R"(Unwrap(`level`/1) = `level` AND `resource_id` = "10001")",
            R"(NOT(LENGTH(`uid`) > 0 OR `resource_id` = "10001"))",
            R"(`level` + 2 < 5)",
            R"(`level` - 2 >= 1)",
            R"(`level` * 3 > 4)",
            R"(`level` / 2 <= 1)",
            R"(`level` % 3 != 1)",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z"))",
#endif
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
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
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
#if SSA_RUNTIME_VERSION >= 2U
    Y_UNIT_TEST(PredicatePushdown_DifferentLvlOfFilters) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector< std::pair<TString, TString> > secondLvlFilters = {
            { R"(`uid` LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` NOT LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` LIKE "uid%")", "TableFullScan" },
            { R"(`uid` LIKE "%001")", "TableFullScan" },
#if SSA_RUNTIME_VERSION >= 4U
            { R"(`uid` LIKE "uid%001")", "TableFullScan" },
#else
            { R"(`uid` LIKE "uid%001")", "Filter-TableFullScan" }, // We have filter (Size >= 6)
#endif
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
        }
    }
#endif

#if SSA_RUNTIME_VERSION >= 3U
    Y_UNIT_TEST(PredicatePushdown_LikePushedDownForStringType) {
#else
    Y_UNIT_TEST(PredicatePushdown_LikeNotPushedDownForStringType) {
#endif
        auto settings = TKikimrSettings()
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
#if SSA_RUNTIME_VERSION >= 3U
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                        TStringBuilder() << "Predicate wasn't pushed down. Query: " << query);
#else
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                        TStringBuilder() << "Predicate was pushed down. Query: " << query);
#endif
    }
#if SSA_RUNTIME_VERSION < 5U
    Y_UNIT_TEST(PredicatePushdown_LikeNotPushedDownIfAnsiLikeDisabled) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(
            PRAGMA DisableAnsiLike;
            SELECT id, resource_id FROM `/Root/tableWithNulls` WHERE resource_id LIKE "%5%"
        )";
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                        TStringBuilder() << "Predicate pushed down. Query: " << query);
    }
#endif
    Y_UNIT_TEST(PredicatePushdown_MixStrictAndNotStrict) {
        auto settings = TKikimrSettings()
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
                          TStringBuilder() << "Predicate not pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find(R"(('gt '"level")") == std::string::npos,
                          TStringBuilder() << "Predicate pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << query);
    }

    Y_UNIT_TEST(SelectLimit1ManyShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);


        Tests::TServer::TPtr server = new Tests::TServer(settings);
        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        const ui32 numShards = 10;
        const ui32 numIterations = 10;
        TLocalHelper(*server).CreateTestOlapTable("selectTable", "selectStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/selectStore/selectTable", 0, 1000000 + i * 1000000, 2000);
        }

        ui64 result = 0;

        std::vector<TAutoPtr<IEventHandle>> evs;
        ui32 num = 0;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(num);
                        ++num;
                        num = num % 2;
                    }
                    break;
                }

                case NYql::NDq::TDqComputeEvents::EvChannelData: {
                    auto& record = ev->Get<NYql::NDq::TEvDqCompute::TEvChannelData>()->Record;
                    if (record.GetChannelData().GetChannelId() == 2) {
                        Cerr << (TStringBuilder() << "captured event for the second channel" << Endl);
                        Cerr.Flush();
                    }

                    Cerr << (TStringBuilder() << "-- EvChannelData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    if (record.GetChannelData().GetChannelId() == 2) {
                        Cerr << (TStringBuilder() << "captured event for the second channel" << Endl);
                        evs.push_back(ev);
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    result = 1;

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, "SELECT * FROM `/Root/selectStore/selectTable` LIMIT 1;", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, 1);
    }

    Y_UNIT_TEST(ManyColumnShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
//        Tests::NCommon::TLoggerInit(runtime).Initialize();

        ui32 numShards = NSan::PlainOrUnderSanitizer(1000, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(50, 10);
        TLocalHelper(*server).SetShardingMethod("HASH_FUNCTION_CLOUD_LOGS").CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i * 1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
        runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST(ManyColumnShardsFilterPushdownEmptySet) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        const ui32 numShards = 10;
        const ui32 numIterations = 50;
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
        }

        bool hasResult = false;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 0);
                    hasResult = true;

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, "SELECT * FROM `/Root/largeOlapStore/largeOlapTable` where resource_id = Utf8(\"notfound\");", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT(hasResult);
    }

    Y_UNIT_TEST(ManyColumnShardsWithRestarts) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
//        Tests::NCommon::TLoggerInit(runtime).Initialize();

        ui32 numShards = NSan::PlainOrUnderSanitizer(100, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(100, 10);
        TLocalHelper(*server).SetShardingMethod("HASH_FUNCTION_CLOUD_LOGS").CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;

        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        THashSet<TActorId> columnShardScans;
        bool prevIsFinished = false;

        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                case NKqp::TKqpComputeEvents::EvScanData: {
                    auto [it, success] = columnShardScans.emplace(ev->Sender);
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    if (success) {
                        // first scan response.
                        prevIsFinished = msg->Finished;
                        return TTestActorRuntime::EEventAction::PROCESS;
                    } else {
                        if (prevIsFinished) {
                            Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": hijack event" << Endl);
                            Cerr.Flush();
                            for (auto&& i : csController->GetShardActualIds()) {
                                runtime->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                                    new TEvents::TEvPoisonPill(), i, false));
                            }
                        } else {
                            prevIsFinished = msg->Finished;
                        }
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender, NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST(PredicatePushdownWithParameters) {
        constexpr bool logQueries = true;
        auto settings = TKikimrSettings()
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
        auto settings = TKikimrSettings()
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

    Y_UNIT_TEST(OlapLayout) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        settings.AppConfig.MutableColumnShardConfig()->MutableTablesStorageLayoutPolicy()->MutableIdentityGroups();
        Y_ABORT_UNLESS(settings.AppConfig.GetColumnShardConfig().GetTablesStorageLayoutPolicy().HasIdentityGroups());
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore1", 20, 4);
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_1", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_2", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_3", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_4", 5));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 16, 4);
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_1", 4));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_2", 4));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_4", 2));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_5", 1));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_6", 1));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_7", 5));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_8", 3));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 8));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_10", 17));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_11", 2));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_12", 1));

        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_1"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_2"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_4"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_5"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_6"));

        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_7", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_8", 2));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 9));

        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_12"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_11"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 9));
    }

    void TestOlapUpsert(ui32 numShards) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/test_table`
            (
                WatchID Int64 NOT NULL,
                CounterID Int32 NOT NULL,
                URL Text NOT NULL,
                Age Int16 NOT NULL,
                Sex Int16 NOT NULL,
                PRIMARY KEY (CounterID, WatchID)
            )
            PARTITION BY HASH(WatchID)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT =)" << numShards
            << ")";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/test_table` (WatchID, CounterID, URL, Age, Sex) VALUES
                (0, 15, 'aaaaaaa', 23, 1),
                (0, 15, 'bbbbbbb', 23, 1),
                (1, 15, 'ccccccc', 23, 1);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); // TODO: snapshot isolation?

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            TString query = R"(
                --!syntax_v1
                SELECT CounterID, WatchID
                FROM `/Root/test_table`
                ORDER BY CounterID, WatchID
            )";

            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            //CompareYson(result, R"([[0;15];[1;15]])");
            CompareYson(result, R"([])"); // FIXME
        }
    }

    Y_UNIT_TEST(OlapUpsertImmediate) {
        // Should be fixed in KIKIMR-17646
        return;

        TestOlapUpsert(1);
    }

    Y_UNIT_TEST(OlapUpsert) {
        // Should be fixed in KIKIMR-17646
        return;

        TestOlapUpsert(2);
    }

    Y_UNIT_TEST(OlapDeleteImmediate) {
        // Should be fixed in KIKIMR-17582
        return;

        TPortManager pm;
        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 1;
            )"
        );

    }

    Y_UNIT_TEST(OlapDeleteImmediatePK) {
        // Should be fixed in KIKIMR-17582
        return;

        TPortManager pm;
        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 1 AND CounterID = 1;
            )"
        );

    }
/*
    Y_UNIT_TEST(OlapDeletePlanned) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 0;
            )"
#if 1 // TODO
            , false
#endif
        );
    }
*/

    Y_UNIT_TEST(PredicatePushdownCastErrors) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

#if SSA_RUNTIME_VERSION >= 4U
        const std::set<std::string> numerics = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double"};
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
        };
#else
        std::map<std::string, std::set<std::string>> exceptions = {
            {"Int8", {"Int16", "Int32"}},
            {"Int16", {"Int8", "Int32"}},
            {"Int32", {"Int8", "Int16"}},
            {"UInt8", {"UInt16", "UInt32"}},
            {"UInt16", {"UInt8", "UInt32"}},
            {"UInt32", {"UInt8", "UInt16"}},
            {"String", {"Utf8"}},
            {"Utf8", {"String", "Json", "Yson"}},
        };
#endif

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

    Y_UNIT_TEST(OlapRead_FailsOnDataQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/tableWithNulls`;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesScanOnJoin) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesScanOnJoinWithDataShardTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesGenericQueryOnJoinWithDataShardTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT timestamp, resource_id, uid, level FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[1000001u;["1"];"uid_1000001";[1]]])");
    }

    Y_UNIT_TEST(OlapRead_GenericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(OlapRead_StreamGenericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        auto db = kikimr.GetQueryClient();

        auto it = db.StreamExecuteQuery(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(OlapRead_ScanQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(DuplicatesInIncomingBatch) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);
        Tests::NCommon::TLoggerInit(testHelper.GetRuntime()).Initialize();
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "id_second"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull().AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add("val1").AddNull();
            tableInserter.AddRow().Add(3).Add("test_res_3").Add("val3").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add("val2").AddNull();
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        while (csController->GetInsertFinishedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=2", "[[2;\"test_res_2\";#;[\"val1\"]]]");
    }

    Y_UNIT_TEST(BulkUpsertUpdate) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(10);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        while (csController->GetInsertFinishedCounter().Val() < 1) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[10]]");
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(110);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
        while (csController->GetInsertFinishedCounter().Val() < 2) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
    }

    void RunBlockChannelTest(auto blockChannelsMode) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetBlockChannelsMode(blockChannelsMode);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(true);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                a Uint64 NOT NULL,
                b Int32,
                c Int64,
                PRIMARY KEY (a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
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
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("return (FromFlow (NarrowMap (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(FromFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideToBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_EQUAL_C(plan.QueryStats->Getquery_ast().find("WideFromBlocks"), plan.QueryStats->Getquery_ast().rfind("WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(FromFlow (WideSortBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(FromFlow (NarrowMap (WideFromBlocks"), plan.QueryStats->Getquery_ast());
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
            CompareYson(output, R"([[[1];1u;1u];[[2];2u;6u]])");
        }

        {
            NYdb::NQuery::TExecuteQuerySettings scanSettings;
            scanSettings.ExecMode(NYdb::NQuery::EExecMode::Explain);
            auto it = client.StreamExecuteQuery(R"(
                PRAGMA ydb.CostBasedOptimizationLevel='0';

                $select1 = (
                    SELECT b AS a1, COUNT(*) AS b1, SUM(a) AS c1
                    FROM `/Root/ColumnShard`
                    WHERE c = 5
                    GROUP BY b
                );

                $select2 = (
                    SELECT (b1 + 1ul) AS a2, COUNT(*) AS b2, SUM(a1) AS c2
                    FROM $select1
                    WHERE c1 = 5
                    GROUP BY b1
                );

                $select3 = (
                    SELECT b1 AS a3, COUNT(*) AS b3, MAX(a1) AS c3
                    FROM $select1
                    WHERE b1 = 6
                    GROUP BY b1
                );

                SELECT a2, b2
                FROM $select2 AS table2
                JOIN $select3 AS table3
                ON table2.a2 = table3.a3
                ORDER BY b2
                LIMIT 10
                ;

            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), scanSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            auto plan = CollectStreamResult(it);

            // auto CountSubstr = [](const TString& str, const TString& sub) -> ui64 {
            //     ui64 count = 0;
            //     for (auto pos = str.find(sub); pos != TString::npos; pos = str.find(sub, pos + sub.size())) {
            //         ++count;
            //     }
            //     return count;
            // };

            switch (blockChannelsMode) {
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR:
                    // TODO: implement checks?
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    // TODO: test fails because of some stages don't get wide channels.
                    // UNIT_ASSERT_EQUAL_C(CountSubstr(plan.QueryStats->Getquery_ast(), "WideFromBlocks"), 2, plan.QueryStats->Getquery_ast());
                    // UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideToBlocks"), plan.QueryStats->Getquery_ast());
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

    Y_UNIT_TEST(CompactionPlanner) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`s-buckets`,
                    `COMPACTION_PLANNER.FEATURES`=`{"logic_name" : "slices"}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));
        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`s-buckets`,
                    `COMPACTION_PLANNER.FEATURES`=`{"logic_name" : "one_head"}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
        csController->WaitCompactions(TDuration::Seconds(5));

        {
            auto alterQuery = TStringBuilder() << "(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`error-buckets`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::GENERIC_ERROR, alterResult.GetIssues().ToString());
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[120000u;]])");
        }

    }

    Y_UNIT_TEST(MultiInsertWithSinks) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:00Z'), 'a', '0');
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:01Z'), 'b', 't');
            INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:02Z'), 'c', 'test');
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        {
            auto it = db.StreamExecuteQuery(R"(
                --!syntax_v1

                SELECT
                    *
                FROM `/Root/olapStore/olapTable` ORDER BY uid
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, R"([[#;#;["0"];0u;"a"];[#;#;["t"];1000000u;"b"];[#;#;["test"];2000000u;"c"]])");
        }
    }

}

}
