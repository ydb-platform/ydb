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
#include <ydb/public/lib/ydb_cli/common/format.h>

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
                new_column1 Uint64,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp)
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = %d
                )
            )",
                             storeName.data(), tableName.data(), shardsCount);
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

    Y_UNIT_TEST(AlterObjectDisabled) {
        auto settings = TKikimrSettings()
             .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();

        {
            //1. QueryService
            //1.1 Check that ALTER OBJECT is not working for column tables
            auto client = kikimr.GetQueryClient();
            const auto result = client.ExecuteQuery(
                "ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=message, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`, `COMPRESSION.LEVEL`=`4`)",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Error: ALTER OBJECT is disabled for column tables", result.GetIssues().ToString());

            //1.2 Check that ALTER TABLE is still working for column tables
            {
                const auto result = client.ExecuteQuery(
                    "ALTER TABLE `/Root/olapTable` DROP COLUMN message",
                    NYdb::NQuery::TTxControl::NoTx()
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                const auto result = client.ExecuteQuery(
                    "ALTER TABLE `/Root/olapTable` ADD COLUMN message Text",
                    NYdb::NQuery::TTxControl::NoTx()
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                const auto result = client.ExecuteQuery(
                    "ALTER TABLE `/Root/olapTable` ALTER FAMILY default SET compression 'LZ4';",
                    NYdb::NQuery::TTxControl::NoTx()
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                const auto result = client.ExecuteQuery(
                    "ALTER TABLE `/Root/olapTable` set TTL Interval('P1D') on timestamp;",
                    NYdb::NQuery::TTxControl::NoTx()
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }
        {
            //2. TableService
            //2.1 Check that ALTER OBJECT is not working for column tables
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto result = session.ExecuteSchemeQuery(
                "ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=message, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`, `COMPRESSION.LEVEL`=`4`)"
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Error: ALTER OBJECT is disabled for column tables", result.GetIssues().ToString());
            //2.2 Check that ALTER TABLE is still working for column tables
            {
                const auto result = session.ExecuteSchemeQuery(
                "ALTER TABLE `/Root/olapTable` DROP COLUMN message"
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                const auto result = session.ExecuteSchemeQuery(
                    "ALTER TABLE `/Root/olapTable` ADD COLUMN message Text"
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }
        {
            //3. YqlScript
            //3.1 Check that ALTER OBJECT is not working for column tables
            NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
            auto result = client.ExecuteYqlScript(
                "ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=message, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`, `COMPRESSION.LEVEL`=`4`)"
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Error: ALTER OBJECT is disabled for column tables", result.GetIssues().ToString());
            //3.2 YqlScript is deprecated, not woth bothering about positive checks
            //skipped
        }
    }

    Y_UNIT_TEST(ConstantIfPushDown) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `statistics2` (
                        username Utf8 NOT NULL,
                        PRIMARY KEY (username)
                    )
                    PARTITION BY HASH(username)
                    WITH (
                        STORE = COLUMN,
                        PARTITION_COUNT=1
                    );
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                    R"(
                    UPSERT INTO `statistics2`
                    ( `username`)
                    VALUES ( "a" ), ( "b" ), ( "c" ), ( "d" );
                    )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(R"(
                --!syntax_v1
                select count(*) as cnt
                from statistics2 where IF(len(' ') > 0, username = ' ', True)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(status.GetResultSet(0).RowsCount(), 1);
        }
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

    Y_UNIT_TEST(EmptyColumnsRead) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetQueryClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.ExecuteQuery(R"(
                --!syntax_v1

                SELECT 1
                FROM `/Root/olapStore/olapTable`
            )",NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
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

    Y_UNIT_TEST(SimpleQueryOlapMeta) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Basic);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonMeta;
            CollectRows(it, nullptr, &jsonMeta);
            UNIT_ASSERT_C(!jsonMeta.IsDefined(), "Query result meta should be empty, but it's not");
        }

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
            NJson::TJsonValue jsonMeta;
            CollectRows(it, nullptr, &jsonMeta);
            UNIT_ASSERT(!jsonMeta.IsNull());

            UNIT_ASSERT_C(jsonMeta.IsMap(), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_id"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("version"), "Incorrect Meta");
            UNIT_ASSERT_C(!jsonMeta.Has("query_text"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_parameter_types"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("table_metadata"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("created_at"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_syntax"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_database"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_cluster"), "Incorrect Meta");
            UNIT_ASSERT_C(!jsonMeta.Has("query_plan"), "Incorrect Meta");
            UNIT_ASSERT_C(jsonMeta.Has("query_type"), "Incorrect Meta");
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
                    [5000u];
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
        auto selectQuerySortDesc = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` DESC LIMIT 4;
        )");
        auto selectQuerySortAsc = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` LIMIT 4;
        )");
        auto selectQueryNoSort = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` LIMIT 4;
        )");

        NJson::TJsonValue plan, node, reverse, limit, pushedLimit;
        {
            auto it = tableClient.StreamExecuteScanQuery(selectQuerySortAsc, scanSettings).GetValueSync();
            auto result = CollectStreamResult(it);

            NJson::ReadJsonTree(*result.PlanJson, &plan, true);
            Cerr << *result.PlanJson << Endl;
            Cerr << result.QueryStats->query_plan() << Endl;
            Cerr << result.QueryStats->query_ast() << Endl;

            node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableFullScan");
            UNIT_ASSERT(node.IsDefined());
            reverse = FindPlanNodeByKv(node, "Reverse", "false");
            UNIT_ASSERT(reverse.IsDefined());
            pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
            UNIT_ASSERT(pushedLimit.IsDefined());
            limit = FindPlanNodeByKv(node, "Limit", "4");
            UNIT_ASSERT(limit.IsDefined());
        }

        {
            // Check that Reverse flag is set in query plan
            auto it = tableClient.StreamExecuteScanQuery(selectQuerySortDesc, scanSettings).GetValueSync();
            auto result = CollectStreamResult(it);

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
        }

        {
            // Check that Reverse flag is set in query plan
            auto it = tableClient.StreamExecuteScanQuery(selectQueryNoSort, scanSettings).GetValueSync();
            auto result = CollectStreamResult(it);

            NJson::ReadJsonTree(*result.PlanJson, &plan, true);
            Cerr << "==============================" << Endl;
            Cerr << *result.PlanJson << Endl;
            Cerr << result.QueryStats->query_plan() << Endl;
            Cerr << result.QueryStats->query_ast() << Endl;

            node = FindPlanNodeByKv(plan, "Node Type", "Limit-TableFullScan");
            UNIT_ASSERT(node.IsDefined());
            limit = FindPlanNodeByKv(node, "Limit", "4");
            UNIT_ASSERT(limit.IsDefined());
            pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
            UNIT_ASSERT(pushedLimit.IsDefined());
        }

        // Run actual request in case explain did not execute anything
        auto it = tableClient.StreamExecuteScanQuery(selectQuerySortDesc).GetValueSync();

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

    Y_UNIT_TEST(ExtractRangesSimple) {
        auto settings = TKikimrSettings()
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
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, true);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        // TODO: Add support for DqPhyPrecompute push-down: Cast((2+2) as Uint64)
        // - this is not needed because constant folding eliminates this now
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
            //R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z"))",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:03.000001Z") AND `level` < 4)",
            //R"((`timestamp`, `level`) >= (Timestamp("1970-01-01T00:00:03.000001Z"), 3))", //-- Started to break with bad kernel
            R"(`resource_id` != "10001" XOR "XXX" == "YYY")",
            R"(IF(`level` > 3, -`level`, +`level`) < 2)",
            R"(StartsWith(`message` ?? `resource_id`, "10000"))",
            R"(NOT EndsWith(`message` ?? `resource_id`, "xxx"))",
            R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) == <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
            R"(ChooseMembers(TableRow(), ['level', 'uid', 'resource_id']) != <|level:1, uid:"uid_3000001", resource_id:"10001"|>)",
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

    Y_UNIT_TEST(PredicatePushdown_LikePushedDownForStringType) {
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
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                        TStringBuilder() << "Predicate wasn't pushed down. Query: " << query);
    }

    Y_UNIT_TEST(PredicatePushdown_SimpleAsciiILike) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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

            const auto res = session
                                 .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                     NYdb::NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full))
                                 .ExtractValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues());

            const auto ast = res.GetStats()->GetAst();
            UNIT_ASSERT_C(ast->find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "ILIKE not pushed down. Query: " << query);
            CompareYson(FormatResultSetYson(res.GetResultSet(0)), expectedResults[i]);
        }
    }

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

    Y_UNIT_TEST(DisablePushdownAggregate) {
        auto settings = TKikimrSettings()
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
                a Int64	NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());


        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/foo` (a, b)  VALUES (1, 1);
            INSERT INTO `/Root/foo` (a, b)  VALUES (2, 11);
            INSERT INTO `/Root/foo` (a, b)  VALUES (3, 11);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        std::vector<TString> queries = {
            R"(
                SELECT count(*) FROM `/Root/foo`
                where b > a
                GROUP BY a;
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];

            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            Cerr << "AST " << ast << Endl;
            UNIT_ASSERT_C(ast.find("TKqpOlapAgg") == std::string::npos, TStringBuilder() << "Aggregatee pushed down. Query: " << query);
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Olap filter not pushed down. Query: " << query);
        }
    }

    Y_UNIT_TEST(PushdownFilterMultiConsumersRead) {
        auto settings = TKikimrSettings()
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
                a Int64	NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        std::vector<TString> queries = {
            R"(
                $sub = (select distinct (b) from `/Root/t1` where b > 10);

                select count(*) from `/Root/t1` as t1
                where t1.b = $sub;
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
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

    Y_UNIT_TEST(PushdownIfWithParams)
    {
        auto settings = TKikimrSettings()
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
                a Int64	NOT NULL,
                b Uint8,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        std::vector<TString> queries = {
            R"(
                declare $str_param as String;

                $filter = ($filter_param)->{
                    RETURN case
                        WHEN $str_param = "One" THEN IF($filter_param=1, True, False)
                        ELSE True
                        END
                    };
                select t1.a from `/Root/t1` as t1 where $filter(t1.b);
            )",
            R"(
                declare $str_param as String;

                $filter = ($filter_param)->{
                   RETURN case
                       WHEN $str_param = "One" THEN IF($filter_param=1, True, False)
                       WHEN $str_param = "Two" THEN IF($filter_param=2, True, False)
                       ELSE True
                       END
                };
                select t1.a from `/Root/t1` as t1 where $filter(t1.b);
            )",
            R"(
                declare $str_param as String;

                $filter = ($filter_param)->{
                   RETURN case
                       WHEN $str_param = "One" THEN IF($filter_param=1, True, False)
                       WHEN $str_param = "Two" THEN IF($filter_param=2, True, False)
                       WHEN $str_param = "Three" THEN IF($filter_param=3, True, False)
                       ELSE True
                       END
                };
                select t1.a from `/Root/t1` as t1 where $filter(t1.b);
            )"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos, TStringBuilder() << "Olap filter not pushed down. Query: " << query);
        }
    }

    Y_UNIT_TEST(ProjectionPushDown) {
        auto settings = TKikimrSettings()
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
                a Int64	NOT NULL,
                b Int32,
                timestamp Timestamp,
                jsonDoc JsonDocument,
                jsonDoc1 JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());


        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (1, 1, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"a.b.c" : "a1", "b.c.d" : "b1", "c.d.e" : "c1"}'), JsonDocument('{"a" : "1.1", "b" : "1.2", "c" : "1.3"}'));
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (2, 11, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"a.b.c" : "a2", "b.c.d" : "b2", "c.d.e" : "c2"}'), JsonDocument('{"a" : "2.1", "b" : "2.2", "c" : "2.3"}'));
            INSERT INTO `/Root/foo` (a, b, timestamp, jsonDoc, jsonDoc1)
            VALUES (3, 11, Timestamp("1970-01-01T00:00:03.000001Z"), JsonDocument('{"b.c.a" : "a3", "b.c.d" : "b3", "c.d.e" : "c3"}'), JsonDocument('{"x" : "3.1", "y" : "1.2", "z" : "1.3"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        std::vector<TString> queries = {
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT * FROM `/Root/foo`
                where b > 10
                GROUP BY JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as r
                ORDER BY r;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT min(`timestamp`) as bucket, column, count(*)
                FROM `/Root/foo`
                WHERE JSON_EXISTS(jsonDoc, "$.\"a.b.c\"")
                GROUP BY CAST(`timestamp` as Int64) / 1200 / 1000000, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as column
                ORDER BY column;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT min(`timestamp`) as bucket, column, count(*)
                FROM `/Root/foo`
                GROUP BY CAST(`timestamp` as Int64) / 1200 / 1000000, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as column
                ORDER BY column;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT min(`timestamp`) as bucket, column, count(*)
                FROM `/Root/foo`
                WHERE JSON_EXISTS(jsonDoc, "$.\"a.b.c\"") AND (`timestamp`) != CurrentUtcTimestamp()-DateTime::IntervalFromHours(24)
                GROUP BY CAST(`timestamp` as Int64) / 1200 / 1000000, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as column
                ORDER BY column;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT a, JSON_VALUE(jsonDoc, "$.\"a.b.c\""), JSON_VALUE(jsonDoc, "$.\"b.c.d\""), JSON_VALUE(jsonDoc, "$.\"c.d.e\"")
                FROM `/Root/foo`
                ORDER BY a;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT CAST(JSON_VALUE(jsonDoc1, "$.\"a\"") as Double) as result
                FROM `/Root/foo`
                ORDER BY result;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT (JSON_VALUE(jsonDoc, "$.\"a.b.c\"") in ["a1", "a3", "a4"]) as col1, CAST(JSON_VALUE(jsonDoc1, "$.\"a\"") as Double) as col2
                FROM `/Root/foo`
                ORDER BY col2;
            )",
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT (JSON_VALUE(jsonDoc, "$.\"a.b.c\"") in ["a1", "a3", "a4"]) as col1,
                       CAST(JSON_VALUE(jsonDoc1, "$.\"a\"") as Double) as col2,
                       CAST(JSON_VALUE(jsonDoc1, "$.\"b\"") as Double) as col3
                FROM `/Root/foo`
                ORDER BY col2;
            )",
            R"(
                PRAGMA kikimr.OptEnableOlapPushdownProjections="true";

                SELECT a, JSON_VALUE(jsonDoc, "$.\"a.b.c\"") as result
                FROM `/Root/foo`
                WHERE timestamp = Timestamp("1970-01-01T00:00:03.000001Z")
                ORDER BY a
                LIMIT 1;
            )",
        };

        std::vector<TString> results = {
            R"([[#];[["a2"]]])",
            R"([[[3000001u];["a1"];1u];[[3000001u];["a2"];1u]])",
            R"([[[3000001u];#;1u];[[3000001u];["a1"];1u];[[3000001u];["a2"];1u]])",
            R"([[[3000001u];["a1"];1u];[[3000001u];["a2"];1u]])",
            R"([[1;["a1"];["b1"];["c1"]];[2;["a2"];["b2"];["c2"]];[3;#;["b3"];["c3"]]])",
            R"([[#];[[1.1]];[[2.1]]])",
            R"([[#;#];[[%true];[1.1]];[[%false];[2.1]]])",
            R"([[#;#;#];[[%true];[1.1];[1.2]];[[%false];[2.1];[2.2]]])",
            R"([[1;["a1"]]])"
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];

            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto ast = *result.GetStats()->GetAst();

            UNIT_ASSERT_C(ast.find("KqpOlapProjections") != std::string::npos, TStringBuilder() << "Projections not pushed down. Query: " << query);
            UNIT_ASSERT_C(ast.find("KqpOlapProjection") != std::string::npos, TStringBuilder() << "Projection not pushed down. Query: " << query);

            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TString output = FormatResultSetYson(result.GetResultSet(0));
            CompareYson(output, results[i]);
        }

        std::vector<TString> notPushedQueries = {
            R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

                SELECT jsonDoc, JSON_VALUE(jsonDoc, "$.\"a.b.c\"")
                FROM `/Root/foo`
                where b == 1;
            )"
        };

        for (ui32 i = 0; i < notPushedQueries.size(); ++i) {
            const auto query = notPushedQueries[i];
            auto result =
                session2
                    .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("KqpOlapProjections") == std::string::npos, TStringBuilder() << "Projections pushed down. Query: " << query);
            UNIT_ASSERT_C(ast.find("KqpOlapProjection") == std::string::npos, TStringBuilder() << "Projection pushed down. Query: " << query);
        }
    }

    // Unit tests for datetime pushdowns in query service
    Y_UNIT_TEST(PredicatePushdown_Datetime_QS) {
        auto settings = TKikimrSettings()
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

        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/foo` (id, dt, dt32, dtm, dtm64, ts, ts64, inter64)
            VALUES (1,
                CAST('1998-12-01' AS Date),
                CAST('1998-12-01' AS Date32),
                CAST('1998-12-01' AS DateTime),
                CAST('1998-12-01' AS DateTime64),
                CAST('1998-12-01' AS Timestamp),
                CAST('1998-12-01' AS Timestamp64),
                CAST('1D' AS Interval64));
            )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

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

        for (const auto& predicate: testData) {

            auto query = queryPrefix + predicate + ";";

            auto result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            //if (result.GetStatus() != EStatus::SUCCESS) {
            //    Cout << "Error in query planning: " << query << "\n";
            //    continue;
            //}

            TString plan = *result.GetStats()->GetPlan();
            auto ast = *result.GetStats()->GetAst();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                              TStringBuilder() << "Predicate not pushed down. Query: " << query);
            //if (ast.find("KqpOlapFilter") != std::string::npos) {
            //    Cout << "Predicate not pushed, Query: " << query << "\n";
            //    continue;
            //}

            result = session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            //if (result.GetStatus() != EStatus::SUCCESS) {
            //    Cout << "Error in query: " << query << "\n";
            //    continue;
            //}
        }

        for (const auto& predicate : testDataBlocks) {
            auto query = queryPrefix + predicate + ";";

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

    // This test triggers "heap-use-after-free" error if we call `PeepholeOptimizeNode()`
    // on expression with `free args`.
    Y_UNIT_TEST(OlapFilterPeephole) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
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
          UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                      TStringBuilder() << "Predicate not pushed down. Query: " << query);
   }

    // Unit tests for datetime pushdowns in scan query
    Y_UNIT_TEST(PredicatePushdown_Datetime_SQ) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();

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

        auto insertRes = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/foo` (id, dt, dt32, dtm, dtm64, ts, ts64, inter64)
            VALUES (1,
                CAST('1998-12-01' AS Date),
                CAST('1998-12-01' AS Date32),
                CAST('1998-12-01' AS DateTime),
                CAST('1998-12-01' AS DateTime64),
                CAST('1998-12-01' AS Timestamp),
                CAST('1998-12-01' AS Timestamp64),
                CAST('1D' AS Interval64));
            )", NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

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
            R"(dt <= Date('2001-01-01'))"
        };

        auto queryPrefix = R"(
                SELECT * FROM `/Root/foo`
                WHERE
            )";

        for (const auto& predicate: testData) {

            auto query = queryPrefix + predicate + ";";

            auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                              TStringBuilder() << "Predicate not pushed down. Query: " << query);

            it = tableClient.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            result = CollectStreamResult(it);
            Cout << result.ResultSetYson;
        }
    }

    Y_UNIT_TEST(SelectLimit1ManyShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2).SetColumnShardReaderClassName("SIMPLE");


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

                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardsToNodes) {
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

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender,
            NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, R"(
                pragma ydb.DqChannelVersion = "1";
                SELECT * FROM `/Root/selectStore/selectTable` LIMIT 1;
            )", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, 1);
    }

    Y_UNIT_TEST(ManyColumnShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false).SetNodeCount(2).SetColumnShardReaderClassName("SIMPLE");

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        runtime->SetScheduledLimit(1000000);

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

                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardsToNodes) {
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

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
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
            .SetUseRealThreads(false).SetNodeCount(2).SetColumnShardReaderClassName(
                "SIMPLE");

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
//        Tests::NCommon::TLoggerInit(runtime).Initialize();

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

                    auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardsToNodes) {
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

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                    resp->Record.SetEnough(false);
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

    class TManyColumnShardsWithRestartsExecutor {
    private:
        virtual void FillSettings(Tests::TServerSettings& settings) const = 0;
        virtual void OnAfterTest() const = 0;

        Tests::TServerSettings BuildSettings() {
            Tests::TServerSettings settings(PortsManager.GetPort(2134));
            FillSettings(settings);
            return settings;
        }

    protected:
        TPortManager PortsManager;

    public:
        void Execute() {
            auto settings = BuildSettings();

            Tests::TServer::TPtr server = new Tests::TServer(settings);
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
            auto kqpController = NYDBTest::TControllers::RegisterKqpControllerGuard<NYDBTest::TTestKqpController>();

            auto runtime = server->GetRuntime();
            auto sender = runtime->AllocateEdgeActor();

            InitRoot(server, sender);
//            Tests::NCommon::TLoggerInit(runtime).Initialize();

            ui32 numShards = NSan::PlainOrUnderSanitizer(100, 10);
            ui32 numIterations = NSan::PlainOrUnderSanitizer(100, 10);
            TLocalHelper(*server)
                .SetShardingMethod("HASH_FUNCTION_CLOUD_LOGS")
                .CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
            ui32 insertRows = 0;

            for (ui64 i = 0; i < numIterations; ++i) {
                TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i * 1000000, 2000);
                insertRows += 2000;
            }

            ui64 result = 0;
            THashSet<TActorId> columnShardScans;
            bool prevIsFinished = false;

            bool needCount = true;
            auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                switch (ev->GetTypeRewrite()) {
                    case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {
                        auto* msg = ev->Get<NKqp::NShardResolver::TEvShardsResolveStatus>();
                        for (auto& [shardId, nodeId] : msg->ShardsToNodes) {
                            Cerr << "-- nodeId: " << nodeId << Endl;
                            nodeId = runtime->GetNodeId(0);
                        }
                        break;
                    }

                    case NKqp::TKqpExecuterEvents::EvStreamData: {
                        auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                        if (needCount) {
                            Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                            Cerr.Flush();
                            Y_ASSERT(record.GetResultSet().rows().size() == 1);
                            Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                            result += record.GetResultSet().rows().at(0).items().at(0).uint64_value();
                        } else {
                            Cerr << (TStringBuilder() << "-- EvStreamData: " << record.GetResultSet().rows().size() << Endl);
                            Cerr.Flush();
                            result += record.GetResultSet().rows().size();
                        }

                        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                        resp->Record.SetEnough(false);
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
                                    runtime->Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
                                        new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
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
            {
                result = 0;
                auto streamSender = runtime->AllocateEdgeActor();
                NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender,
                    NDataShard::NKqpHelpers::MakeStreamRequest(
                        streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
                auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
                UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
            }
            {
                result = 0;
                needCount = false;
                auto streamSender = runtime->AllocateEdgeActor();
                NDataShard::NKqpHelpers::SendRequest(*runtime, streamSender,
                    NDataShard::NKqpHelpers::MakeStreamRequest(streamSender, "SELECT * FROM `/Root/largeOlapStore/largeOlapTable`;", false));
                auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
                UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
            }
            OnAfterTest();
        }
    };

    Y_UNIT_TEST(ManyColumnShardsWithRestartsSimple) {
        class TTestExecutor: public TManyColumnShardsWithRestartsExecutor {
        private:
            virtual void OnAfterTest() const override {
                AFL_VERIFY(NYDBTest::TControllers::GetKqpControllerAs<NYDBTest::TTestKqpController>()->GetInitScanCounter().Val());
                AFL_VERIFY(!NYDBTest::TControllers::GetKqpControllerAs<NYDBTest::TTestKqpController>()->GetResolvingCounter().Val());
            }
            virtual void FillSettings(Tests::TServerSettings& settings) const override {
                settings.SetDomainName("Root").SetUseRealThreads(false).SetNodeCount(2).SetColumnShardReaderClassName("SIMPLE");
            }
        };
        TTestExecutor().Execute();
    }

    Y_UNIT_TEST(ManyColumnShardsWithRestartsWithResolving) {
        class TTestExecutor: public TManyColumnShardsWithRestartsExecutor {
        private:
            virtual void OnAfterTest() const override {
                AFL_VERIFY(NYDBTest::TControllers::GetKqpControllerAs<NYDBTest::TTestKqpController>()->GetInitScanCounter().Val());
                AFL_VERIFY(NYDBTest::TControllers::GetKqpControllerAs<NYDBTest::TTestKqpController>()->GetResolvingCounter().Val());
            }
            virtual void FillSettings(Tests::TServerSettings& settings) const override {
                settings.SetDomainName("Root")
                    .SetUseRealThreads(false)
                    .SetNodeCount(2)
                    .SetColumnShardReaderClassName("SIMPLE")
                    .SetScanReaskToResolve(1);
            }
        };
        TTestExecutor().Execute();
    }

    Y_UNIT_TEST(PredicatePushdownWithParametersILike) {
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

    void TestOlapUpsert(ui32 numShards, bool allowOlapDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(allowOlapDataQuery);
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
                PARTITION_COUNT =)" << numShards
                                      << ")";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/test_table` (WatchID, CounterID, URL, Age, Sex) VALUES
                (0, 15, 'aaaaaaa', 23, 1),
                (0, 15, 'bbbbbbb', 23, 1),
                (1, 15, 'ccccccc', 23, 1);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); // TODO: snapshot isolation?

        if (allowOlapDataQuery) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_C(
                result.GetIssues().ToString().contains(
                    "Data manipulation queries with column-oriented tables are supported only by API QueryService."),
                result.GetIssues().ToString());
        }

        {
            TString query = R"(
                --!syntax_v1
                SELECT CounterID, WatchID
                FROM `/Root/test_table`
                ORDER BY CounterID, WatchID
            )";

            auto it = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();

            if (allowOlapDataQuery) {
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = FormatResultSetYson(it.GetResultSet(0));
                Cout << result << Endl;
                CompareYson(result, R"([[15;0];[15;1]])");
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_C(
                    result.GetIssues().ToString().contains(
                        "Data manipulation queries with column-oriented tables are supported only by API QueryService."),
                    result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(OlapUpsertOneShard) {
        TestOlapUpsert(1, true);
    }

    Y_UNIT_TEST(OlapUpsertTwoShards) {
        TestOlapUpsert(2, true);
    }

    Y_UNIT_TEST(OlapUpsertDisabled) {
        TestOlapUpsert(2, false);
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
                    PARTITION_COUNT = 1
                );
            )");

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
                    PARTITION_COUNT = 1
                );
            )");

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
                    PARTITION_COUNT = 8
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

    // Unit test for https://github.com/ydb-platform/ydb/issues/7967
    Y_UNIT_TEST(PredicatePushdownNulls) {
        auto settings = TKikimrSettings()
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
        auto settings = TKikimrSettings()
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
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=2", "[[2;\"test_res_2\";#;[\"val2\"]]]");
    }

    Y_UNIT_TEST(BulkUpsertUpdate) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        runnerSettings.SetColumnShardAlterObjectEnabled(true);
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
            auto result = testHelper.GetSession().ExecuteSchemeQuery("ALTER OBJECT `/Root/ColumnTableTest` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(10);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[10]]");
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(110);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
        while (csController->GetCompactionFinishedCounter().Val() < 1) {
            Cout << "Wait compaction..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
    }

    void RunBlockChannelTest(auto blockChannelsMode) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
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
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("return (FromFlow (NarrowMap (ToFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(!plan.QueryStats->Getquery_ast().Contains("WideToBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_EQUAL_C(plan.QueryStats->Getquery_ast().find("WideFromBlocks"), plan.QueryStats->Getquery_ast().rfind("WideFromBlocks"), plan.QueryStats->Getquery_ast());
                    break;
                case NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE:
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(WideSortBlocks"), plan.QueryStats->Getquery_ast());
                    UNIT_ASSERT_C(plan.QueryStats->Getquery_ast().Contains("(FromFlow (NarrowMap (ToFlow (WideFromBlocks"), plan.QueryStats->Getquery_ast());
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

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnIsHard` (a, b, c) VALUES
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
                PRAGMA ydb.OptimizerHints =
                '
                    JoinType(T1 T2 Shuffle)
                ';

                SELECT *
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

    Y_UNIT_TEST(CompactionPlanner) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "180s", "expected_blobs_size" : 2048000},
                               {"class_name" : "Zero", "expected_blobs_size" : 2048000}, {"class_name" : "Zero"}]}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));
        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "120s", "expected_blobs_size" : 2048000},
                               {"class_name" : "Zero", "expected_blobs_size" : 2048000}, {"class_name" : "Zero"}]}`);
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
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "180s", "expected_blobs_size" : 2048000},
                               {"class_name" : "Zero", "expected_blobs_size" : 2048000}, {"class_name" : "Zero"}]}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
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

    Y_UNIT_TEST(CompactionPlannerOneLevelDuplicationsCleaner) {
        auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_count_available" : 0, "portions_live_duration" : "5s", "expected_blobs_size" : 100000},
                               {"class_name" : "Zero", "portions_count_available" : 0, "portions_live_duration" : "10s", "expected_blobs_size" : 200000},
                               {"class_name" : "Zero", "portions_count_available" : 0, "portions_live_duration" : "20s", "expected_blobs_size" : 400000},
                               {"class_name" : "OneLayer"}]}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000, 70000);
        csController->WaitCompactions(TDuration::Seconds(5));
        const TInstant startInstant = TInstant::Now();
        while (TInstant::Now() - startInstant < TDuration::Seconds(100)) {
            auto rows = ExecuteScanQuery(tableClient, R"(
                SELECT level, SUM(records_count) as sum_records_count FROM (
                --!syntax_v1
                SELECT JSON_VALUE(CAST(`Details` AS JsonDocument), '$.level') as level, CAST(JSON_VALUE(CAST(`Details` AS JsonDocument), '$.selectivity.default.records_count') AS Uint64) as records_count, Details
                FROM `/Root/olapStore/olapTable/.sys/primary_index_optimizer_stats`
                )
                GROUP BY level
                ORDER BY level
            )");

            AFL_VERIFY(rows.size() == 4)("count", rows.size());
            if (GetUint64(rows[0].at("sum_records_count")) == 0 && GetUint64(rows[1].at("sum_records_count")) == 0 &&
                GetUint64(rows[2].at("sum_records_count")) == 0 && GetUint64(rows[3].at("sum_records_count")) == 70000) {
                break;
            } else {
                TStringBuilder sb;
                for (auto&& i : rows) {
                    sb << GetUint64(i.at("sum_records_count")) << "/";
                }
                Cerr << sb << Endl;
            }
            Sleep(TDuration::Seconds(1));
        }
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[70000u;]])");
        }
    }

    Y_UNIT_TEST(CompactionPlannerQueryService) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

        {
            auto alterQuery =
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 1, "portions_count_available" : 3},
                               {"class_name" : "Zero", "expected_blobs_size" : 1}]}`);
                )";
            auto result = session.ExecuteQuery(alterQuery, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 300000000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 300100000, 1000);
        csController->WaitCompactions(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(csController->GetCompactionStartedCounter().Val(), 0);

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 300200000, 1000);
        csController->WaitCompactions(TDuration::Seconds(5));
        UNIT_ASSERT_GT(csController->GetCompactionStartedCounter().Val(), 0);
    }

    Y_UNIT_TEST(MetadataMemoryManager) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[20000u;]])");
        }
        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`,
                    `METADATA_MEMORY_MANAGER.FEATURES`=`{"memory_cache_size" : 0}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[20000u;]])");
        }
    }

    Y_UNIT_TEST(NormalizeAbsentColumn) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper testHelper(kikimr);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));

        testHelper.CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD, NKikimrServices::TX_COLUMNSHARD_SCAN }, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column1 Uint64;";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 1000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 1000);

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column2 Uint64;";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

    }

    Y_UNIT_TEST(MultiInsertWithSinks) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

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
            CompareYson(result, R"([[#;#;#;["0"];0u;"a"];[#;#;#;["t"];1000000u;"b"];[#;#;#;["test"];2000000u;"c"]])");
        }
    }

    Y_UNIT_TEST(CountWhereColumnIsNull) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 300, true);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

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
                --!syntax_v1

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
                --!syntax_v1

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
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 300, true);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

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

    Y_UNIT_TEST(ScanFailedSnapshotTooOld) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableColumnShardConfig()->SetMaxReadStaleness_ms(5000);

        TTestHelper testHelper(settings);

        TTestHelper::TColumnTable cnt;
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("key").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("c").SetType(NScheme::NTypeIds::Int32).SetNullable(true)
        };
        cnt.SetName("/Root/cnt").SetPrimaryKey({ "key" }).SetSchema(schema);
        testHelper.CreateTable(cnt);
        Sleep(TDuration::Seconds(10));
        auto client = testHelper.GetKikimr().GetQueryClient();
        auto result =
            client
                .ExecuteQuery(
                    TStringBuilder() << "$v = SELECT CAST(COUNT(*) AS INT32) FROM `/Root/cnt`; INSERT INTO `/Root/cnt` (key, c) values(1, $v);",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(InsertIntoNullablePK) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableColumnShardConfig()->SetAllowNullableColumnsInPK(true);
        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("pk1").SetType(NScheme::NTypeIds::Int64).SetNullable(true),
            TTestHelper::TColumnSchema().SetName("pk2").SetType(NScheme::NTypeIds::Int32).SetNullable(true),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::String).SetNullable(true),
        };
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ttt").SetPrimaryKey({ "pk1", "pk2" }).SetSharding({ "pk1" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        auto client = testHelper.GetKikimr().GetQueryClient();
        const auto result = client
            .ExecuteQuery(
                R"(
                 INSERT INTO `/Root/ttt` (pk1, pk2, value) VALUES
                 (1, 2, "value"),
                 (null, 2, "value"),
                 (1, null, "value"),
                 (null, null, "value")
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx())
            .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        {
            const auto resultSelect = client
                .ExecuteQuery(
                    "SELECT * FROM `/Root/ttt` ORDER BY pk1, pk2",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                .GetValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
            const auto resultSets = resultSelect.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
            const auto resultSet = resultSets[0];
            CompareYson(R"(
                [
                    [#;#;["value"]];
                    [#;[2];["value"]];
                    [[1];#;["value"]];
                    [[1];[2];["value"]]
                ]
            )", FormatResultSetYson(resultSet));
        }
    }

    Y_UNIT_TEST(BulkUpserPredefinedData) {   //test for https://github.com/ydb-platform/ydb/issues/20003
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TTestHelper testHelper(settings);
        auto queryClient = testHelper.GetKikimr().GetQueryClient();
        {
            auto result = queryClient
                              .ExecuteQuery(R"(
                    CREATE TABLE `/Root/arrow/columns` (
                        hash Utf8 NOT NULL,
                        length Int32 NOT NULL,
                        PRIMARY KEY(hash)
                    )
                    WITH (STORE = COLUMN)
                )", NQuery::TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const unsigned char schemaBytes[] = { 0xff, 0xff, 0xff, 0xff, 0xe0, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00,
            0x0e, 0x00, 0x06, 0x00, 0x0d, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x0a, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x60, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x00, 0x18, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x13, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04, 0x00, 0x12, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x04, 0x00, 0x04, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x68, 0x61, 0x73, 0x68, 0x00, 0x00, 0x12, 0x00,
            0x18, 0x00, 0x14, 0x00, 0x13, 0x00, 0x12, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04, 0x00, 0x12, 0x00, 0x00, 0x00, 0x14, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x0c, 0x00, 0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00,
            0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        const auto& serializedSchema = TString{ (const char*)schemaBytes, sizeof(schemaBytes) };

        const unsigned char dataBytes[] = { 0xff, 0xff, 0xff, 0xff, 0xc8, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
            0x00, 0x16, 0x00, 0x0e, 0x00, 0x15, 0x00, 0x10, 0x00, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x03, 0x0a, 0x00, 0x18, 0x00, 0x0c, 0x00, 0x08, 0x00, 0x04, 0x00, 0x0a,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
            0x00, 0x00, 0x00, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

        {
            //write prepared serialized batch, that contains a single row
            const auto& serializedData = TString{ (const char*)dataBytes, sizeof(dataBytes) };
            const auto& arrowSchema = NArrow::DeserializeSchema(serializedSchema);
            const auto& arrowBatch = NArrow::DeserializeBatch(serializedData, arrowSchema);
            auto csHelper = NKikimr::Tests::NCS::THelper{ testHelper.GetKikimr().GetTestServer() };
            csHelper.SendDataViaActorSystem("/Root/arrow/columns", arrowBatch, Ydb::StatusIds::SUCCESS);
        }

        {
            //check that the row is written to the table
            auto queryClient = testHelper.GetKikimr().GetQueryClient();
            auto result = queryClient.ExecuteQuery("SELECT * FROM `/Root/arrow/columns`", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[\"1234567890\";10]]", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(BulkUpsertIntoNotNullableColumn, AddNull) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("hash").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("length").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        const TString tableName = "/Root/ttt";
        testTable.SetName(tableName).SetPrimaryKey({ "hash" }).SetSharding({ "hash" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TTestHelper::TUpdatesBuilder rowsBuilder(testTable.GetArrowSchema(schema));
            for (ui32 i = 0; i < 10; ++i) {
                auto hash = TStringBuilder() << "SomeArtificalHash:" << TString(i, '0');
                if (AddNull && (i == 5)) { //Add single incorrect row
                    rowsBuilder.AddRow().Add(hash.c_str()).AddNull();
                } else {
                    rowsBuilder.AddRow().Add(hash.c_str()).Add(hash.size());
                }
            }
            if (AddNull) {
                testHelper.BulkUpsert(testTable, rowsBuilder, Ydb::StatusIds::BAD_REQUEST,
                    "Cannot write data into shard(Incorrect request: cannot prepare incoming batch: empty field for non-default column: "
                    "'length')");
            } else {
                testHelper.BulkUpsert(testTable, rowsBuilder);
            }
        }
    }

    Y_UNIT_TEST(InsertEmptyString) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableColumnShardConfig()->SetAllowNullableColumnsInPK(true);

        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::String).SetNullable(false),
        };
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ttt").SetPrimaryKey({ "id", }).SetSharding({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        auto client = testHelper.GetKikimr().GetQueryClient();
        const auto result = client
            .ExecuteQuery(
                R"(
                 INSERT INTO `/Root/ttt` (id, value) VALUES
                 (347, '')
                )",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx())
            .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        {
            const auto resultSelect = client
                .ExecuteQuery(
                    "SELECT * FROM `/Root/ttt`",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                .GetValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
            const auto resultSets = resultSelect.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
            const auto resultSet = resultSets[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        }
    }

    Y_UNIT_TEST(DoubleOutOfRangeInJson) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardDoubleOutOfRangeHandling(
            NKikimrConfig::TColumnShardConfig_EJsonDoubleOutOfRangeHandlingPolicy_CAST_TO_INFINITY);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient()
                              .ExecuteQuery(R"(
                CREATE TABLE olapTable (
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
            auto it = client.StreamExecuteScanQuery("SELECT * FROM olapTable WHERE k < 4 ORDER BY k").ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[0u;"\"-inf\""];[1u;"\"inf\""];[2u;"\"inf\""];[3u;"\"-inf\""]])");
        }
    }

    Y_UNIT_TEST(SingleShardRead) {
        std::unique_ptr<TKikimrRunner> Kikimr;
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto kikimr = std::make_unique<TKikimrRunner>(settings);
        Tests::NCommon::TLoggerInit(*kikimr).Initialize();
        auto queryClient = kikimr->GetQueryClient();
        const auto noTx = NQuery::TTxControl::NoTx();
        {
            auto result = queryClient.ExecuteQuery(R"(
                CREATE TABLE Test (
                    Id Uint32 not null,
                    Name String not null,
                    Comment String,
                    PRIMARY KEY (Name, Id)
                ) WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT = 3
                );

            )", noTx).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            result = queryClient.ExecuteQuery(R"(
                UPSERT INTO Test (Id, Name, Comment) VALUES
                    (10,  "n1", "aa"),
                    (20, "n2", "bb"),
                    (30, "n3", "cc"),
                    (40, "n4", "dd"),
                    (50, "n5", "ee")
            )", noTx).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto tableClient = kikimr->GetTableClient();
        auto tableClientSession = tableClient.GetSession().GetValueSync().GetSession();
        auto result = tableClientSession.DescribeTable("/Root/Test", NYdb::NTable::TDescribeTableSettings{}.WithKeyShardBoundary(true).WithShardNodesInfo(true).WithPartitionStatistics(true).WithTableStatistics(true)).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        //TODO USE shard ids from the table description. Not avaiable now
        {
            auto result = queryClient.ExecuteQuery("SELECT * FROM Test WITH TabletId = '72075186224037888'", noTx).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[\"bb\"];20u;\"n2\"];[[\"dd\"];40u;\"n4\"]]", FormatResultSetYson(result.GetResultSet(0)));
            result = queryClient.ExecuteQuery("SELECT * FROM Test WITH TabletId = '72075186224037889'", noTx).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[\"ee\"];50u;\"n5\"]]", FormatResultSetYson(result.GetResultSet(0)));
            result = queryClient.ExecuteQuery("SELECT * FROM Test WITH TabletId = '72075186224037890'", noTx).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[\"aa\"];10u;\"n1\"];[[\"cc\"];30u;\"n3\"]]", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CountWithPredicate) {
        auto runnerSettings = TKikimrSettings().SetWithSampleTables(true);
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("time").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("class").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "time", "class" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(100).Add("test");
            tableInserter.AddRow().Add(ts.MicroSeconds() * 2).Add("test");
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTableTest` WHERE time > CurrentUtcTimestamp()", "[[1u]]");
    }

    Y_UNIT_TEST(WithDefaultValue) {
        std::unique_ptr<TKikimrRunner> Kikimr;
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto kikimr = std::make_unique<TKikimrRunner>(settings);
        Tests::NCommon::TLoggerInit(*kikimr).Initialize();
        auto queryClient = kikimr->GetQueryClient();
        {
            auto result = queryClient.ExecuteQuery(R"(
                CREATE TABLE Test (
                    Id Uint32 not null,
                    Value String DEFAULT "aba",
                    PRIMARY KEY (Id)
                ) WITH (
                    STORE = COLUMN
                );
            )", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Default values are not supported in column tables", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(PredicateWithLimit) {
        auto runnerSettings =
            TKikimrSettings().SetWithSampleTables(true).SetColumnShardAlterObjectEnabled(true).SetColumnShardReaderClassName("SIMPLE");
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("a").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("b").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "a", "b" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(1);
            tableInserter.AddRow().Add(2).Add(2);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT a, b FROM `/Root/ColumnTableTest` WHERE b = 2 LIMIT 2", "[[2u;2u]]");
    }

    Y_UNIT_TEST(SimpleRequestHasProjections) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 20);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT 1
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);

            CompareYson(result, R"([[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1];[1]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT count(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);

            CompareYson(result, R"([[20u]])");
        }
    }

    Y_UNIT_TEST(ReverseMerge) {
        auto runnerSettings =
            TKikimrSettings().SetWithSampleTables(true).SetColumnShardAlterObjectEnabled(true);
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

        TTestHelper testHelper(runnerSettings);
        testHelper.GetKikimr().GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_TRACE);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("k").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("v").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "k" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(0);
            tableInserter.AddRow().Add(2).Add(0);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(0).Add(0);
            tableInserter.AddRow().Add(1).Add(0);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(2).Add(0);
            tableInserter.AddRow().Add(3).Add(0);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData(
            "SELECT k, v FROM (SELECT * FROM `/Root/ColumnTableTest` WHERE k >= 0 AND k < 1000) ORDER BY k DESC LIMIT 3", "[[3u;0u];[2u;0u];[1u;0u]]");
    }

    Y_UNIT_TEST(GroupByWithMakeDatetime) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto querySession = result.GetSession();

        {
            auto result = tableSession.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/query_stat` (
                    ts      Timestamp NOT NULL,
                    folder_id String,
                    primary key(ts)
                )
                PARTITION BY HASH(ts)
                WITH (STORE = COLUMN);
            )").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = querySession.ExecuteQuery(R"(
                    INSERT INTO `/Root/query_stat` (ts, folder_id)
                    VALUES (
                        CurrentUtcTimestamp(),
                        "abc"
                    )
                )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = querySession.ExecuteQuery(R"(
                    SELECT
                        ts1, count(*)
                    FROM
                        query_stat
                    where
                        folder_id not in [ "b1g0gammoel2iuh0hir6" ]
                    GROUP BY DateTime::MakeDatetime(DateTime::StartOf(ts, DateTime::IntervalFromDays(1))) as ts1
                )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropTable) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideMaxReadStaleness(TDuration::Seconds(1));
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 20);
        auto client = kikimr.GetTableClient();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto result = kikimr.GetQueryClient().ExecuteQuery("DROP TABLE `olapStore/olapTable`", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        csController->WaitCleaning(TDuration::Seconds(5));

        {
            auto result = kikimr.GetQueryClient()
                              .ExecuteQuery("SELECT * FROM `olapStore/.sys/store_primary_index_portion_stats`", NQuery::TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_EQUAL(result.GetResultSet(0).RowsCount(), 0);
        }
    }

    Y_UNIT_TEST(OlapTxMode) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            CREATE TABLE `/Root/Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto client = kikimr.GetQueryClient();
        {
            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(
                result.GetIssues().ToString(),
                "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes.",
                result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::OnlineRO()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(
                result.GetIssues().ToString(),
                "Read from column-oriented tables is not supported in Online Read-Only or Stale Read-Only transaction modes.",
                result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SysViewScanAfterDropTable) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 100);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery("DROP TABLE `olapStore/olapTable`", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result =
                client.ExecuteQuery("SELECT * FROM `/Root/olapStore/.sys/store_primary_index_optimizer_stats`", NQuery::TTxControl::NoTx())
                    .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(PredicateWithLimitLostRecords) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(R"(
                    CREATE TABLE IF NOT EXISTS `olap_table` (
                        id Uint64 NOT NULL,
                        message Utf8,
                        PRIMARY KEY (id)
                    )
                    WITH (
                        STORE = COLUMN,
                        PARTITION_COUNT = 1
                    );
                )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString query = "UPSERT INTO `olap_table` (id, message) VALUES (1, '2'), (2, '2');";
            auto status = queryClient.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(R"(
                SELECT id FROM `olap_table`
                WHERE id = 2 AND message = '2'
                LIMIT 1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            TString result = FormatResultSetYson(status.GetResultSet(0));

            CompareYson(result, R"([[2u]])");
        }
    }

    Y_UNIT_TEST(PredicateWithTimestampParameter) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();

        {
            const TString query = R"(
                CREATE TABLE `/Root/tmp_olap` (
                    Key Uint32 NOT NULL,
                    Value Timestamp NOT NULL,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = COLUMN
                );
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const TString query = R"(
                INSERT INTO `/Root/tmp_olap` (Key, Value) VALUES
                (1, Timestamp('2021-01-01T00:00:00Z'))
            )";

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const TString query = R"(
                DECLARE $flag1 AS Timestamp;
                SELECT * FROM tmp_olap WHERE Value >= $flag1;
            )";

            auto params = TParamsBuilder()
                .AddParam("$flag1")
                    .Timestamp(TInstant::ParseIso8601("2021-01-01T00:00:00Z"))
                    .Build()
                .Build();

            auto result = client.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx(), params).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetResultSet(0).RowsCount() == 1, result.GetIssues().ToString());
        }
    }
}
}
