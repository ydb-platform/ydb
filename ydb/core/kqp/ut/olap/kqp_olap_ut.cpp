#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_long_tx.h>

#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/formats/ssa_runtime_version.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/datashard_ut_common.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/cs_helper.h>
#include <util/system/sanitizers.h>

#include <fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace NActors;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest,
    Ydb::Table::BulkUpsertResponse>;

Y_UNIT_TEST_SUITE(KqpOlap) {
    void EnableDebugLogging(NActors::TTestActorRuntime* runtime) {
        //runtime->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        // runtime->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
    }

    void EnableDebugLogging(TKikimrRunner& kikimr) {
        EnableDebugLogging(kikimr.GetTestServer().GetRuntime());
    }

    class TLocalHelper: public Tests::NCS::THelper {
    private:
        using TBase = Tests::NCS::THelper;
    public:
        void CreateTestOlapTable(TString tableName = "olapTable", TString storeName = "olapStore",
            ui32 storeShardsCount = 4, ui32 tableShardsCount = 3,
            TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();
            CreateTestOlapStore(sender, Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                SchemaPresets {
                    Name: "default"
                    Schema {
                        %s
                    }
                }
            )", storeName.c_str(), storeShardsCount, GetTestTableSchema().data()));

            TString shardingColumns = "[\"timestamp\", \"uid\"]";
            if (shardingFunction != "HASH_FUNCTION_CLOUD_LOGS") {
                shardingColumns = "[\"uid\"]";
            }

            TBase::CreateTestOlapTable(sender, storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            })", tableName.c_str(), tableShardsCount, shardingFunction.c_str(), shardingColumns.c_str()));
        }
        using TBase::TBase;
        TLocalHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {

        }
    };

    class TClickHelper : public Tests::NCS::TCickBenchHelper {
    private:
        using TBase = Tests::NCS::TCickBenchHelper;
    public:
        using TBase::TBase;

        TClickHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {}

        void CreateClickBenchTable(TString tableName = "benchTable", ui32 shardsCount = 4) {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();

            TBase::CreateTestOlapTable(sender, "", Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                Schema {
                    %s
                }
                Sharding {
                    HashSharding {
                        Function: HASH_FUNCTION_MODULO_N
                        Columns: "EventTime"
                    }
                })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
        }
    };

    class TTableWithNullsHelper : public Tests::NCS::TTableWithNullsHelper {
    private:
        using TBase = Tests::NCS::TTableWithNullsHelper;
    public:
        using TBase::TBase;

        TTableWithNullsHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {}

        void CreateTableWithNulls(TString tableName = "tableWithNulls", ui32 shardsCount = 4) {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();

            TBase::CreateTestOlapTable(sender, "", Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                Schema {
                    %s
                }
                Sharding {
                    HashSharding {
                        Function: HASH_FUNCTION_MODULO_N
                        Columns: "id"
                    }
                })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
        }
    };

    void WriteTestData(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        UNIT_ASSERT(testTable != "/Root/benchTable"); // TODO: check schema instead

        TLocalHelper lHelper(kikimr.GetTestServer());
        NYdb::NLongTx::TClient client(kikimr.GetDriver());

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resBeginTx.Status().GetStatus(), EStatus::SUCCESS, resBeginTx.Status().GetIssues().ToString());

        auto txId = resBeginTx.GetResult().tx_id();
        auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        TString data = NArrow::SerializeBatchNoCompression(batch);

        NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, testTable, txId, data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resWrite.Status().GetStatus(), EStatus::SUCCESS, resWrite.Status().GetIssues().ToString());

        NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resCommitTx.Status().GetStatus(), EStatus::SUCCESS, resCommitTx.Status().GetIssues().ToString());
    }

    void WriteTestDataForClickBench(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        UNIT_ASSERT(testTable == "/Root/benchTable"); // TODO: check schema instead

        TClickHelper lHelper(kikimr.GetTestServer());
        NYdb::NLongTx::TClient client(kikimr.GetDriver());

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resBeginTx.Status().GetStatus(), EStatus::SUCCESS, resBeginTx.Status().GetIssues().ToString());

        auto txId = resBeginTx.GetResult().tx_id();
        auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        TString data = NArrow::SerializeBatchNoCompression(batch);

        NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, testTable, txId, data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resWrite.Status().GetStatus(), EStatus::SUCCESS, resWrite.Status().GetIssues().ToString());

        NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resCommitTx.Status().GetStatus(), EStatus::SUCCESS, resCommitTx.Status().GetIssues().ToString());
    }

    void WriteTestDataForTableWithNulls(TKikimrRunner& kikimr, TString testTable) {
        UNIT_ASSERT(testTable == "/Root/tableWithNulls"); // TODO: check schema instead
        TTableWithNullsHelper lHelper(kikimr.GetTestServer());
        NYdb::NLongTx::TClient client(kikimr.GetDriver());

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resBeginTx.Status().GetStatus(), EStatus::SUCCESS, resBeginTx.Status().GetIssues().ToString());

        auto txId = resBeginTx.GetResult().tx_id();
        auto batch = lHelper.TestArrowBatch();
        TString data = NArrow::SerializeBatchNoCompression(batch);

        NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, testTable, txId, data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resWrite.Status().GetStatus(), EStatus::SUCCESS, resWrite.Status().GetIssues().ToString());

        NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resCommitTx.Status().GetStatus(), EStatus::SUCCESS, resCommitTx.Status().GetIssues().ToString());
    }

    TVector<THashMap<TString, NYdb::TValue>> CollectRows(NYdb::NTable::TScanQueryPartIterator& it) {
        TVector<THashMap<TString, NYdb::TValue>> rows;

        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(),
                "Unexpected empty scan query response.");

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                NYdb::TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    THashMap<TString, NYdb::TValue> row;
                    for (size_t ci = 0; ci < resultSet.ColumnsCount(); ++ci) {
                        row.emplace(resultSet.GetColumnsMeta()[ci].Name, rsParser.GetValue(ci));
                    }
                    rows.emplace_back(std::move(row));
                }
            }
        }
        return rows;
    }

    void PrintValue(IOutputStream& out, const NYdb::TValue& v) {
        NYdb::TValueParser value(v);

        while (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            if (value.IsNull()) {
                out << "<NULL>";
                return;
            } else {
                value.OpenOptional();
            }
        }

        switch (value.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Uint32: {
                out << value.GetUint32();
                break;
            }
            case NYdb::EPrimitiveType::Uint64: {
                out << value.GetUint64();
                break;
            }
            case NYdb::EPrimitiveType::Utf8: {
                out << value.GetUtf8();
                break;
            }
            case NYdb::EPrimitiveType::Timestamp: {
                out << value.GetTimestamp();
                break;
            }
            default: {
                UNIT_ASSERT_C(false, "PrintValue not iplemented for this type");
            }
        }
    }

    void PrintRow(IOutputStream& out, const THashMap<TString, NYdb::TValue>& fields) {
        for (const auto& f : fields) {
            out << f.first << ": ";
            PrintValue(out, f.second);
            out << " ";
        }
    }

    void PrintRows(IOutputStream& out, const TVector<THashMap<TString, NYdb::TValue>>& rows) {
        for (const auto& r : rows) {
            PrintRow(out, r);
            out << "\n";
        }
    }

    TVector<THashMap<TString, NYdb::TValue>> ExecuteScanQuery(NYdb::NTable::TTableClient& tableClient, const TString& query) {
        Cerr << "====================================\n"
            << "QUERY:\n" << query
            << "\n\nRESULT:\n";

        TStreamExecScanQuerySettings scanSettings;
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        auto rows = CollectRows(it);

        PrintRows(Cerr, rows);
        Cerr << "\n";

        return rows;
    }

    ui64 GetUint32(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalUint32();
        } else {
            return value.GetUint32();
        }
    }

    ui64 GetUint64(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalUint64();
        } else {
            return value.GetUint64();
        }
    }

    TInstant GetTimestamp(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalTimestamp();
        } else {
            return value.GetTimestamp();
        }
    }

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

    void CheckPlanForAggregatePushdown(const TString& query, NYdb::NTable::TTableClient& tableClient, const std::vector<std::string>& planNodes,
        const std::string& readNodeType)
    {
        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        auto res = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto planRes = CollectStreamResult(res);
        auto ast = planRes.QueryStats->Getquery_ast();
        Cerr << "JSON Plan:" << Endl;
        Cerr << planRes.PlanJson.GetOrElse("NO_PLAN") << Endl;
        Cerr << "AST:" << Endl;
        Cerr << ast << Endl;
        for (auto planNode : planNodes) {
            UNIT_ASSERT_C(ast.find(planNode) != std::string::npos,
                TStringBuilder() << planNode << " was not found. Query: " << query);
        }
        UNIT_ASSERT_C(ast.find("SqueezeToDict") == std::string::npos, TStringBuilder() << "SqueezeToDict denied for aggregation requests. Query: " << query);

        if (!readNodeType.empty()) {
            NJson::TJsonValue planJson;
            NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true);
            auto readNode = FindPlanNodeByKv(planJson, "Node Type", readNodeType.c_str());
            UNIT_ASSERT(readNode.IsDefined());

            auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
            for (auto& op : operators) {
                if (op.GetMapSafe().at("Name") == "TableFullScan") {
                    auto ssaProgram = op.GetMapSafe().at("SsaProgram");
                    UNIT_ASSERT(ssaProgram.IsDefined());
                    UNIT_ASSERT(FindPlanNodes(ssaProgram, "Projection").size());
                    break;
                }
            }
        }
    }

    Y_UNIT_TEST(SimpleQueryOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        // EnableDebugLogging(kikimr);

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

    Y_UNIT_TEST(SimpleLookupOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

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

        // EnableDebugLogging(kikimr);

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

        // EnableDebugLogging(kikimr);

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

        // EnableDebugLogging(kikimr);

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

        // EnableDebugLogging(kikimr);

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

    Y_UNIT_TEST(Aggregation) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();

        // EnableDebugLogging(kikimr);

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
            CompareYson(result, R"([[0u;]])");
        }

        // EnableDebugLogging(kikimr);

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        // EnableDebugLogging(kikimr);

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*), MAX(`resource_id`), MAX(`timestamp`), MIN(LENGTH(`message`))
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;["40999"];[3004999u];[1036u]]])");
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
            CompareYson(result, R"([[23000u;]])");
        }
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
                    ["uid_1000005"]
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

        // EnableDebugLogging(kikimr);

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

        node = FindPlanNodeByKv(plan, "Node Type", "Limit-TableFullScan");
        UNIT_ASSERT(node.IsDefined());
        reverse = FindPlanNodeByKv(node, "Reverse", "false");
        UNIT_ASSERT(!reverse.IsDefined());
        pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
        UNIT_ASSERT(pushedLimit.IsDefined());

        // Check that Reverse flag is set in query plan
        it = tableClient.StreamExecuteScanQuery(selectQueryWithSort, scanSettings).GetValueSync();
        result = CollectStreamResult(it);

        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        Cerr << "==============================" << Endl;
        Cerr << *result.PlanJson << Endl;
        Cerr << result.QueryStats->query_plan() << Endl;
        Cerr << result.QueryStats->query_ast() << Endl;

        node = FindPlanNodeByKv(plan, "Node Type", "Limit-TableFullScan");
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

    Y_UNIT_TEST(ExtractRanges) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp`;
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
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        // EnableDebugLogging(kikimr);

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
                ORDER BY `timestamp` DESC;
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

    Y_UNIT_TEST(PredicatePushdown) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        // TODO: Add support for DqPhyPrecompute push-down: Cast((2+2) as Uint64)
        std::vector<TString> testData = {
            R"(`resource_id` = `uid`)",
            R"(`resource_id` = "10001")",
            R"(`level` = 1)",
            R"(`level` = Int8("1"))",
            R"(`level` = Int16("1"))",
            R"(`level` = Int32("1"))",
            R"((`level`, `uid`, `resource_id`) = (Int32("1"), "uid_3000001", "10001"))",
            R"(`level` > Int32("3"))",
            R"(`level` < Int32("1"))",
            R"(`level` >= Int32("4"))",
            R"(`level` <= Int32("0"))",
            R"(`level` != Int32("0"))",
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
            R"(`timestamp` >= CAST(3000001u AS Timestamp) AND `level` > 3)",
            R"((`level`, `uid`) > (Int32("2"), "uid_3000004") OR (`level`, `uid`) < (Int32("1"), "uid_3000002"))",
            R"(Int32("3") > `level`)",
            R"((Int32("1"), "uid_3000001", "10001") = (`level`, `uid`, `resource_id`))",
            R"((Int32("1"), `uid`, "10001") = (`level`, "uid_3000001", `resource_id`))",
            R"(`level` = 0 AND "uid_3000000" = `uid`)",
            R"(`uid` > `resource_id`)",
            R"(`level` IS NULL)",
            R"(`level` IS NOT NULL)",
            R"((`level`, `uid`) > (Int32("1"), NULL))",
            R"((`level`, `uid`) != (Int32("1"), NULL))",
            R"(`level` >= CAST("2" As Int32))",
            R"(CAST("2" As Int32) >= `level`)",
            R"(`timestamp` >= CAST(3000001u AS Timestamp))",
            R"((`timestamp`, `level`) >= (CAST(3000001u AS Timestamp), 3))",
#if SSA_RUNTIME_VERSION >= 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
        };

        std::vector<TString> testDataNoPush = {
            R"(`level` != NULL)",
            R"(`level` > NULL)",
            R"(`timestamp` >= CAST(3000001 AS Timestamp))",
            R"(`level` >= CAST("2" As Uint32))",
            R"(`level` = NULL)",
            R"(`level` > NULL)",
            R"(LENGTH(`uid`) > 0 OR `resource_id` = "10001")",
            R"((LENGTH(`uid`) > 0 AND `resource_id` = "10001") OR `resource_id` = "10002")",
            R"((LENGTH(`uid`) > 0 OR `resource_id` = "10002") AND (LENGTH(`uid`) < 15 OR `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 AND `resource_id` = "10001"))",
            // Not strict function in the beginning causes to disable pushdown
            R"(Unwrap(`level`/1) = `level` AND `resource_id` = "10001")",
            // We can handle this case in future
            R"(NOT(LENGTH(`uid`) > 0 OR `resource_id` = "10001"))",
#if SSA_RUNTIME_VERSION < 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
        };

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

        auto buildQuery = [](const TString& predicate, bool pushEnabled) {
            TStringBuilder qBuilder;

            qBuilder << "--!syntax_v1" << Endl;

            if (!pushEnabled) {
                qBuilder << R"(PRAGMA Kikimr.OptEnableOlapPushdown = "false";)" << Endl;
            }

            qBuilder << R"(PRAGMA Kikimr.OptEnablePredicateExtract = "false";)" << Endl;
            qBuilder << "SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE ";
            qBuilder << predicate;
            qBuilder << " ORDER BY `timestamp`";

            return TString(qBuilder);
        };

        for (const auto& predicate: testData) {
            auto normalQuery = buildQuery(predicate, false);
            auto pushQuery = buildQuery(predicate, true);

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

        for (const auto& predicate: testDataNoPush) {
            auto pushQuery = buildQuery(predicate, true);

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

        for (const auto& predicate: testDataPartialPush) {
            auto normalQuery = buildQuery(predicate, false);
            auto pushQuery = buildQuery(predicate, true);

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
        EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        std::vector< std::pair<TString, TString> > secondLvlFilters = {
            { R"(`uid` LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` NOT LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` LIKE "uid%")", "TableFullScan" },
            { R"(`uid` LIKE "%001")", "TableFullScan" },
            { R"(`uid` LIKE "uid%001")", "Filter-TableFullScan" }, // We have filter (Size >= 6)
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
                    auto ssa = op.GetMapSafe().at("SsaProgram").GetStringRobust();
                    int filterCmdCount = 0;
                    std::string::size_type pos = 0;
                    std::string filterCmd = R"("Filter":{)";
                    while ((pos = ssa.find(filterCmd, pos)) != std::string::npos) {
                        ++filterCmdCount;
                        pos += filterCmd.size();
                    }
                    UNIT_ASSERT_EQUAL(filterCmdCount, 2);
                }
            }
        }
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
        EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(
            PRAGMA Kikimr.OptEnablePredicateExtract = "false";
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                `resource_id` = "10001" AND Unwrap(`level`/1) = `level` AND `level` > 1;
        )";

        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find(R"("eq" '"resource_id")") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find(R"("gt" '"level")") == std::string::npos,
                          TStringBuilder() << "Predicate pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << query);
    }

    Y_UNIT_TEST(AggregationCountPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = R"(
                --!syntax_v1
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
            )";
            auto opStartTime = Now();
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << "!!!\nPushdown query execution time: " << (Now() - opStartTime).MilliSeconds() << "\n!!!\n";
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST(AggregationCountGroupByPushdown) {
        // Should be fixed in KIKIMR-17007
        return;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = R"(
                --!syntax_v1
                SELECT
                    level, COUNT(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )";
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "WideCombiner" }, "TableFullScan");
//            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdown, UseLlvm) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = fmt::format(R"(
                --!syntax_v1
                PRAGMA ydb.EnableLlvm = "{}";

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )", UseLlvm ? "true" : "false");
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "Condense" }, "");
#endif
        }
    }

    Y_UNIT_TEST(CountAllNoPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
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
            CompareYson(result, R"([[23000u;]])");
        }
    }

    class TExpectedLimitChecker {
    private:
        std::optional<ui32> ExpectedLimit;
        std::optional<ui32> ExpectedResultCount;
        ui32 CheckScanData = 0;
        ui32 CheckScanTask = 0;
    public:
        TExpectedLimitChecker& SetExpectedLimit(const ui32 value) {
            ExpectedLimit = value;
            ExpectedResultCount = value;
            return *this;
        }
        TExpectedLimitChecker& SetExpectedResultCount(const ui32 value) {
            ExpectedResultCount = value;
            return *this;
        }
        bool CheckExpectedLimitOnScanData(const ui32 resultCount) {
            if (!ExpectedResultCount) {
                return true;
            }
            ++CheckScanData;
            UNIT_ASSERT_LE(resultCount, *ExpectedResultCount);
            return true;
        }
        bool CheckExpectedLimitOnScanTask(const ui32 taskLimit) {
            if (!ExpectedLimit) {
                return true;
            }
            ++CheckScanTask;
            UNIT_ASSERT_EQUAL(taskLimit, *ExpectedLimit);
            return true;
        }
        bool CheckFinish() const {
            if (!ExpectedLimit) {
                return true;
            }
            return CheckScanData && CheckScanTask;
        }
    };

    class TExpectedRecordChecker {
    private:
        std::optional<ui32> ExpectedColumnsCount;
        ui32 CheckScanData = 0;
    public:
        TExpectedRecordChecker& SetExpectedColumnsCount(const ui32 value) {
            ExpectedColumnsCount = value;
            return *this;
        }
        bool CheckExpectedOnScanData(const ui32 columnsCount) {
            if (!ExpectedColumnsCount) {
                return true;
            }
            ++CheckScanData;
            UNIT_ASSERT_EQUAL(columnsCount, *ExpectedColumnsCount);
            return true;
        }
        bool CheckFinish() const {
            if (!ExpectedColumnsCount) {
                return true;
            }
            return CheckScanData;
        }
    };

    class TAggregationTestCase {
    private:
        TString Query;
        TString ExpectedReply;
        std::vector<std::string> ExpectedPlanOptions;
        bool Pushdown = true;
        std::string ExpectedReadNodeType;
        TExpectedLimitChecker LimitChecker;
        TExpectedRecordChecker RecordChecker;
    public:
        void FillExpectedAggregationGroupByPlanOptions() {
#if SSA_RUNTIME_VERSION >= 2U
//            AddExpectedPlanOptions("TKqpOlapAgg");
            AddExpectedPlanOptions("WideCombiner");
#else
            AddExpectedPlanOptions("CombineCore");
#endif
        }
        TString GetFixedQuery() const {
            TStringBuilder queryFixed;
            queryFixed << "--!syntax_v1" << Endl;
            if (!Pushdown) {
                queryFixed << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }
            queryFixed << "PRAGMA Kikimr.OptUseFinalizeByKey;" << Endl;

            queryFixed << Query << Endl;
            Cerr << "REQUEST:\n" << queryFixed << Endl;
            return queryFixed;
        }
        TAggregationTestCase() = default;
        TExpectedLimitChecker& MutableLimitChecker() {
            return LimitChecker;
        }
        TExpectedRecordChecker& MutableRecordChecker() {
            return RecordChecker;
        }
        bool GetPushdown() const {
            return Pushdown;
        }
        TAggregationTestCase& SetPushdown(const bool value = true) {
            Pushdown = value;
            return *this;
        }
        bool CheckFinished() const {
            return LimitChecker.CheckFinish();
        }

        const TString& GetQuery() const {
            return Query;
        }
        TAggregationTestCase& SetQuery(const TString& value) {
            Query = value;
            return *this;
        }
        const TString& GetExpectedReply() const {
            return ExpectedReply;
        }
        TAggregationTestCase& SetExpectedReply(const TString& value) {
            ExpectedReply = value;
            return *this;
        }
        TAggregationTestCase& AddExpectedPlanOptions(const std::string& value) {
            ExpectedPlanOptions.emplace_back(value);
            return *this;
        }
        const std::vector<std::string>& GetExpectedPlanOptions() const {
            return ExpectedPlanOptions;
        }

        TAggregationTestCase& SetExpectedReadNodeType(const std::string& value) {
            ExpectedReadNodeType = value;
            return *this;
        }

        const std::string& GetExpectedReadNodeType() const {
            return ExpectedReadNodeType;
        }
    };

    void TestAggregationsBase(const std::vector<TAggregationTestCase>& cases) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            {
                auto it = tableClient.StreamExecuteScanQuery(queryFixed).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);
                if (!i.GetExpectedReply().empty()) {
                    CompareYson(result, i.GetExpectedReply());
                }
            }
            CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    }

    void TestAggregationsInternal(const std::vector<TAggregationTestCase>& cases) {
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
        EnableDebugLogging(runtime);

        ui32 numShards = 1;
        ui32 numIterations = 10;
        TLocalHelper(*server).CreateTestOlapTable("olapTable", "olapStore", numShards, numShards);
        const ui32 iterationPackSize = 2000;
        for (ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/olapStore/olapTable", 0, 1000000 + i * 1000000, iterationPackSize);
        }

        TAggregationTestCase currentTest;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpComputeEvents::EvScanData:
                {
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    Y_VERIFY(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_rows() : 0));
                    Y_VERIFY(currentTest.MutableRecordChecker().CheckExpectedOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_columns() : 0));
                    break;
                }
                case TEvDataShard::EvKqpScan:
                {
                    auto* msg = ev->Get<TEvDataShard::TEvKqpScan>();
                    Y_VERIFY(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanTask(msg->Record.GetItemsLimit()));
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime->SetObserverFunc(captureEvents);

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            currentTest = i;
            auto streamSender = runtime->AllocateEdgeActor();
            SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, queryFixed, false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
            Y_VERIFY(currentTest.CheckFinished());
        }
    }

    void TestAggregations(const std::vector<TAggregationTestCase>& cases) {
        TestAggregationsBase(cases);
        TestAggregationsInternal(cases);
    }

    void TestClickBenchBase(const std::vector<TAggregationTestCase>& cases) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
        TClickHelper(kikimr).CreateClickBenchTable();
        auto tableClient = kikimr.GetTableClient();


        ui32 numIterations = 10;
        const ui32 iterationPackSize = 2000;
        for (ui64 i = 0; i < numIterations; ++i) {
            WriteTestDataForClickBench(kikimr, "/Root/benchTable", 0, 1000000 + i * 1000000, iterationPackSize);
        }

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            {
                auto it = tableClient.StreamExecuteScanQuery(queryFixed).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);
                if (!i.GetExpectedReply().empty()) {
                    CompareYson(result, i.GetExpectedReply());
                }
            }
            CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    }

    void TestClickBenchInternal(const std::vector<TAggregationTestCase>& cases) {
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
        EnableDebugLogging(runtime);

        TClickHelper(*server).CreateClickBenchTable();

        // write data

        ui32 numIterations = 10;
        const ui32 iterationPackSize = 2000;
        for (ui64 i = 0; i < numIterations; ++i) {
            TClickHelper(*server).SendDataViaActorSystem("/Root/benchTable", 0, 1000000 + i * 1000000,
                                                         iterationPackSize);
        }

        TAggregationTestCase currentTest;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpComputeEvents::EvScanData:
                {
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    Y_VERIFY(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_rows() : 0));
                    Y_VERIFY(currentTest.MutableRecordChecker().CheckExpectedOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_columns() : 0));
                    break;
                }
                case TEvDataShard::EvKqpScan:
                {
                    auto* msg = ev->Get<TEvDataShard::TEvKqpScan>();
                    Y_VERIFY(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanTask(msg->Record.GetItemsLimit()));
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime->SetObserverFunc(captureEvents);

        // selects

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            currentTest = i;
            auto streamSender = runtime->AllocateEdgeActor();
            SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, queryFixed, false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
            Y_VERIFY(currentTest.CheckFinished());
        }
    }

    void TestClickBench(const std::vector<TAggregationTestCase>& cases) {
        TestClickBenchBase(cases);
        TestClickBenchInternal(cases);
    }

    void TestTableWithNulls(const std::vector<TAggregationTestCase>& cases) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            {
                auto it = tableClient.StreamExecuteScanQuery(queryFixed).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);
                if (!i.GetExpectedReply().empty()) {
                    CompareYson(result, i.GetExpectedReply());
                }
            }
            CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    }

    Y_UNIT_TEST(Filter_NotAllUsedFieldsInResultSet) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultDistinctCountRI_GroupByL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(DISTINCT resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]]")
            ;
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountAll_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE level = 2
                )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("Condense")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountL_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountT_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(timestamp)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("CombineCore")
            .AddExpectedPlanOptions("KqpOlapFilter")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultL_FilterL_OrderL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level > 1
                ORDER BY level
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountExpr) {
        NColumnShard::TLimits::SetMaxBlobSize(10000);
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(level + 2)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[0u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[5u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;1u];[5;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;0u];[7;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;1u];[6;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNull) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNullMix) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u];[[5];1u;1u;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_NoPushdownOnDisabledEmitAggApply) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    PRAGMA DisableEmitAggApply;
                    SELECT
                        COUNT(level)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(AggregationAndFilterPushdownOnDiffCols) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(`timestamp`)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level), MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[2.];[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[3.]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4.]];[5;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5.]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#];[[5];5.;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[46000;]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[15]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_SumL_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[0]];[[1];[4600]];[[2];[9200]];[[3];[13800]];[[4];[18400]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MAX(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[4]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MIN(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"10000\"]];[[1];[\"10001\"]];[[2];[\"10002\"]];[[3];[\"10003\"]];[[4];[\"10004\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MAX(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"40995\"]];[[1];[\"40996\"]];[[2];[\"40997\"]];[[3];[\"40998\"]];[[4];[\"40999\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ProjectionOrder) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    resource_id, level, count(*) as c
                FROM `/Root/olapStore/olapTable`
                GROUP BY resource_id, level
                ORDER BY c, resource_id DESC LIMIT 3
            )")
            .SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        testCase.FillExpectedAggregationGroupByPlanOptions();
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id=1
            )")
            .SetExpectedReply("[[[1]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id > 5
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 5 AND 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id = 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(ClickBenchSmoke) {
        TAggregationTestCase q7;
        q7.SetQuery(R"(
                SELECT
                    AdvEngineID, COUNT(*) as c
                FROM `/Root/benchTable`
                WHERE AdvEngineID != 0
                GROUP BY AdvEngineID
                ORDER BY c DESC
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        ;
        q7.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q9;
        q9.SetQuery(R"(
                SELECT
                    RegionID, SUM(AdvEngineID), COUNT(*) AS c, avg(ResolutionWidth), COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                GROUP BY RegionID
                ORDER BY c DESC
                LIMIT 10
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q9.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q12;
        q12.SetQuery(R"(
                SELECT
                    SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q12.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q14;
        q14.SetQuery(R"(
                SELECT
                    SearchEngineID, SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchEngineID, SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q14.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q22;
        q22.SetQuery(R"(
                SELECT
                    SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q22.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q39;
        q39.SetQuery(R"(
                SELECT TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, COUNT(*) AS PageViews
                FROM `/Root/benchTable`
                WHERE CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0
                GROUP BY
                    TraficSourceID, SearchEngineID, AdvEngineID, IF (SearchEngineID = 0 AND AdvEngineID = 0, Referer, '') AS Src,
                    URL AS Dst
                ORDER BY PageViews DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("Aggregate-Filter-TableFullScan");
        q39.FillExpectedAggregationGroupByPlanOptions();

        TestClickBench({ q7, q9, q12, q14, q22, q39 });
    }

    Y_UNIT_TEST(StatsSysView) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        TLocalHelper(kikimr).CreateTestOlapTable();
        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 1000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT *
            FROM `/Root/olapStore/.sys/store_primary_index_stats`
            ORDER BY PathId, Kind, TabletId
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), numKinds*3);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[0].at("Kind")), 1ull);
        UNIT_ASSERT_GE(GetUint64(rows[0].at("TabletId")), 72075186224037888ull);
        UNIT_ASSERT_GE(GetUint64(rows[1].at("TabletId")), GetUint64(rows[0].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[2].at("Kind")), 1ull);
        UNIT_ASSERT_GE(GetUint64(rows[2].at("TabletId")), GetUint64(rows[1].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[6].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[6].at("Kind")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[6].at("TabletId")), GetUint64(rows[0].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[7].at("Kind")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[7].at("TabletId")), GetUint64(rows[1].at("TabletId")));
        UNIT_ASSERT_GE(
            GetUint64(rows[0].at("Rows")) + GetUint64(rows[1].at("Rows")) + GetUint64(rows[2].at("Rows")) +
            GetUint64(rows[3].at("Rows")) + GetUint64(rows[4].at("Rows")) + GetUint64(rows[5].at("Rows")) +
            GetUint64(rows[6].at("Rows")) + GetUint64(rows[7].at("Rows")) + GetUint64(rows[8].at("Rows")),
            0.3*0.9*100*1000); // >= 90% of 100K inserted rows
    }

    Y_UNIT_TEST(StatsSysViewTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 2000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                ORDER BY PathId, Kind, TabletId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GT(rows.size(), 1*numKinds);
            UNIT_ASSERT_LE(rows.size(), 3*numKinds);
            UNIT_ASSERT_VALUES_EQUAL(rows.size() % numKinds, 0);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 3ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_2/.sys/primary_index_stats`
                ORDER BY PathId, Kind, TabletId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GT(rows.size(), 1*numKinds);
            UNIT_ASSERT_LE(rows.size(), 3*numKinds);
            UNIT_ASSERT_VALUES_EQUAL(rows.size() % numKinds, 0);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 4ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 4ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                WHERE
                    PathId > UInt64("3")
                ORDER BY PathId, Kind, TabletId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
        }
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
        EnableDebugLogging(runtime);

        const ui32 numShards = 10;
        const ui32 numIterations = 10;
        TLocalHelper(*server).CreateTestOlapTable("selectTable", "selectStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/selectStore/selectTable", 0, 1000000 + i*1000000, 2000);
        }

        ui64 result = 0;

        std::vector<TAutoPtr<IEventHandle>> evs;
        ui32 num = 0;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
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
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT * FROM `/Root/selectStore/selectTable` LIMIT 1;", false));
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
        EnableDebugLogging(runtime);

        ui32 numShards = NSan::PlainOrUnderSanitizer(1000, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(50, 10);
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
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
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
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
        EnableDebugLogging(runtime);

        const ui32 numShards = 10;
        const ui32 numIterations = 50;
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
        }

        bool hasResult = false;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
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
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT * FROM `/Root/largeOlapStore/largeOlapTable` where resource_id = Utf8(\"notfound\");", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT(hasResult);
    }

    Y_UNIT_TEST(GranulesInShard) {
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
        EnableDebugLogging(runtime);

        ui32 numShards = 1;
        ui32 numIterations = 100;
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;
        const ui32 iterationPackSize = 2000;
        for (ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i * 1000000, iterationPackSize);
            insertRows += iterationPackSize;
        }

        ui64 result = 0;
        bool testProcessed = false;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpComputeEvents::EvCostData:
                {

                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvCostData>();
                    if (msg->GetTableRanges().GetMarksCount()) {
                        Y_VERIFY(msg->GetSerializedTableRanges(1).size() == 1);
                        Y_VERIFY(msg->GetSerializedTableRanges(2).size() == 2);
                        testProcessed = true;
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus:
                {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId] : msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData:
                {
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
        const TInstant start = Now();
        while (Now() - start < TDuration::Seconds(20) && !testProcessed) {
            SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
            UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(testProcessed);
    }

    Y_UNIT_TEST(ManyColumnShardsWithRestarts) {
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
        EnableDebugLogging(runtime);

        ui32 numShards = NSan::PlainOrUnderSanitizer(100, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(100, 10);
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;

        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        THashSet<TActorId> columnShardScans;
        bool prevIsFinished = false;

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> auto {
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
                            Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": hijack event");
                            Cerr.Flush();
                            auto resp = std::make_unique<NKqp::TEvKqpCompute::TEvScanError>(msg->Generation);
                            runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, resp.release()));
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
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST(StatsSysViewColumns) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 2000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT TabletId, PathId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY PathId, Kind, TabletId
                LIMIT 4;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[0].at("Kind")), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[3].at("Kind")), 2ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT Bytes, Rows
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY Bytes
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3*numKinds);
            UNIT_ASSERT_LE(GetUint64(rows[0].at("Bytes")), GetUint64(rows[1].at("Bytes")));
        }
        {
            auto selectQuery = TString(R"(
                SELECT Rows, Kind, RawBytes, Rows as Rows2, Rows as Rows3, PathId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY PathId, Kind, Rows3
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3*numKinds);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("Rows2")), GetUint64(rows[0].at("Rows3")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("Rows")), GetUint64(rows[1].at("Rows3")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("Rows")), GetUint64(rows[2].at("Rows2")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[5].at("Rows")), GetUint64(rows[5].at("Rows3")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[11].at("Rows")), GetUint64(rows[11].at("Rows2")));
        }
    }

    Y_UNIT_TEST(StatsSysViewRanges) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 3000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 5000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Kind < UInt32("4")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3*3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[0].at("Kind")), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[2].at("Kind")), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[8].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[8].at("Kind")), 3ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY
                    PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 3*3*numKinds;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[0].at("Kind")), numKinds);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected-1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[numExpected-1].at("Kind")), 1ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId > UInt64("0") AND PathId < UInt32("4")
                    OR PathId > UInt64("4") AND PathId <= UInt64("5")
                ORDER BY
                    PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 2*3*numKinds;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[0].at("Kind")), numKinds);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected-1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint32(rows[numExpected-1].at("Kind")), 1ull);
        }
    }

    Y_UNIT_TEST(StatsSysViewFilter) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 2000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Bytes > UInt64("0")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Bytes > UInt64("0")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind == UInt32("6")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 0);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind >= UInt32("3")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3*3);
        }
    }

    Y_UNIT_TEST(StatsSysViewAggregation) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 3000);
        }

        EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != UInt32("4") -- not INACTIVE
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != UInt32("4") -- not INACTIVE
                GROUP BY
                    PathId
                ORDER BY
                    PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 5);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(Bytes) as bytes,
                    SUM(RawBytes) as bytes_raw,
                    SUM(Portions) as portions,
                    SUM(Blobs) as blobs
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind < UInt32("4")
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(Bytes) as bytes,
                    SUM(RawBytes) as bytes_raw,
                    SUM(Portions) as portions,
                    SUM(Blobs) as blobs
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Kind < UInt32("4")
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(Bytes) as bytes,
                    SUM(RawBytes) as bytes_raw,
                    SUM(Portions) as portions,
                    SUM(Blobs) as blobs
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId >= UInt64("4") AND Kind < UInt32("4")
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
        }

        {
            auto selectQuery = TString(R"(
                SELECT count(*)
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            // 3 Tables with 3 Shards each and 4 KindId-s of stats
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("column0")), 3*3*numKinds);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    count(distinct(PathId)),
                    count(distinct(Kind)),
                    count(distinct(TabletId))
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("column0")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("column1")), numKinds);
            UNIT_ASSERT_GE(GetUint64(rows[0].at("column2")), 3ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, count(*), sum(Rows), sum(Bytes), sum(RawBytes)
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId
                ORDER BY PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            for (ui64 pathId = 3, row = 0; pathId <= 5; ++pathId, ++row) {
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[row].at("PathId")), pathId);
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[row].at("column1")), 3*numKinds);
            }
        }
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

            if (pushEnabled) {
                builder << "PRAGMA Kikimr.OptEnablePredicateExtract=\"false\";" << Endl;
            } else {
                builder << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }

            builder << R"(
                DECLARE $in_timestamp AS Timestamp;
                DECLARE $in_uid AS Utf8;
                DECLARE $in_level AS Int32;

                SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                    `timestamp` > $in_timestamp AND uid > $in_uid AND level > $in_level
                ORDER BY `timestamp`;
            )" << Endl;

            return builder;
        };

        auto normalQuery = buildQuery(false);
        auto pushQuery = buildQuery(true);

        auto params = tableClient.GetParamsBuilder()
            .AddParam("$in_timestamp")
                .Timestamp(TInstant::MicroSeconds(3000990))
                .Build()
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

    void TestOlapUpsert(ui32 numShards) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        //EnableDebugLogging(kikimr);

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
        TestOlapUpsert(1);
    }

    Y_UNIT_TEST(OlapUpsert) {
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
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        EnableDebugLogging(&runtime);

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
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        EnableDebugLogging(&runtime);

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
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        EnableDebugLogging(&runtime);

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

        std::map<std::string, std::set<std::string>> exceptions = {
            {"Int8", {"Int16", "Int32"}},
            {"Int16", {"Int8", "Int32"}},
            {"Int32", {"Int8", "Int16"}},
            {"UInt8", {"UInt16", "UInt32"}},
            {"UInt16", {"UInt8", "UInt32"}},
            {"UInt32", {"UInt8", "UInt16"}},
            {"String", {"Utf8"}},
            {"Utf8", {"String", "Json", "Yson"}},
            {"Json", {"Utf8", "Yson"}},
            {"Yson", {"Utf8", "Json"}},
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

    Y_UNIT_TEST(NoErrorOnLegacyPragma) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "false";
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(BlocksRead) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                PRAGMA Kikimr.OptEnableOlapPushdown = "false";

                SELECT
                    id, resource_id
                FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Blocks_NoAggPushdown) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                SELECT
                    COUNT(DISTINCT id)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[10u]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(OlapRead_FailsOnDataQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
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
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
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
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        EnableDebugLogging(kikimr);
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
}

} // namespace NKqp
} // namespace NKikimr
