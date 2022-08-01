#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_long_tx.h>

#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

#include <ydb/core/kqp/executer/kqp_executer.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/datashard_ut_common.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tablet_helpers.h>
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
        //runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_TRACE);
        //runtime->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::TX_OLAPSHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        //runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
    }

    void EnableDebugLogging(TKikimrRunner& kikimr) {
        EnableDebugLogging(kikimr.GetTestServer().GetRuntime());
    }

    void WaitForSchemeOperation(Tests::TServer& server, TActorId sender, ui64 txId) {
        auto &runtime = *server.GetRuntime();
        auto &settings = server.GetSettings();
        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        auto tid = Tests::ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
        runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
    }

    void CreateTestOlapStore(Tests::TServer& server, TActorId sender, TString scheme) {
        NKikimrSchemeOp::TColumnStoreDescription store;
        UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &store));

        auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());
        auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
        op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
        op->SetWorkingDir("/Root");
        op->MutableCreateColumnStore()->CopyFrom(store);

        server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
        auto ev = server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        ui64 txId = ev->Get()->Record.GetTxId();
        WaitForSchemeOperation(server, sender, txId);
    }

    void CreateTestOlapTable(Tests::TServer& server, TActorId sender, TString storeName, TString scheme) {
        NKikimrSchemeOp::TColumnTableDescription table;
        UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &table));
        auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());
        auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
        op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
        op->SetWorkingDir("/Root/" + storeName);
        op->MutableCreateColumnTable()->CopyFrom(table);

        server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
        auto ev = server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        ui64 txId = ev->Get()->Record.GetTxId();
        WaitForSchemeOperation(server, sender, txId);
    }

    void CreateTestOlapTable(Tests::TServer& server, TString tableName = "olapTable", TString storeName = "olapStore",
                             ui32 storeShardsCount = 4, ui32 tableShardsCount = 3,
                             TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {
        TActorId sender = server.GetRuntime()->AllocateEdgeActor();
        CreateTestOlapStore(server, sender, Sprintf(R"(
             Name: "%s"
             ColumnShardCount: %d
             SchemaPresets {
                 Name: "default"
                 Schema {
                     Columns { Name: "timestamp" Type: "Timestamp" }
                     #Columns { Name: "resource_type" Type: "Utf8" }
                     Columns { Name: "resource_id" Type: "Utf8" }
                     Columns { Name: "uid" Type: "Utf8" }
                     Columns { Name: "level" Type: "Int32" }
                     Columns { Name: "message" Type: "Utf8" }
                     #Columns { Name: "json_payload" Type: "Json" }
                     #Columns { Name: "ingested_at" Type: "Timestamp" }
                     #Columns { Name: "saved_at" Type: "Timestamp" }
                     #Columns { Name: "request_id" Type: "Utf8" }
                     KeyColumnNames: "timestamp"
                     Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                 }
             }
        )", storeName.c_str(), storeShardsCount));

        TString shardingColumns = "[\"timestamp\", \"uid\"]";
        if (shardingFunction != "HASH_FUNCTION_CLOUD_LOGS") {
            shardingColumns = "[\"uid\"]";
        }

        CreateTestOlapTable(server, sender, storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            })", tableName.c_str(), tableShardsCount, shardingFunction.c_str(), shardingColumns.c_str()));
    }


    void CreateTestOlapTable(TKikimrRunner& kikimr, TString tableName = "olapTable", TString storeName = "olapStore",
                             ui32 storeShardsCount = 4, ui32 tableShardsCount = 3,
                             TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {

        CreateTestOlapTable(kikimr.GetTestServer(), tableName, storeName, storeShardsCount, tableShardsCount,
                            shardingFunction);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() {
        return std::make_shared<arrow::Schema>(
            std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO)),
                arrow::field("resource_id", arrow::utf8()),
                arrow::field("uid", arrow::utf8()),
                arrow::field("level", arrow::int32()),
                arrow::field("message", arrow::utf8())
            });
    }

    std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        std::shared_ptr<arrow::Schema> schema = GetArrowSchema();

        arrow::TimestampBuilder b1(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
        arrow::StringBuilder b2;
        arrow::StringBuilder b3;
        arrow::Int32Builder b4;
        arrow::StringBuilder b5;

        for (size_t i = 0; i < rowCount; ++i) {
            std::string uid("uid_" + std::to_string(tsBegin + i));
            std::string message("some prefix " + std::string(1024 + i % 200, 'x'));
            Y_VERIFY(b1.Append(tsBegin + i).ok());
            Y_VERIFY(b2.Append(std::to_string(pathIdBegin + i)).ok());
            Y_VERIFY(b3.Append(uid).ok());
            Y_VERIFY(b4.Append(i % 5).ok());
            Y_VERIFY(b5.Append(message).ok());
        }

        std::shared_ptr<arrow::TimestampArray> a1;
        std::shared_ptr<arrow::StringArray> a2;
        std::shared_ptr<arrow::StringArray> a3;
        std::shared_ptr<arrow::Int32Array> a4;
        std::shared_ptr<arrow::StringArray> a5;

        Y_VERIFY(b1.Finish(&a1).ok());
        Y_VERIFY(b2.Finish(&a2).ok());
        Y_VERIFY(b3.Finish(&a3).ok());
        Y_VERIFY(b4.Finish(&a4).ok());
        Y_VERIFY(b5.Finish(&a5).ok());

        return arrow::RecordBatch::Make(schema, rowCount, { a1, a2, a3, a4, a5 });
    }

    TString TestBlob(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        auto batch = TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        int64_t size;
        auto status = arrow::ipc::GetRecordBatchSize(*batch, &size);
        Y_VERIFY(status.ok());

        TString buf;
        buf.resize(size);
        auto writer = arrow::Buffer::GetWriter(arrow::MutableBuffer::Wrap(&buf[0], size));
        Y_VERIFY(writer.ok());

        // UNCOMPRESSED
        status = SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults(), (*writer).get());
        Y_VERIFY(status.ok());
        return buf;
    }

    void WriteTestData(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        NYdb::NLongTx::TClient client(kikimr.GetDriver());

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resBeginTx.Status().GetStatus(), EStatus::SUCCESS, resBeginTx.Status().GetIssues().ToString());

        auto txId = resBeginTx.GetResult().tx_id();
        TString data = TestBlob(pathIdBegin, tsBegin, rowCount);

        NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, testTable, txId, data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resWrite.Status().GetStatus(), EStatus::SUCCESS, resWrite.Status().GetIssues().ToString());

        NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resCommitTx.Status().GetStatus(), EStatus::SUCCESS, resCommitTx.Status().GetIssues().ToString());
    }

    void SendDataViaActorSystem(NActors::TTestActorRuntime* runtime, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        std::shared_ptr<arrow::Schema> schema = GetArrowSchema();
        TString serializedSchema = NArrow::SerializeSchema(*schema);
        Y_VERIFY(serializedSchema);

        auto batch = TestBlob(pathIdBegin, tsBegin, rowCount);
        Y_VERIFY(batch);

        Ydb::Table::BulkUpsertRequest request;
        request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
        request.set_data(batch);
        request.set_table(testTable);

        size_t responses = 0;
        auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", runtime->GetActorSystem(0));
        future.Subscribe([&](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) mutable {
            ++responses;
            UNIT_ASSERT_VALUES_EQUAL(f.GetValueSync().operation().status(), Ydb::StatusIds::SUCCESS);
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return responses >= 1;
        };

        runtime->DispatchEvents(options);
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
                                             Columns { Name: "key" Type: "Int32" }
                                             Columns { Name: "Bool_column" Type: "Bool" }
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
                                             Columns { Name: "Decimal_column" Type: "Decimal" }
                                             Columns { Name: "String_column" Type: "String" }
                                             Columns { Name: "Utf8_column" Type: "Utf8" }
                                             Columns { Name: "Json_column" Type: "Json" }
                                             Columns { Name: "Yson_column" Type: "Yson" }
                                             Columns { Name: "Timestamp_column" Type: "Timestamp" }
                                             Columns { Name: "Date_column" Type: "Date" }
                                             Columns { Name: "Datetime_column" Type: "Datetime" }
                                             Columns { Name: "Interval_column" Type: "Interval" }
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
            {
                "Bool",
                tableClient.GetParamsBuilder().AddParam("$in_value").Bool(false).Build().Build()
            },
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
            {
                "Interval",
                tableClient.GetParamsBuilder().AddParam("$in_value").Interval(1010).Build().Build()
            },
            {
                "Decimal(12,9)",
                tableClient.GetParamsBuilder().AddParam("$in_value").Decimal(TDecimalValue("10.123456789", 12, 9)).Build().Build()
            },
#if 0
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

    void CheckPlanForAggregatePushdown(const TString& query, NYdb::NTable::TTableClient& tableClient) {
        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        auto res = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto planRes = CollectStreamResult(res);
        auto ast = planRes.QueryStats->Getquery_ast();

        UNIT_ASSERT_C(ast.find("TKqpOlapAgg") != std::string::npos,
            TStringBuilder() << "Aggregate was not pushed down. Query: " << query);
    }

    Y_UNIT_TEST_TWIN(SimpleQueryOlap, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);

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
            CompareYson(result, R"([[["0"];[1000000u]];[["1"];[1000001u]]])");
        }
    }

    Y_UNIT_TEST_TWIN(SimpleLookupOlap, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);

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

            CompareYson(result, R"([[["0"];[1000000u]]])");
        }
    }

    Y_UNIT_TEST_TWIN(SimpleRangeOlap, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);

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

            CompareYson(result, R"([[["0"];[1000000u]];[["1"];[1000001u]]])");
        }
    }

    Y_UNIT_TEST_TWIN(CompositeRangeOlap, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);

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

            CompareYson(result, R"([[["0"];[1000000u]]])");
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

            CompareYson(result, R"([[["0"];[1000000u]];[["1"];[1000001u]]])");
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

            CompareYson(result, R"([[["1"];[1000001u]]])");
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

            CompareYson(result, R"([[["0"];[1000000u]]])");
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

            CompareYson(result, R"([[["1"];[1000001u]]])");
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

            CompareYson(result, R"([[["1"];[1000001u]]])");
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

            CompareYson(result, R"([[["0"];[1000000u]]])");
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

    Y_UNIT_TEST_TWIN(QueryOltpAndOlap, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        auto client = kikimr.GetTableClient();

        CreateTestOlapTable(kikimr);

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
            CompareYson(result, R"([[[1u];["Value-001"];["1"];["1"];[1000001u]];[[2u];["Value-002"];["2"];["2"];[1000002u]]])");
        }
    }

    Y_UNIT_TEST_TWIN(EmptyRange, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        CreateTestOlapTable(kikimr);
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

    Y_UNIT_TEST_TWIN(Aggregation, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);

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

    Y_UNIT_TEST_TWIN(PushdownFilter, UseSessionActor) {
        static bool enableLog = false;

        auto doTest = [](std::optional<bool> viaSettings, std::optional<bool> viaPragma, bool pushdownPresent) {
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false)
                .SetEnableKqpSessionActor(UseSessionActor);

            if (enableLog) {
                Cerr << "Run test:" << Endl;
                Cerr << "viaSettings is " << (viaSettings.has_value() ? "" : "not ") << "present.";
                if (viaSettings.has_value()) {
                    Cerr << " Value: " << viaSettings.value();
                }
                Cerr << Endl;
                Cerr << "viaPragma is " << (viaPragma.has_value() ? "" : "not ") << "present.";
                if (viaPragma.has_value()) {
                    Cerr << " Value: " << viaPragma.value();
                }
                Cerr << Endl;
                Cerr << "Expected result: " << pushdownPresent << Endl;
            }

            if (viaSettings.has_value()) {
                auto setting = NKikimrKqp::TKqpSetting();
                setting.SetName("_KqpPushOlapProcess");
                setting.SetValue(viaSettings.value() ? "true" : "false");
                settings.KqpSettings = { setting };
            }

            TKikimrRunner kikimr(settings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

            auto client = kikimr.GetTableClient();

            CreateTestOlapTable(kikimr);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 10);

            TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);

            {
                TString query = TString(R"(
                    --!syntax_v1
                    SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id = "5"u;
                )");

                if (viaPragma.has_value()) {
                    TString pragma = TString(R"(
                        PRAGMA Kikimr.KqpPushOlapProcess = "<ENABLE_PUSH>";
                    )");
                    SubstGlobal(pragma, "<ENABLE_PUSH>", viaPragma.value() ? "true" : "false");
                    query = pragma + query;
                }

                auto it = client.StreamExecuteScanQuery(query).GetValueSync();

                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);

                CompareYson(result, R"([[
                    [0];
                    ["some prefix xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"];
                    ["5"];
                    [1000005u];
                    ["uid_1000005"]
                    ]])");

                it = client.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
                auto explainResult = CollectStreamResult(it);
                NJson::TJsonValue plan, pushdown;
                NJson::ReadJsonTree(*explainResult.PlanJson, &plan, true);

                if (pushdownPresent) {
                    pushdown = FindPlanNodeByKv(plan, "PredicatePushdown", "true");
                } else {
                    pushdown = FindPlanNodeByKv(plan, "PredicatePushdown", "false");
                }

                UNIT_ASSERT(pushdown.IsDefined());
            }
        };

        TVector<std::tuple<std::optional<bool>, std::optional<bool>, bool>> testData = {
            {std::nullopt, std::nullopt, false},
            {false, std::nullopt, false},
            {true, std::nullopt, true},
            {std::nullopt, false, false},
            {std::nullopt, true, true},
            {false, false, false},
            {true, false, false},
            {false, true, true},
        };

        for (auto &data: testData) {
            doTest(std::get<0>(data), std::get<1>(data), std::get<2>(data));
        }
    }

    Y_UNIT_TEST_TWIN(PKDescScan, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        // EnableDebugLogging(kikimr);

        CreateTestOlapTable(kikimr);
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
            [[1000127u]];
            [[1000126u]];
            [[1000125u]];
            [[1000124u]]
        ])");

        CompareYson(expectedYson, ysonResult);
    }

    Y_UNIT_TEST_TWIN(ExtractRanges, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        CreateTestOlapTable(kikimr);
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
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_GE_C(ts, tsPrev, "result is not sorted in ASC order");
            tsPrev = ts;
        }
    }

    Y_UNIT_TEST_TWIN(ExtractRangesReverse, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        CreateTestOlapTable(kikimr);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp` DESC;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        TInstant tsPrev = TInstant::MicroSeconds(2000000);
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_LE_C(ts, tsPrev, "result is not sorted in DESC order");
            tsPrev = ts;
        }
    }

    Y_UNIT_TEST_TWIN(PredicatePushdown, UseSessionActor) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTestOlapTable(kikimr);
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
        };

        std::vector<TString> testDataNoPush = {
            R"(`level` != NULL)",
            R"(`level` > NULL)",
            R"(`timestamp` >= CAST(3000001 AS Timestamp))",
            R"(`level` >= CAST("2" As Uint32))",
        };

        auto buildQuery = [](const TString& predicate, bool pushEnabled) {
            TStringBuilder qBuilder;

            qBuilder << "--!syntax_v1" << Endl;

            if (pushEnabled) {
                qBuilder << R"(PRAGMA Kikimr.KqpPushOlapProcess = "true";)" << Endl;
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
    }

    Y_UNIT_TEST(AggregationPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        CreateTestOlapTable(kikimr);
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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
            CheckPlanForAggregatePushdown(query, tableClient);
        }
    }

    Y_UNIT_TEST(AggregationGroupByPushdown) {
        // remove this return when GROUP BY will be implemented on columnshard
        return;

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        CreateTestOlapTable(kikimr);
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
                SELECT
                    level, COUNT(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
            )";
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
            CheckPlanForAggregatePushdown(query, tableClient);
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdown, UseLlvm) {
        // remove this return when COUNT(*) will be implemented on columnshard
        return;

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        CreateTestOlapTable(kikimr);
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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
            CheckPlanForAggregatePushdown(query, tableClient);
        }
    }

    Y_UNIT_TEST(CountAllNoPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableOlapSchemaOperations(true);
        TKikimrRunner kikimr(settings);

        // EnableDebugLogging(kikimr);
        CreateTestOlapTable(kikimr);
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

    Y_UNIT_TEST_TWIN(StatsSysView, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        CreateTestOlapTable(kikimr);
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

    Y_UNIT_TEST_TWIN(StatsSysViewTable, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        CreateTestOlapTable(kikimr, "olapTable_1");
        CreateTestOlapTable(kikimr, "olapTable_2");
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

    Y_UNIT_TEST_TWIN(ManyColumnShards, UseSessionActor) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        server->GetRuntime()->GetAppData().FeatureFlags.SetEnableKqpScanQueryMultipleOlapShardsReads(true);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        EnableDebugLogging(runtime);

        ui32 numShards = NSan::PlainOrUnderSanitizer(1000, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(50, 10);
        CreateTestOlapTable(*server, "largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;
        for(ui64 i = 0; i < numIterations; ++i) {
            SendDataViaActorSystem(runtime, "/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
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
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST_TWIN(ManyColumnShardsWithRestarts, UseSessionActor) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        server->GetRuntime()->GetAppData().FeatureFlags.SetEnableKqpScanQueryMultipleOlapShardsReads(true);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        EnableDebugLogging(runtime);

        ui32 numShards = NSan::PlainOrUnderSanitizer(100, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(100, 10);
        CreateTestOlapTable(*server, "largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;

        for(ui64 i = 0; i < numIterations; ++i) {
            SendDataViaActorSystem(runtime, "/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
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

    Y_UNIT_TEST_TWIN(StatsSysViewColumns, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        CreateTestOlapTable(kikimr);
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

    Y_UNIT_TEST_TWIN(StatsSysViewRanges, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        CreateTestOlapTable(kikimr, "olapTable_1");
        CreateTestOlapTable(kikimr, "olapTable_2");
        CreateTestOlapTable(kikimr, "olapTable_3");

        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 3000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 5000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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

    Y_UNIT_TEST_TWIN(StatsSysViewFilter, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        CreateTestOlapTable(kikimr);
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 2000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind >= UInt32("3")
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3*3);
        }
    }

    Y_UNIT_TEST_TWIN(StatsSysViewAggregation, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 5;

        CreateTestOlapTable(kikimr, "olapTable_1");
        CreateTestOlapTable(kikimr, "olapTable_2");
        CreateTestOlapTable(kikimr, "olapTable_3");

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 3000);
        }

        // EnableDebugLogging(kikimr);

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
                PRAGMA Kikimr.KqpPushOlapProcess = "true";

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

    Y_UNIT_TEST_TWIN(PredicatePushdownWithParameters, UseSessionActor) {
        constexpr bool logQueries = true;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTestOlapTable(kikimr);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto buildQuery = [](bool pushEnabled) {
            TStringBuilder builder;

            builder << "--!syntax_v1" << Endl;

            if (pushEnabled) {
                builder << "PRAGMA Kikimr.KqpPushOlapProcess = \"true\";" << Endl;
                builder << "PRAGMA Kikimr.OptEnablePredicateExtract=\"false\";" << Endl;
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

    Y_UNIT_TEST_TWIN(PredicatePushdownParameterTypesValidation, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

        std::map<std::string, TParams> testData = CreateParametersOfAllTypes(tableClient);

        const TString queryTemplate = R"(
            --!syntax_v1
            PRAGMA Kikimr.KqpPushOlapProcess = "true";
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

    Y_UNIT_TEST_TWIN(PredicatePushdownCastErrors, UseSessionActor) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetEnableKqpSessionActor(UseSessionActor);
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
            "Bool",
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
            "Decimal(12,9)",
            "String",
            "Utf8",
            "Timestamp",
            "Date",
            "Datetime",
            "Interval"
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
            PRAGMA Kikimr.KqpPushOlapProcess = "true";

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
}

} // namespace NKqp
} // namespace NKikimr
