#include "cs_helper.h"
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/binary_json/write.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::Tests::NCS {

void THelperSchemaless::CreateTestOlapStore(TActorId sender, TString scheme) {
    NKikimrSchemeOp::TColumnStoreDescription store;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &store));

    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
    op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
    op->SetWorkingDir(ROOT_PATH);
    op->MutableCreateColumnStore()->CopyFrom(store);

    Server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = Server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    Cerr << ev->Get()->Record.DebugString() << Endl;
    auto status = ev->Get()->Record.GetStatus();
    ui64 txId = ev->Get()->Record.GetTxId();
    UNIT_ASSERT(status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
    WaitForSchemeOperation(sender, txId);
}

void THelperSchemaless::CreateTestOlapTable(TActorId sender, TString storeOrDirName, TString scheme) {
    NKikimrSchemeOp::TColumnTableDescription table;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &table));
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());

    TString workingDir = ROOT_PATH;
    if (!storeOrDirName.empty()) {
        workingDir += "/" + storeOrDirName;
    }

    auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
    op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
    op->SetWorkingDir(workingDir);
    op->MutableCreateColumnTable()->CopyFrom(table);

    Server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = Server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    ui64 txId = ev->Get()->Record.GetTxId();
    auto status = ev->Get()->Record.GetStatus();
    Cerr << ev->Get()->Record.DebugString() << Endl;
    UNIT_ASSERT(status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
    WaitForSchemeOperation(sender, txId);
}

void THelperSchemaless::SendDataViaActorSystem(TString testTable, std::shared_ptr<arrow::RecordBatch> batch, const Ydb::StatusIds_StatusCode& expectedStatus) const {
    auto* runtime = Server.GetRuntime();

    UNIT_ASSERT(batch);
    UNIT_ASSERT(batch->num_rows());
    auto data = NArrow::SerializeBatchNoCompression(batch);
    UNIT_ASSERT(!data.empty());
    TString serializedSchema = NArrow::SerializeSchema(*batch->schema());
    UNIT_ASSERT(serializedSchema);

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table(testTable);

    std::atomic<size_t> responses = 0;
    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest,
        Ydb::Table::BulkUpsertResponse>;
    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", runtime->GetActorSystem(0));
    future.Subscribe([&](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) mutable {
        responses.fetch_add(1);
        auto op = f.GetValueSync().operation();
        if (op.status() != Ydb::StatusIds::SUCCESS) {
            for (auto& issue : op.issues()) {
                Cerr << issue.message() << " ";
            }
            Cerr << "\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(op.status(), expectedStatus);
        });

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return responses.load() >= 1;
    };

    runtime->DispatchEvents(options);
}

void THelperSchemaless::SendDataViaActorSystem(TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui32 tsStepUs) const {
    auto batch = TestArrowBatch(pathIdBegin, tsBegin, rowCount, tsStepUs);
    SendDataViaActorSystem(testTable, batch);
}

//

std::shared_ptr<arrow::Schema> THelper::GetArrowSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.emplace_back(arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false));
    fields.emplace_back(arrow::field("resource_id", arrow::utf8()));
    fields.emplace_back(arrow::field("uid", arrow::utf8(), false));
    fields.emplace_back(arrow::field("level", arrow::int32()));
    fields.emplace_back(arrow::field("message", arrow::utf8()));
    if (GetWithJsonDocument()) {
        fields.emplace_back(arrow::field("json_payload", arrow::utf8()));
    }
    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::RecordBatch> THelper::TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui32 tsStepUs) const {
    std::shared_ptr<arrow::Schema> schema = GetArrowSchema();

    arrow::TimestampBuilder b1(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder b2;
    arrow::StringBuilder b3;
    arrow::Int32Builder b4;
    arrow::StringBuilder b5;
    arrow::StringBuilder b6;

    NJson::TJsonValue jsonInfo;
    jsonInfo["a"]["b"] = 1;
    jsonInfo["a"]["c"] = "asds";
    jsonInfo["b"] = "asd";

    size_t index = 1ULL;
    const auto magic = WithSomeNulls_ ? 3ULL : 0ULL;
    for (size_t i = 0; i < rowCount; ++i) {
        std::string uid("uid_" + std::to_string(tsBegin + i));
        std::string message("some prefix " + std::string(1024 + i % 200, 'x'));
        Y_ABORT_UNLESS(b1.Append(tsBegin + i * tsStepUs).ok());
        Y_ABORT_UNLESS(b2.Append(std::to_string(pathIdBegin + i)).ok());
        Y_ABORT_UNLESS(b3.Append(uid).ok());

        if (magic && !(++index % magic))
            Y_ABORT_UNLESS(b4.AppendNull().ok());
        else
            Y_ABORT_UNLESS(b4.Append(i % 5).ok());

        if (magic && !(++index % magic))
            Y_ABORT_UNLESS(b5.AppendNull().ok());
        else
            Y_ABORT_UNLESS(b5.Append(message).ok());

        jsonInfo["a"]["b"] = i;
        auto jsonStringBase = jsonInfo.GetStringRobust();
        Y_ABORT_UNLESS(b6.Append(jsonStringBase.data(), jsonStringBase.size()).ok());
    }

    std::shared_ptr<arrow::TimestampArray> a1;
    std::shared_ptr<arrow::StringArray> a2;
    std::shared_ptr<arrow::StringArray> a3;
    std::shared_ptr<arrow::Int32Array> a4;
    std::shared_ptr<arrow::StringArray> a5;
    std::shared_ptr<arrow::StringArray> a6;

    Y_ABORT_UNLESS(b1.Finish(&a1).ok());
    Y_ABORT_UNLESS(b2.Finish(&a2).ok());
    Y_ABORT_UNLESS(b3.Finish(&a3).ok());
    Y_ABORT_UNLESS(b4.Finish(&a4).ok());
    Y_ABORT_UNLESS(b5.Finish(&a5).ok());
    Y_ABORT_UNLESS(b6.Finish(&a6).ok());

    if (GetWithJsonDocument()) {
        return arrow::RecordBatch::Make(schema, rowCount, { a1, a2, a3, a4, a5, a6 });
    } else {
        return arrow::RecordBatch::Make(schema, rowCount, { a1, a2, a3, a4, a5 });
    }

}

TString THelper::GetTestTableSchema() const {
    TStringBuilder sb;
    sb << R"(Columns{ Name: "timestamp" Type : "Timestamp" NotNull : true })";
    sb << R"(Columns{ Name: "resource_id" Type : "Utf8" DataAccessorConstructor{ ClassName: "SPARSED" } })";
    sb << "Columns{ Name: \"uid\" Type : \"Utf8\" NotNull : true StorageId : \"" + OptionalStorageId + "\" }";
    sb << R"(Columns{ Name: "level" Type : "Int32" })";
    sb << "Columns{ Name: \"message\" Type : \"Utf8\" StorageId : \"" + OptionalStorageId + "\" }";
    if (GetWithJsonDocument()) {
        sb << R"(Columns{ Name: "json_payload" Type : "JsonDocument" })";
    }
    sb << R"(
        KeyColumnNames: "timestamp"
        KeyColumnNames: "uid"
        Engine : COLUMN_ENGINE_REPLACING_TIMESERIES
    )";
    return sb;
}

void THelper::CreateOlapTableWithStore(TString tableName /*= "olapTable"*/, TString storeName /*= "olapStore"*/, ui32 storeShardsCount /*= 4*/, ui32 tableShardsCount /*= 3*/) {
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

    const TString shardingColumns = "[\"" + JoinSeq("\",\"", GetShardingColumns()) + "\"]";

    TBase::CreateTestOlapTable(sender, storeName, Sprintf(R"(
        Name: "%s"
        ColumnShardCount: %d
        Sharding {
            HashSharding {
                Function: %s
                Columns: %s
            }
        })", tableName.c_str(), tableShardsCount, ShardingMethod.data(), shardingColumns.c_str()));
}

// Clickbench table

std::shared_ptr<arrow::Schema> TCickBenchHelper::GetArrowSchema() const {
    return std::make_shared<arrow::Schema>(
        std::vector<std::shared_ptr<arrow::Field>> {
            arrow::field("WatchID", arrow::int64(), false),
            arrow::field("JavaEnable", arrow::int16(), false),
            arrow::field("Title", arrow::utf8(), false),
            arrow::field("GoodEvent", arrow::int16(), false),
            arrow::field("EventTime", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
            arrow::field("EventDate", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false), // TODO: Date
            arrow::field("CounterID", arrow::int32(), false),
            arrow::field("ClientIP", arrow::int32(), false),
            arrow::field("RegionID", arrow::int32(), false),
            arrow::field("UserID", arrow::int64(), false),
            arrow::field("CounterClass", arrow::int16(), false),
            arrow::field("OS", arrow::int16(), false),
            arrow::field("UserAgent", arrow::int16(), false),
            arrow::field("URL", arrow::utf8(), false),
            arrow::field("Referer", arrow::utf8(), false),
            arrow::field("IsRefresh", arrow::int16(), false),
            arrow::field("RefererCategoryID", arrow::int16(), false),
            arrow::field("RefererRegionID", arrow::int32(), false),
            arrow::field("URLCategoryID", arrow::int16(), false),
            arrow::field("URLRegionID", arrow::int32(), false),
            arrow::field("ResolutionWidth", arrow::int16(), false),
            arrow::field("ResolutionHeight", arrow::int16(), false),
            arrow::field("ResolutionDepth", arrow::int16(), false),
            arrow::field("FlashMajor", arrow::int16(), false),
            arrow::field("FlashMinor", arrow::int16(), false),
            arrow::field("FlashMinor2", arrow::utf8(), false),
            arrow::field("NetMajor", arrow::int16(), false),
            arrow::field("NetMinor", arrow::int16(), false),
            arrow::field("UserAgentMajor", arrow::int16(), false),
            arrow::field("UserAgentMinor", arrow::binary(), false),
            arrow::field("CookieEnable", arrow::int16(), false),
            arrow::field("JavascriptEnable", arrow::int16(), false),
            arrow::field("IsMobile", arrow::int16(), false),
            arrow::field("MobilePhone", arrow::int16(), false),
            arrow::field("MobilePhoneModel", arrow::utf8(), false),
            arrow::field("Params", arrow::utf8(), false),
            arrow::field("IPNetworkID", arrow::int32(), false),
            arrow::field("TraficSourceID", arrow::int16(), false),
            arrow::field("SearchEngineID", arrow::int16(), false),
            arrow::field("SearchPhrase", arrow::utf8(), false),
            arrow::field("AdvEngineID", arrow::int16(), false),
            arrow::field("IsArtifical", arrow::int16(), false),
            arrow::field("WindowClientWidth", arrow::int16(), false),
            arrow::field("WindowClientHeight", arrow::int16(), false),
            arrow::field("ClientTimeZone", arrow::int16(), false),
            arrow::field("ClientEventTime", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
            arrow::field("SilverlightVersion1", arrow::int16(), false),
            arrow::field("SilverlightVersion2", arrow::int16(), false),
            arrow::field("SilverlightVersion3", arrow::int32(), false),
            arrow::field("SilverlightVersion4", arrow::int16(), false),
            arrow::field("PageCharset", arrow::utf8(), false),
            arrow::field("CodeVersion", arrow::int32(), false),
            arrow::field("IsLink", arrow::int16(), false),
            arrow::field("IsDownload", arrow::int16(), false),
            arrow::field("IsNotBounce", arrow::int16(), false),
            arrow::field("FUniqID", arrow::int64(), false),
            arrow::field("OriginalURL", arrow::utf8(), false),
            arrow::field("HID", arrow::int32(), false),
            arrow::field("IsOldCounter", arrow::int16(), false),
            arrow::field("IsEvent", arrow::int16(), false),
            arrow::field("IsParameter", arrow::int16(), false),
            arrow::field("DontCountHits", arrow::int16(), false),
            arrow::field("WithHash", arrow::int16(), false),
            arrow::field("HitColor", arrow::binary(), false),
            arrow::field("LocalEventTime", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), false),
            arrow::field("Age", arrow::int16(), false),
            arrow::field("Sex", arrow::int16(), false),
            arrow::field("Income", arrow::int16(), false),
            arrow::field("Interests", arrow::int16(), false),
            arrow::field("Robotness", arrow::int16(), false),
            arrow::field("RemoteIP", arrow::int32(), false),
            arrow::field("WindowName", arrow::int32(), false),
            arrow::field("OpenerName", arrow::int32(), false),
            arrow::field("HistoryLength", arrow::int16(), false),
            arrow::field("BrowserLanguage", arrow::utf8(), false),
            arrow::field("BrowserCountry", arrow::utf8(), false),
            arrow::field("SocialNetwork", arrow::utf8(), false),
            arrow::field("SocialAction", arrow::utf8(), false),
            arrow::field("HTTPError", arrow::int16(), false),
            arrow::field("SendTiming", arrow::int32(), false),
            arrow::field("DNSTiming", arrow::int32(), false),
            arrow::field("ConnectTiming", arrow::int32(), false),
            arrow::field("ResponseStartTiming", arrow::int32(), false),
            arrow::field("ResponseEndTiming", arrow::int32(), false),
            arrow::field("FetchTiming", arrow::int32(), false),
            arrow::field("SocialSourceNetworkID", arrow::int16(), false),
            arrow::field("SocialSourcePage", arrow::utf8(), false),
            arrow::field("ParamPrice", arrow::int64(), false),
            arrow::field("ParamOrderID", arrow::utf8(), false),
            arrow::field("ParamCurrency", arrow::utf8(), false),
            arrow::field("ParamCurrencyID", arrow::int16(), false),
            arrow::field("OpenstatServiceName", arrow::utf8(), false),
            arrow::field("OpenstatCampaignID", arrow::utf8(), false),
            arrow::field("OpenstatAdID", arrow::utf8(), false),
            arrow::field("OpenstatSourceID", arrow::utf8(), false),
            arrow::field("UTMSource", arrow::utf8(), false),
            arrow::field("UTMMedium", arrow::utf8(), false),
            arrow::field("UTMCampaign", arrow::utf8(), false),
            arrow::field("UTMContent", arrow::utf8(), false),
            arrow::field("UTMTerm", arrow::utf8(), false),
            arrow::field("FromTag", arrow::utf8(), false),
            arrow::field("HasGCLID", arrow::int16(), false),
            arrow::field("RefererHash", arrow::int64(), false),
            arrow::field("URLHash", arrow::int64(), false),
            arrow::field("CLID", arrow::int32(), false)
    });
}

std::shared_ptr<arrow::RecordBatch> TCickBenchHelper::TestArrowBatch(ui64, ui64 begin, size_t rowCount, const ui32 tsStepUs) const {
    std::shared_ptr<arrow::Schema> schema = GetArrowSchema();
    UNIT_ASSERT(schema);
    UNIT_ASSERT(schema->num_fields());

    std::unique_ptr<arrow::RecordBatchBuilder> builders;
    auto res = arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), rowCount, &builders);
    UNIT_ASSERT(res.ok());

    for (i32 col = 0; col < schema->num_fields(); ++col) {
        auto& field = schema->field(col);
        auto typeId = field->type()->id();
        for (size_t row = 0; row < rowCount; ++row) {
            ui64 value = begin + row;
            switch (typeId) {
                case arrow::Type::INT16: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow::Int16Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow::Type::INT32: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow::Int32Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow::Type::INT64: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow::Int64Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow::Type::TIMESTAMP: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow::TimestampBuilder>(col)->Append(begin + row * tsStepUs).ok());
                    break;
                }
                case arrow::Type::BINARY: {
                    auto str = ToString(value);
                    UNIT_ASSERT(builders->GetFieldAs<arrow::BinaryBuilder>(col)->Append(str.data(), str.size()).ok());
                    break;
                }
                case arrow::Type::STRING: {
                    auto str = ToString(value);
                    UNIT_ASSERT(builders->GetFieldAs<arrow::StringBuilder>(col)->Append(str.data(), str.size()).ok());
                    break;
                }
                default:
                    Y_ABORT("unexpected type");
            }
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    UNIT_ASSERT(builders->Flush(&batch).ok());
    UNIT_ASSERT(batch);
    UNIT_ASSERT(batch->num_rows());
    UNIT_ASSERT(batch->Validate().ok());
    return batch;
}

// Table with NULLs

std::shared_ptr<arrow::Schema> TTableWithNullsHelper::GetArrowSchema() const {
    return std::make_shared<arrow::Schema>(
        std::vector<std::shared_ptr<arrow::Field>>{
            arrow::field("id", arrow::int32(), false),
            arrow::field("resource_id", arrow::utf8()),
            arrow::field("level", arrow::int32()),
            arrow::field("binary_str", arrow::binary()),
            arrow::field("jsonval", arrow::utf8()),
            arrow::field("jsondoc", arrow::binary())
    });
}

std::shared_ptr<arrow::RecordBatch> TTableWithNullsHelper::TestArrowBatch() const {
    return TestArrowBatch(0, 0, 10, 1);
}

std::shared_ptr<arrow::RecordBatch> TTableWithNullsHelper::TestArrowBatch(ui64, ui64, size_t rowCount, const ui32 /*tsStepUs*/) const {
    rowCount = 10;
    std::shared_ptr<arrow::Schema> schema = GetArrowSchema();

    arrow::Int32Builder bId;
    arrow::StringBuilder bResourceId;
    arrow::Int32Builder bLevel;
    arrow::StringBuilder bBinaryStr;
    arrow::StringBuilder bJsonVal;
    arrow::BinaryBuilder bJsonDoc;

    for (size_t i = 1; i <= rowCount / 2; ++i) {
        Y_ABORT_UNLESS(bId.Append(i).ok());
        Y_ABORT_UNLESS(bResourceId.AppendNull().ok());
        Y_ABORT_UNLESS(bLevel.Append(i).ok());
        Y_ABORT_UNLESS(bBinaryStr.AppendNull().ok());
        Y_ABORT_UNLESS(bJsonVal.Append(std::string(R"({"col1": "val1", "col-abc": "val-abc", "obj": {"obj_col2_int": 16}})")).ok());
        Y_ABORT_UNLESS(bJsonDoc.AppendNull().ok());
    }

    const auto maybeJsonDoc = std::string(R"({"col1": "val1", "col-abc": "val-abc", "obj": {"obj_col2_int": 16}})");
    for (size_t i = rowCount / 2 + 1; i <= rowCount; ++i) {
        Y_ABORT_UNLESS(bId.Append(i).ok());
        Y_ABORT_UNLESS(bResourceId.Append(std::to_string(i)).ok());
        Y_ABORT_UNLESS(bLevel.AppendNull().ok());
        Y_ABORT_UNLESS(bBinaryStr.Append(std::to_string(i)).ok());
        Y_ABORT_UNLESS(bJsonVal.AppendNull().ok());
        Y_ABORT_UNLESS(bJsonDoc.Append(maybeJsonDoc.data(), maybeJsonDoc.length()).ok());
    }

    std::shared_ptr<arrow::Int32Array> aId;
    std::shared_ptr<arrow::StringArray> aResourceId;
    std::shared_ptr<arrow::Int32Array> aLevel;
    std::shared_ptr<arrow::StringArray> aBinaryStr;
    std::shared_ptr<arrow::StringArray> aJsonVal;
    std::shared_ptr<arrow::BinaryArray> aJsonDoc;

    Y_ABORT_UNLESS(bId.Finish(&aId).ok());
    Y_ABORT_UNLESS(bResourceId.Finish(&aResourceId).ok());
    Y_ABORT_UNLESS(bLevel.Finish(&aLevel).ok());
    Y_ABORT_UNLESS(bBinaryStr.Finish(&aBinaryStr).ok());
    Y_ABORT_UNLESS(bJsonVal.Finish(&aJsonVal).ok());
    Y_ABORT_UNLESS(bJsonDoc.Finish(&aJsonDoc).ok());

    return arrow::RecordBatch::Make(schema, rowCount, { aId, aResourceId, aLevel, aBinaryStr, aJsonVal, aJsonDoc });
}

}
