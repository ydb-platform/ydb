#include "cs_helper.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <yql/essentials/types/binary_json/write.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_fwd.h>

namespace NKikimr::Tests::NCS {

void THelperSchemaless::ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme) {
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    *request->Record.MutableTransaction()->MutableModifyScheme() = modifyScheme;
    TActorId sender = Server.GetRuntime()->AllocateEdgeActor();
    Server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = Server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    auto status = ev->Get()->Record.GetStatus();
    ui64 txId = ev->Get()->Record.GetTxId();
    UNIT_ASSERT(status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
    WaitForSchemeOperation(sender, txId);
}

void THelperSchemaless::CreateTestOlapStore(TString scheme) {
    NKikimrSchemeOp::TColumnStoreDescription store;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &store));
    NKikimrSchemeOp::TModifyScheme op;
    op.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
    op.SetWorkingDir(ROOT_PATH);
    op.MutableCreateColumnStore()->CopyFrom(store);
    ExecuteModifyScheme(op);
}

void THelperSchemaless::CreateTestOlapTable(TString storeOrDirName, TString scheme) {
    NKikimrSchemeOp::TColumnTableDescription table;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &table));
    TString workingDir = ROOT_PATH;
    if (!storeOrDirName.empty()) {
        workingDir += "/" + storeOrDirName;
    }

    NKikimrSchemeOp::TModifyScheme op;
    op.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
    op.SetWorkingDir(workingDir);
    op.MutableCreateColumnTable()->CopyFrom(table);
    ExecuteModifyScheme(op);
}

void THelperSchemaless::SendDataViaActorSystem(TString testTable, std::shared_ptr<arrow20::RecordBatch> batch,
    const Ydb::StatusIds_StatusCode& expectedStatus, const TString& expectedIssuePrefix) const {
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
    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", runtime->GetActorSystem(0));
    future.Subscribe([&](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) {
        auto op = f.GetValueSync().operation();
        TStringBuilder issues;
        if (op.status() != Ydb::StatusIds::SUCCESS) {
            for (auto& issue : op.issues()) {
                issues << issue.message() << " ";
            }
            issues << "\n";
        }
        Cerr << issues;
        UNIT_ASSERT_VALUES_EQUAL(op.status(), expectedStatus);
        UNIT_ASSERT(issues.StartsWith(expectedIssuePrefix));
        responses.fetch_add(1);
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


std::shared_ptr<arrow20::Schema> THelper::GetArrowSchema() const {
    std::vector<std::shared_ptr<arrow20::Field>> fields;
    fields.emplace_back(arrow20::field("timestamp", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), false));
    fields.emplace_back(arrow20::field("resource_id", arrow20::utf8()));
    fields.emplace_back(arrow20::field("uid", arrow20::utf8(), false));
    fields.emplace_back(arrow20::field("level", arrow20::int32()));
    fields.emplace_back(arrow20::field("message", arrow20::utf8()));
    if (GetWithJsonDocument()) {
        fields.emplace_back(arrow20::field("json_payload", arrow20::utf8()));
    }

    fields.emplace_back(arrow20::field("new_column1", arrow20::uint64()));
    return std::make_shared<arrow20::Schema>(std::move(fields));
}

std::shared_ptr<arrow20::RecordBatch> THelper::TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, const ui64 tsStepUs) const {
    std::shared_ptr<arrow20::Schema> schema = GetArrowSchema();

    arrow20::TimestampBuilder b1(arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), arrow20::default_memory_pool());
    arrow20::StringBuilder b2;
    arrow20::StringBuilder b3;
    arrow20::Int32Builder b4;
    arrow20::StringBuilder b5;
    arrow20::StringBuilder b6;
    arrow20::UInt64Builder b7;

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
        Y_ABORT_UNLESS(b7.Append(i * 1000).ok());
    }

    std::shared_ptr<arrow20::TimestampArray> a1;
    std::shared_ptr<arrow20::StringArray> a2;
    std::shared_ptr<arrow20::StringArray> a3;
    std::shared_ptr<arrow20::Int32Array> a4;
    std::shared_ptr<arrow20::StringArray> a5;
    std::shared_ptr<arrow20::StringArray> a6;
    std::shared_ptr<arrow20::UInt64Array> a7;

    Y_ABORT_UNLESS(b1.Finish(&a1).ok());
    Y_ABORT_UNLESS(b2.Finish(&a2).ok());
    Y_ABORT_UNLESS(b3.Finish(&a3).ok());
    Y_ABORT_UNLESS(b4.Finish(&a4).ok());
    Y_ABORT_UNLESS(b5.Finish(&a5).ok());
    Y_ABORT_UNLESS(b6.Finish(&a6).ok());
    Y_ABORT_UNLESS(b7.Finish(&a7).ok());

    if (GetWithJsonDocument()) {
        return arrow20::RecordBatch::Make(schema, rowCount, { a1, a2, a3, a4, a5, a6, a7 });
    } else {
        return arrow20::RecordBatch::Make(schema, rowCount, { a1, a2, a3, a4, a5, a7 });
    }

}

void THelper::SetForcedCompaction(const TString& storeName) {
    //In some tests we expect, that a compaction will start immidiately
    //For now, we use l-bucket optimizer for this purpose
    //In the future it should be replaced with lc-bucket or more sophisticated compaction optimizer planner
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    NKikimrSchemeOp::TModifyScheme modyfySchemeOp;
    modyfySchemeOp.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore);
    modyfySchemeOp.SetWorkingDir(ROOT_PATH);
    NKikimrSchemeOp::TAlterColumnStore* alterColumnStore = modyfySchemeOp.MutableAlterColumnStore();
    alterColumnStore->SetName(storeName);
    auto schemaPreset = alterColumnStore->AddAlterSchemaPresets();
    schemaPreset->SetName("default");
    auto schemaOptions = schemaPreset->MutableAlterSchema()->MutableOptions();
    schemaOptions->SetSchemeNeedActualization(false);
    auto plannerConstructot =schemaOptions->MutableCompactionPlannerConstructor();
    plannerConstructot->SetClassName("l-buckets");
    *plannerConstructot->MutableLBuckets() = NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer{};

    ExecuteModifyScheme(modyfySchemeOp);
}


TString THelper::GetTestTableSchema() const {
    TStringBuilder sb;
    sb << R"(Columns{ Name: "timestamp" Type : "Timestamp" NotNull : true })";
    sb << R"(Columns{ Name: "resource_id" Type : "Utf8" DataAccessorConstructor{ ClassName: "SPARSED" } })";
    sb << "Columns{ Name: \"uid\" Type : \"Utf8\" NotNull : true StorageId : \"" + OptionalStorageId + "\" }";
    sb << R"(Columns{ Name: "level" Type : "Int32" })";
    sb << "Columns{ Name: \"message\" Type : \"Utf8\" StorageId : \"" + OptionalStorageId + "\" }";
    sb << R"(Columns{ Name: "new_column1" Type : "Uint64" })";
    if (GetWithJsonDocument()) {
        sb << R"(Columns{ Name: "json_payload" Type : "JsonDocument" })";
    }
    sb << R"(
        KeyColumnNames: "timestamp"
        KeyColumnNames: "uid"
    )";
    return sb;
}

void THelper::CreateSchemaOlapTablesWithStore(const TString tableSchema, TVector<TString> tableNames /*= "olapTable"*/, TString storeName /*= "olapStore"*/, ui32 storeShardsCount /*= 4*/, ui32 tableShardsCount /*= 3*/) {
    CreateTestOlapStore(Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            SchemaPresets {
                Name: "default"
                Schema {
                    %s
                }
            }
        )", storeName.c_str(), storeShardsCount, tableSchema.data()));

    const TString shardingColumns = "[\"" + JoinSeq("\",\"", GetShardingColumns()) + "\"]";

    for (const TString& tableName : tableNames) {
        TBase::CreateTestOlapTable(storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            })", tableName.c_str(), tableShardsCount, ShardingMethod.data(), shardingColumns.c_str()));
    }
}

void THelper::CreateOlapTablesWithStore(TVector<TString> tableNames /*= {"olapTable"}*/, TString storeName /*= "olapStore"*/, ui32 storeShardsCount /*= 4*/, ui32 tableShardsCount /*= 3*/) {
    CreateSchemaOlapTablesWithStore(GetTestTableSchema(), tableNames, storeName, storeShardsCount, tableShardsCount);
}

void THelper::CreateSchemaOlapTables(const TString tableSchema, TVector<TString> tableNames, ui32 tableShardsCount) {
    const TString shardingColumns = "[\"" + JoinSeq("\",\"", GetShardingColumns()) + "\"]";

    for (const TString& tableName : tableNames) {
        TBase::CreateTestOlapTable("", Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            }
            Schema {
                %s
            }
        )", tableName.c_str(), tableShardsCount, ShardingMethod.data(), shardingColumns.c_str(), tableSchema.data()));
    }
}

void THelper::CreateOlapTables(TVector<TString> tableNames /*= {"olapTable"}*/, ui32 tableShardsCount /*= 3*/) {
    CreateSchemaOlapTables(GetTestTableSchema(), tableNames, tableShardsCount);
}

// Clickbench table

std::shared_ptr<arrow20::Schema> TCickBenchHelper::GetArrowSchema() const {
    return std::make_shared<arrow20::Schema>(
        std::vector<std::shared_ptr<arrow20::Field>> {
        arrow20::field("WatchID", arrow20::int64(), false),
            arrow20::field("JavaEnable", arrow20::int16(), false),
            arrow20::field("Title", arrow20::utf8(), false),
            arrow20::field("GoodEvent", arrow20::int16(), false),
            arrow20::field("EventTime", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), false),
            arrow20::field("EventDate", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), false), // TODO: Date
            arrow20::field("CounterID", arrow20::int32(), false),
            arrow20::field("ClientIP", arrow20::int32(), false),
            arrow20::field("RegionID", arrow20::int32(), false),
            arrow20::field("UserID", arrow20::int64(), false),
            arrow20::field("CounterClass", arrow20::int16(), false),
            arrow20::field("OS", arrow20::int16(), false),
            arrow20::field("UserAgent", arrow20::int16(), false),
            arrow20::field("URL", arrow20::utf8(), false),
            arrow20::field("Referer", arrow20::utf8(), false),
            arrow20::field("IsRefresh", arrow20::int16(), false),
            arrow20::field("RefererCategoryID", arrow20::int16(), false),
            arrow20::field("RefererRegionID", arrow20::int32(), false),
            arrow20::field("URLCategoryID", arrow20::int16(), false),
            arrow20::field("URLRegionID", arrow20::int32(), false),
            arrow20::field("ResolutionWidth", arrow20::int16(), false),
            arrow20::field("ResolutionHeight", arrow20::int16(), false),
            arrow20::field("ResolutionDepth", arrow20::int16(), false),
            arrow20::field("FlashMajor", arrow20::int16(), false),
            arrow20::field("FlashMinor", arrow20::int16(), false),
            arrow20::field("FlashMinor2", arrow20::utf8(), false),
            arrow20::field("NetMajor", arrow20::int16(), false),
            arrow20::field("NetMinor", arrow20::int16(), false),
            arrow20::field("UserAgentMajor", arrow20::int16(), false),
            arrow20::field("UserAgentMinor", arrow20::binary(), false),
            arrow20::field("CookieEnable", arrow20::int16(), false),
            arrow20::field("JavascriptEnable", arrow20::int16(), false),
            arrow20::field("IsMobile", arrow20::int16(), false),
            arrow20::field("MobilePhone", arrow20::int16(), false),
            arrow20::field("MobilePhoneModel", arrow20::utf8(), false),
            arrow20::field("Params", arrow20::utf8(), false),
            arrow20::field("IPNetworkID", arrow20::int32(), false),
            arrow20::field("TraficSourceID", arrow20::int16(), false),
            arrow20::field("SearchEngineID", arrow20::int16(), false),
            arrow20::field("SearchPhrase", arrow20::utf8(), false),
            arrow20::field("AdvEngineID", arrow20::int16(), false),
            arrow20::field("IsArtifical", arrow20::int16(), false),
            arrow20::field("WindowClientWidth", arrow20::int16(), false),
            arrow20::field("WindowClientHeight", arrow20::int16(), false),
            arrow20::field("ClientTimeZone", arrow20::int16(), false),
            arrow20::field("ClientEventTime", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), false),
            arrow20::field("SilverlightVersion1", arrow20::int16(), false),
            arrow20::field("SilverlightVersion2", arrow20::int16(), false),
            arrow20::field("SilverlightVersion3", arrow20::int32(), false),
            arrow20::field("SilverlightVersion4", arrow20::int16(), false),
            arrow20::field("PageCharset", arrow20::utf8(), false),
            arrow20::field("CodeVersion", arrow20::int32(), false),
            arrow20::field("IsLink", arrow20::int16(), false),
            arrow20::field("IsDownload", arrow20::int16(), false),
            arrow20::field("IsNotBounce", arrow20::int16(), false),
            arrow20::field("FUniqID", arrow20::int64(), false),
            arrow20::field("OriginalURL", arrow20::utf8(), false),
            arrow20::field("HID", arrow20::int32(), false),
            arrow20::field("IsOldCounter", arrow20::int16(), false),
            arrow20::field("IsEvent", arrow20::int16(), false),
            arrow20::field("IsParameter", arrow20::int16(), false),
            arrow20::field("DontCountHits", arrow20::int16(), false),
            arrow20::field("WithHash", arrow20::int16(), false),
            arrow20::field("HitColor", arrow20::binary(), false),
            arrow20::field("LocalEventTime", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), false),
            arrow20::field("Age", arrow20::int16(), false),
            arrow20::field("Sex", arrow20::int16(), false),
            arrow20::field("Income", arrow20::int16(), false),
            arrow20::field("Interests", arrow20::int16(), false),
            arrow20::field("Robotness", arrow20::int16(), false),
            arrow20::field("RemoteIP", arrow20::int32(), false),
            arrow20::field("WindowName", arrow20::int32(), false),
            arrow20::field("OpenerName", arrow20::int32(), false),
            arrow20::field("HistoryLength", arrow20::int16(), false),
            arrow20::field("BrowserLanguage", arrow20::utf8(), false),
            arrow20::field("BrowserCountry", arrow20::utf8(), false),
            arrow20::field("SocialNetwork", arrow20::utf8(), false),
            arrow20::field("SocialAction", arrow20::utf8(), false),
            arrow20::field("HTTPError", arrow20::int16(), false),
            arrow20::field("SendTiming", arrow20::int32(), false),
            arrow20::field("DNSTiming", arrow20::int32(), false),
            arrow20::field("ConnectTiming", arrow20::int32(), false),
            arrow20::field("ResponseStartTiming", arrow20::int32(), false),
            arrow20::field("ResponseEndTiming", arrow20::int32(), false),
            arrow20::field("FetchTiming", arrow20::int32(), false),
            arrow20::field("SocialSourceNetworkID", arrow20::int16(), false),
            arrow20::field("SocialSourcePage", arrow20::utf8(), false),
            arrow20::field("ParamPrice", arrow20::int64(), false),
            arrow20::field("ParamOrderID", arrow20::utf8(), false),
            arrow20::field("ParamCurrency", arrow20::utf8(), false),
            arrow20::field("ParamCurrencyID", arrow20::int16(), false),
            arrow20::field("OpenstatServiceName", arrow20::utf8(), false),
            arrow20::field("OpenstatCampaignID", arrow20::utf8(), false),
            arrow20::field("OpenstatAdID", arrow20::utf8(), false),
            arrow20::field("OpenstatSourceID", arrow20::utf8(), false),
            arrow20::field("UTMSource", arrow20::utf8(), false),
            arrow20::field("UTMMedium", arrow20::utf8(), false),
            arrow20::field("UTMCampaign", arrow20::utf8(), false),
            arrow20::field("UTMContent", arrow20::utf8(), false),
            arrow20::field("UTMTerm", arrow20::utf8(), false),
            arrow20::field("FromTag", arrow20::utf8(), false),
            arrow20::field("HasGCLID", arrow20::int16(), false),
            arrow20::field("RefererHash", arrow20::int64(), false),
            arrow20::field("URLHash", arrow20::int64(), false),
            arrow20::field("CLID", arrow20::int32(), false)
    });
}

std::shared_ptr<arrow20::RecordBatch> TCickBenchHelper::TestArrowBatch(ui64, ui64 begin, size_t rowCount, const ui64 tsStepUs) const {
    std::shared_ptr<arrow20::Schema> schema = GetArrowSchema();
    UNIT_ASSERT(schema);
    UNIT_ASSERT(schema->num_fields());

    std::unique_ptr<arrow20::RecordBatchBuilder> builders;
    auto res = arrow20::RecordBatchBuilder::Make(schema, arrow20::default_memory_pool(), rowCount, &builders);
    UNIT_ASSERT(res.ok());

    for (i32 col = 0; col < schema->num_fields(); ++col) {
        auto& field = schema->field(col);
        auto typeId = field->type()->id();
        for (size_t row = 0; row < rowCount; ++row) {
            ui64 value = begin + row;
            switch (typeId) {
                case arrow20::Type::INT16: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::Int16Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow20::Type::INT32: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::Int32Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow20::Type::INT64: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::Int64Builder>(col)->Append(value).ok());
                    break;
                }
                case arrow20::Type::TIMESTAMP: {
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::TimestampBuilder>(col)->Append(begin + row * tsStepUs).ok());
                    break;
                }
                case arrow20::Type::BINARY: {
                    auto str = ToString(value);
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::BinaryBuilder>(col)->Append(str.data(), str.size()).ok());
                    break;
                }
                case arrow20::Type::STRING: {
                    auto str = ToString(value);
                    UNIT_ASSERT(builders->GetFieldAs<arrow20::StringBuilder>(col)->Append(str.data(), str.size()).ok());
                    break;
                }
                default:
                    Y_ABORT("unexpected type");
            }
        }
    }

    std::shared_ptr<arrow20::RecordBatch> batch;
    UNIT_ASSERT(builders->Flush(&batch).ok());
    UNIT_ASSERT(batch);
    UNIT_ASSERT(batch->num_rows());
    UNIT_ASSERT(batch->Validate().ok());
    return batch;
}

// Table with NULLs

std::shared_ptr<arrow20::Schema> TTableWithNullsHelper::GetArrowSchema() const {
    return std::make_shared<arrow20::Schema>(
        std::vector<std::shared_ptr<arrow20::Field>>{
        arrow20::field("id", arrow20::int32(), false),
            arrow20::field("resource_id", arrow20::utf8()),
            arrow20::field("level", arrow20::int32()),
            arrow20::field("binary_str", arrow20::binary()),
            arrow20::field("jsonval", arrow20::utf8()),
            arrow20::field("jsondoc", arrow20::binary())
    });
}

std::shared_ptr<arrow20::RecordBatch> TTableWithNullsHelper::TestArrowBatch() const {
    return TestArrowBatch(0, 0, 10, 1);
}

std::shared_ptr<arrow20::RecordBatch> TTableWithNullsHelper::TestArrowBatch(ui64, ui64, size_t rowCount, const ui64 /*tsStepUs*/) const {
    rowCount = 10;
    std::shared_ptr<arrow20::Schema> schema = GetArrowSchema();

    arrow20::Int32Builder bId;
    arrow20::StringBuilder bResourceId;
    arrow20::Int32Builder bLevel;
    arrow20::BinaryBuilder bBinaryStr;
    arrow20::StringBuilder bJsonVal;
    arrow20::BinaryBuilder bJsonDoc;

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

    std::shared_ptr<arrow20::Int32Array> aId;
    std::shared_ptr<arrow20::StringArray> aResourceId;
    std::shared_ptr<arrow20::Int32Array> aLevel;
    std::shared_ptr<arrow20::BinaryArray> aBinaryStr;
    std::shared_ptr<arrow20::StringArray> aJsonVal;
    std::shared_ptr<arrow20::BinaryArray> aJsonDoc;

    Y_ABORT_UNLESS(bId.Finish(&aId).ok());
    Y_ABORT_UNLESS(bResourceId.Finish(&aResourceId).ok());
    Y_ABORT_UNLESS(bLevel.Finish(&aLevel).ok());
    Y_ABORT_UNLESS(bBinaryStr.Finish(&aBinaryStr).ok());
    Y_ABORT_UNLESS(bJsonVal.Finish(&aJsonVal).ok());
    Y_ABORT_UNLESS(bJsonDoc.Finish(&aJsonDoc).ok());

    return arrow20::RecordBatch::Make(schema, rowCount, { aId, aResourceId, aLevel, aBinaryStr, aJsonVal, aJsonDoc });
}

}
