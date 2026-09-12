#include "write_log_to_olap.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/typed_local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>

#include <ydb/core/kqp/ut/olap/operations/write_log_to_olap.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NLogToDB {

void TBaseDBLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (message.Component != Component) {
        return ;
    }

    if (!TableExists) {
        return ;
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for(auto& column: Columns) {
        column->Write(message);
        arrays.push_back(column->MakeArray());
    }
    auto batch = arrow::RecordBatch::Make(GetArrowSchema(), 1, arrays);
    SendDataViaActorSystem(batch);
}

TString TBaseDBLogWriter::GetStoreDescription() {

    TStringBuilder sb;
    for (const auto& column : Columns) {
        sb << "Columns{ Name: \"" << column->Name << "\" Type : \"" << column->Type << "\"";
        if (column->Settings.IsNotNull) {
            sb << " NotNull : true";
        }
        if (!column->Settings.Extra.empty()) {
            sb << " " << column->Settings.Extra;
        }
        sb << " }";
    }

    for (const auto& column : Columns) {
        if (column->Settings.IsPK) {
            sb << "KeyColumnNames: \"" << column->Name << "\"\n";
        }
    }

    TString storeDesc = Sprintf(R"(
        Name: "%s"
        ColumnShardCount: %d
        SchemaPresets {
            Name: "default"
            Schema {
                %s
                }
            }
        )", Settings.StoreName.c_str(), Settings.StoreShardsCount, sb.data());
    return storeDesc;
}

std::shared_ptr<arrow::Schema> TBaseDBLogWriter::GetArrowSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(Columns.size());
    for (const auto& column : Columns) {
        fields.emplace_back(column->MakeArrowField());
    }
    return std::make_shared<arrow::Schema>(std::move(fields));
}

TString TBaseDBLogWriter::GetTableDescription() {
    TStringBuilder sb;
    sb << "[";
    bool first = true;
    for (const auto& column : Columns) {
        if (column->Settings.IsShardingKey) {
            if (!first) {
                sb << ", ";
            } else {
                first = false;
            }
            sb << "\"" << column->Name << "\"\n";
        }
    }
    sb << "]";

    TString result = Sprintf(R"(
        Name: "%s"
        ColumnShardCount: %d
        Sharding {
            HashSharding {
                Function: %s
                Columns: %s
            }
        })", Settings.TableName.c_str(),
        Settings.TableShardsCount,
        NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction_Name(Settings.ShardingMethod).c_str(),
        sb.c_str());
    return result;
}

void TBaseDBLogWriter::WaitForSchemeOperation(TActorId sender, ui64 txId) {
    auto& server = GetRunner().GetTestServer();
    auto& runtime = *server.GetRuntime();
    auto& settings = server.GetSettings();
    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);

    // auto tid = ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
    const ui64 mask = static_cast<ui64>(0xff) << 56;
    ui64 tabletId = Tests::SchemeRoot;                                                 // @todo What value outside tests?
    ui32 id = settings.Domain;                                                         // @todo What value outside tests?
    auto tid = (tabletId & ~mask) | static_cast<ui64>(id & 0xff) << 56;

    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries()); // @todo What GetPipeConfigWithRetries() outside tests?
    runtime.template GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void TBaseDBLogWriter::ExecuteModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme) {
    auto& server = GetRunner().GetTestServer();
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    *request->Record.MutableTransaction()->MutableModifyScheme() = modifyScheme;
    TActorId sender = server.GetRuntime()->AllocateEdgeActor();
    server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = server.GetRuntime()->template GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    auto status = ev->Get()->Record.GetStatus();
    ui64 txId = ev->Get()->Record.GetTxId();
    UNIT_ASSERT(status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
    WaitForSchemeOperation(sender, txId);
}

void TBaseDBLogWriter::CreateStore() {
    TString scheme = GetStoreDescription();
    NKikimrSchemeOp::TColumnStoreDescription store;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &store));
    NKikimrSchemeOp::TModifyScheme op;
    op.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
    op.SetWorkingDir("/Root");
    op.MutableCreateColumnStore()->CopyFrom(store);
    ExecuteModifyScheme(op);
}

void TBaseDBLogWriter::CreateTable() {
    TString storeOrDirName = Settings.StoreName;
    TString scheme = GetTableDescription();
    NKikimrSchemeOp::TColumnTableDescription table;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &table));
    TString workingDir = "/Root";
    if (!storeOrDirName.empty()) {
        workingDir += "/" + storeOrDirName;
    }

    NKikimrSchemeOp::TModifyScheme op;
    op.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
    op.SetWorkingDir(workingDir);
    op.MutableCreateColumnTable()->CopyFrom(table);
    // @was: helper.ExecuteModifyScheme(op);
    ExecuteModifyScheme(op);
}

void TBaseDBLogWriter::SendDataViaActorSystem(std::shared_ptr<arrow::RecordBatch> batch) {

    auto* runtime = GetRunner().GetTestServer().GetRuntime();

    UNIT_ASSERT(batch);
    UNIT_ASSERT(batch->num_rows());
    auto data = NKikimr::NArrow::SerializeBatchNoCompression(batch);
    UNIT_ASSERT(!data.empty());
    TString serializedSchema = NKikimr::NArrow::SerializeSchema(*batch->schema());
    UNIT_ASSERT(serializedSchema);

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table("/Root/olapStore/olapTable");

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
        UNIT_ASSERT_VALUES_EQUAL(op.status(), Ydb::StatusIds::SUCCESS);
        responses.fetch_add(1);
    });

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return responses.load() >= 1;
    };

    runtime->DispatchEvents(options);
}

}
