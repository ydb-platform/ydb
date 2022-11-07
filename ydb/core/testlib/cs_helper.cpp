#include "cs_helper.h"
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <library/cpp/actors/core/event.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::Tests::NCS {

void THelper::CreateTestOlapStore(TActorId sender, TString scheme) {
    NKikimrSchemeOp::TColumnStoreDescription store;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &store));

    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
    op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
    op->SetWorkingDir("/Root");
    op->MutableCreateColumnStore()->CopyFrom(store);

    Server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = Server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    ui64 txId = ev->Get()->Record.GetTxId();
    WaitForSchemeOperation(sender, txId);
}

void THelper::CreateTestOlapTable(TActorId sender, TString storeName, TString scheme) {
    NKikimrSchemeOp::TColumnTableDescription table;
    UNIT_ASSERT(::google::protobuf::TextFormat::ParseFromString(scheme, &table));
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());
    auto* op = request->Record.MutableTransaction()->MutableModifyScheme();
    op->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
    op->SetWorkingDir("/Root/" + storeName);
    op->MutableCreateColumnTable()->CopyFrom(table);

    Server.GetRuntime()->Send(new IEventHandle(MakeTxProxyID(), sender, request.release()));
    auto ev = Server.GetRuntime()->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    ui64 txId = ev->Get()->Record.GetTxId();
    WaitForSchemeOperation(sender, txId);
}

std::shared_ptr<arrow::Schema> THelper::GetArrowSchema() {
    return std::make_shared<arrow::Schema>(
        std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("resource_id", arrow::utf8()),
            arrow::field("uid", arrow::utf8()),
            arrow::field("level", arrow::int32()),
            arrow::field("message", arrow::utf8())
    });
}

TString THelper::TestBlob(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
    auto batch = TestArrowBatch(pathIdBegin, tsBegin, rowCount);
    return NArrow::SerializeBatchNoCompression(batch);
}

void THelper::SendDataViaActorSystem(TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
    auto* runtime = Server.GetRuntime();
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
    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest,
        Ydb::Table::BulkUpsertResponse>;
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

std::shared_ptr<arrow::RecordBatch> THelper::TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
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

}
