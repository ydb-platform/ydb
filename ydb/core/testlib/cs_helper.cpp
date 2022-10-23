#include "cs_helper.h"
#include <library/cpp/actors/core/event.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

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

}
