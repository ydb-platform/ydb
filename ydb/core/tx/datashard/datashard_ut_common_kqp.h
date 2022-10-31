#pragma once

#include "datashard_ut_common.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {
namespace NDataShard {
namespace NKqpHelpers {

    using TEvExecuteDataQueryRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest,
        Ydb::Table::ExecuteDataQueryResponse>;

    using TEvCreateSessionRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
        Ydb::Table::CreateSessionResponse>;

    template<class TResp>
    inline TResp AwaitResponse(TTestActorRuntime& runtime, NThreading::TFuture<TResp> f) {
        size_t responses = 0;
        TResp response;
        f.Subscribe([&](NThreading::TFuture<TResp> fut){
            ++responses;
            TResp r = fut.ExtractValueSync();
            response.Swap(&r);
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return responses >= 1;
        };

        runtime.DispatchEvents(options);
        return response;
    }

    inline TString CreateSessionRPC(TTestActorRuntime& runtime, const TString& database = {}) {
        Ydb::Table::CreateSessionRequest request;
        auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
           std::move(request), database, "", /* token */ runtime.GetActorSystem(0));
        TString sessionId;
        auto response = AwaitResponse(runtime, future);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateSessionResult result;
        response.operation().result().UnpackTo(&result);
        sessionId = result.session_id();
        UNIT_ASSERT(!sessionId.empty());
        return sessionId;
    }

    inline Ydb::Table::ExecuteDataQueryResponse ExecuteDataQueryRPCResponse(
        TTestActorRuntime& runtime, Ydb::Table::ExecuteDataQueryRequest&& request, const TString& database = {}) {
        auto future = NRpcService::DoLocalRpc<TEvExecuteDataQueryRequest>(
            std::move(request), database, "" /* token */, runtime.GetActorSystem(0));
        return AwaitResponse(runtime, future);
    }

    inline Ydb::Table::ExecuteDataQueryRequest MakeSimpleRequestRPC(
        const TString& sql, const TString& sessionId, const TString& txId, bool commit_tx) {

        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(sessionId);
        request.mutable_tx_control()->set_commit_tx(commit_tx);
        if (txId.empty()) {
            // txId is empty, start a new tx
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        } else {
            // continue new tx.
            request.mutable_tx_control()->set_tx_id(txId);
        }

        request.mutable_query()->set_yql_text(sql);
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSimpleRequest(
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSimpleStaleRoRequest(
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_stale_read_only();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeBeginRequest(
            const TString& sessionId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeContinueRequest(
            const TString& sessionId,
            const TString& txId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->set_tx_id(txId);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeCommitRequest(
            const TString& sessionId,
            const TString& txId,
            const TString& sql,
            const TString& database = {})
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        request->Record.MutableRequest()->MutableTxControl()->set_tx_id(txId);
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(sql);
        if (!database.empty()) {
            request->Record.MutableRequest()->SetDatabase(database);
        }
        return request;
    }

    inline void SendRequest(
            TTestActorRuntime& runtime,
            TActorId sender,
            THolder<NKqp::TEvKqp::TEvQueryRequest> request)
    {
        runtime.Send(
            new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()),
            0, /* via actor system */ true);
    }

    inline NKqp::TEvKqp::TEvQueryResponse::TPtr ExecRequest(
            TTestActorRuntime& runtime,
            TActorId sender,
            THolder<NKqp::TEvKqp::TEvQueryRequest> request)
    {
        SendRequest(runtime, sender, std::move(request));
        return runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
    }

    inline void CloseSession(TTestActorRuntime& runtime, TActorId sender, const TString& sessionId) {
        auto request = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        request->Record.MutableRequest()->SetSessionId(sessionId);
        runtime.Send(
            new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()),
            0, /* via actor system */ true);
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeStreamRequest(
        const TActorId sender,
        const TString& sql,
        const bool collectStats = false)
    {
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        request->Record.MutableRequest()->SetKeepSession(false);
        request->Record.MutableRequest()->SetQuery(sql);
        request->Record.MutableRequest()->SetProfile(collectStats);
        ActorIdToProto(sender, request->Record.MutableRequestActorId());
        return request;
    }

    inline TString KqpSimpleExec(TTestActorRuntime& runtime, const TString& query) {
        TString sessionId = CreateSessionRPC(runtime);
        TString txId;
        auto response = ExecuteDataQueryRPCResponse(runtime, MakeSimpleRequestRPC(query, sessionId, txId, true /* commitTx */));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        if (result.result_sets_size() == 0) {
            return "<empty>";
        }
        UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1u);
        return JoinSeq(", ", result.result_sets(0).rows());
    }

    inline TString KqpSimpleStaleRoExec(TTestActorRuntime& runtime, const TString& query) {
        auto reqSender = runtime.AllocateEdgeActor();
        auto ev = ExecRequest(runtime, reqSender, MakeSimpleStaleRoRequest(query));
        auto& response = ev->Get()->Record.GetRef();
        if (response.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.GetYdbStatus();
        }
        if (response.GetResponse().GetResults().size() == 0) {
            return "<empty>";
        }
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetResults().size(), 1u);
        return response.GetResponse().GetResults()[0].GetValue().ShortDebugString();
    }

    inline TString KqpSimpleBegin(TTestActorRuntime& runtime, TString& sessionId, TString& txId, const TString& query) {
        sessionId = CreateSessionRPC(runtime);
        Y_VERIFY(txId.empty(), "txId reused between transactions"); // ensure
        auto response = ExecuteDataQueryRPCResponse(runtime, MakeSimpleRequestRPC(query, sessionId, txId, false /* commitTx */));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId = result.tx_meta().id();
        if (result.result_sets_size() == 0) {
            return "<empty>";
        }
        UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1u);
        return JoinSeq(", ", result.result_sets(0).rows());
    }

    inline TString KqpSimpleContinue(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& query) {
        Y_VERIFY(!txId.empty(), "continue on empty transaction");
        auto response = ExecuteDataQueryRPCResponse(runtime, MakeSimpleRequestRPC(query, sessionId, txId, false /* commitTx */));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        Y_VERIFY(result.tx_meta().id() == txId);
        if (result.result_sets_size() == 0) {
            return "<empty>";
        }
        UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1u);
        Cerr << JoinSeq(", ", result.result_sets(0).rows()) << Endl;
        return JoinSeq(", ", result.result_sets(0).rows());
    }

    inline TString KqpSimpleCommit(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& query) {
        Y_VERIFY(!txId.empty(), "commit on empty transaction");
        auto response = ExecuteDataQueryRPCResponse(runtime, MakeSimpleRequestRPC(query, sessionId, txId, true /* commitTx */));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        Y_VERIFY(result.tx_meta().id().empty(), "must be empty transaction");
        if (result.result_sets_size() == 0) {
            return "<empty>";
        }
        UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1u);
        Cerr << JoinSeq(", ", result.result_sets(0).rows()) << Endl;
        return JoinSeq(", ", result.result_sets(0).rows());
    }

} // namespace NKqpHelpers
} // namespace NDataShard
} // namespace NKikimr
