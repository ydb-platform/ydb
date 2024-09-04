#pragma once

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {
namespace NDataShard {
namespace NKqpHelpers {

    using TEvExecuteDataQueryRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest,
        Ydb::Table::ExecuteDataQueryResponse>;

    using TEvExecuteSchemeQueryRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteSchemeQueryRequest,
        Ydb::Table::ExecuteSchemeQueryResponse>;

    using TEvCreateSessionRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
        Ydb::Table::CreateSessionResponse>;

    using TEvDeleteSessionRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
        Ydb::Table::DeleteSessionResponse>;

    using TEvCommitTransactionRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CommitTransactionRequest,
        Ydb::Table::CommitTransactionResponse>;

    template<class TResp>
    inline TResp AwaitResponse(TTestActorRuntime& runtime, NThreading::TFuture<TResp> f) {
        if (!f.HasValue() && !f.HasException()) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return f.HasValue() || f.HasException();
            };
            options.FinalEvents.emplace_back([&](IEventHandle&) {
                return f.HasValue() || f.HasException();
            });

            runtime.DispatchEvents(options);

            UNIT_ASSERT(f.HasValue() || f.HasException());
        }

        return f.ExtractValueSync();
    }

    inline TString CreateSessionRPC(TTestActorRuntime& runtime, const TString& database = {}) {
        Ydb::Table::CreateSessionRequest request;
        auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
           std::move(request), database, /* token */ "", runtime.GetActorSystem(0));
        TString sessionId;
        auto response = AwaitResponse(runtime, future);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::CreateSessionResult result;
        response.operation().result().UnpackTo(&result);
        sessionId = result.session_id();
        UNIT_ASSERT(!sessionId.empty());
        return sessionId;
    }

    inline TString CommitTransactionRPC(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& database = {}) {
        Ydb::Table::CommitTransactionRequest request;
        request.set_session_id(sessionId);
        request.set_tx_id(txId);

        auto future = NRpcService::DoLocalRpc<TEvCommitTransactionRequest>(
            std::move(request), database, "", runtime.GetActorSystem(0));
        auto response = AwaitResponse(runtime, future);
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        return "";
    }

    inline NThreading::TFuture<Ydb::Table::ExecuteDataQueryResponse> SendRequest(
        TTestActorRuntime& runtime, Ydb::Table::ExecuteDataQueryRequest&& request, const TString& database = {})
    {
        return NRpcService::DoLocalRpc<TEvExecuteDataQueryRequest>(
            std::move(request), database, /* token */ "", runtime.GetActorSystem(0));
    }

    inline Ydb::Table::ExecuteDataQueryRequest MakeSimpleRequestRPC(
        const TString& sql, const TString& sessionId, const TString& txId, bool commitTx, bool staleRo = false) {

        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(sessionId);
        request.mutable_tx_control()->set_commit_tx(commitTx);
        if (txId.empty()) {
            // txId is empty, start a new tx
            if (!staleRo) {
                request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            } else {
                request.mutable_tx_control()->mutable_begin_tx()->mutable_stale_read_only();
            }
        } else {
            // continue new tx.
            request.mutable_tx_control()->set_tx_id(txId);
        }

        request.mutable_query()->set_yql_text(sql);
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

    inline void CloseSession(TTestActorRuntime& runtime, const TString& sessionId) {
        Ydb::Table::DeleteSessionRequest request;
        request.set_session_id(sessionId);
        auto future = NRpcService::DoLocalRpc<TEvDeleteSessionRequest>(
            std::move(request), "", /* token */ "", runtime.GetActorSystem(0));
    }

    inline THolder<NKqp::TEvKqp::TEvQueryRequest> MakeStreamRequest(
        const TActorId sender,
        const TString& sql,
        const bool collectStats = false)
    {
        Y_UNUSED(collectStats);
        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        request->Record.MutableRequest()->SetKeepSession(false);
        request->Record.MutableRequest()->SetQuery(sql);
        ActorIdToProto(sender, request->Record.MutableRequestActorId());
        return request;
    }

    inline TString FormatResult(const Ydb::ResultSet& rs) {
        Cerr << JoinSeq(", ", rs.rows()) << Endl;
        return JoinSeq(", ", rs.rows());
    }

    inline TString FormatResult(const Ydb::Table::ExecuteQueryResult& result) {
        if (result.result_sets_size() == 0) {
            return "<empty>";
        }
        if (result.result_sets_size() == 1) {
            return FormatResult(result.result_sets(0));
        }
        TStringBuilder sb;
        for (int i = 0; i < result.result_sets_size(); ++i) {
            if (i != 0) {
                sb << "\n";
            }
            sb << FormatResult(result.result_sets(i));
        }
        return sb;
    }

    inline TString FormatResult(const Ydb::Table::ExecuteDataQueryResponse& response) {
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        return FormatResult(result);
    }

    inline auto KqpSimpleSend(TTestActorRuntime& runtime, const TString& query, bool staleRo = false, const TString& database = {}) {
        TString sessionId = CreateSessionRPC(runtime, database);
        TString txId;
        return SendRequest(runtime, MakeSimpleRequestRPC(query, sessionId, txId, /* commitTx */ true, staleRo), database);
    }

    inline TString KqpSimpleExec(TTestActorRuntime& runtime, const TString& query, bool staleRo = false, const TString& database = {}) {
        auto response = AwaitResponse(runtime, KqpSimpleSend(runtime, query, staleRo, database));
        return FormatResult(response);
    }

    inline TString KqpSimpleStaleRoExec(TTestActorRuntime& runtime, const TString& query, const TString& database = {}) {
        return KqpSimpleExec(runtime, query, true, database);
    }

    inline TString KqpSimpleBegin(TTestActorRuntime& runtime, TString& sessionId, TString& txId, const TString& query) {
        sessionId = CreateSessionRPC(runtime);
        txId.clear();
        auto response = AwaitResponse(runtime, SendRequest(runtime, MakeSimpleRequestRPC(query, sessionId, txId, false /* commitTx */)));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId = result.tx_meta().id();
        return FormatResult(result);
    }

    inline TString KqpSimpleContinue(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& query) {
        Y_ABORT_UNLESS(!txId.empty(), "continue on empty transaction");
        auto response = AwaitResponse(runtime, SendRequest(runtime, MakeSimpleRequestRPC(query, sessionId, txId, false /* commitTx */)));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        Y_ABORT_UNLESS(result.tx_meta().id() == txId);
        return FormatResult(result);
    }

    inline auto KqpSimpleSendCommit(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& query) {
        Y_ABORT_UNLESS(!txId.empty(), "commit on empty transaction");
        return SendRequest(runtime, MakeSimpleRequestRPC(query, sessionId, txId, true /* commitTx */));
    }

    inline TString KqpSimpleCommit(TTestActorRuntime& runtime, const TString& sessionId, const TString& txId, const TString& query) {
        auto response = AwaitResponse(runtime, KqpSimpleSendCommit(runtime, sessionId, txId, query));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        Y_ABORT_UNLESS(result.tx_meta().id().empty(), "must be empty transaction");
        return FormatResult(result);
    }

    inline Ydb::Table::ExecuteSchemeQueryRequest MakeSchemeRequestRPC(
        const TString& sql, const TString& sessionId)
    {
        Ydb::Table::ExecuteSchemeQueryRequest request;
        request.set_session_id(sessionId);
        request.set_yql_text(sql);
        return request;
    }

    inline NThreading::TFuture<Ydb::Table::ExecuteSchemeQueryResponse> SendRequest(
        TTestActorRuntime& runtime, Ydb::Table::ExecuteSchemeQueryRequest&& request, const TString& database = {})
    {
        return NRpcService::DoLocalRpc<TEvExecuteSchemeQueryRequest>(
            std::move(request), database, /* token */ "", runtime.GetActorSystem(0));
    }

    inline TString KqpSchemeExec(TTestActorRuntime& runtime, const TString& query) {
        TString sessionId = CreateSessionRPC(runtime);
        auto response = AwaitResponse(runtime, SendRequest(runtime, MakeSchemeRequestRPC(query, sessionId)));
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TStringBuilder() << "ERROR: " << response.operation().status();
        }
        return "SUCCESS";
    }

} // namespace NKqpHelpers
} // namespace NDataShard
} // namespace NKikimr
