#include "service_query.h"
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/query/service_query.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/grpc_services/audit_dml_operations.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <ydb/public/api/protos/ydb_query.pb.h>

namespace NKikimr::NGRpcService {

namespace {

using TEvBeginTransactionRequest = TGrpcRequestOperationCall<Ydb::Query::BeginTransactionRequest,
    Ydb::Query::BeginTransactionResponse>;
using TEvCommitTransactionRequest = TGrpcRequestOperationCall<Ydb::Query::CommitTransactionRequest,
    Ydb::Query::CommitTransactionResponse>;
using TEvRollbackTransactionRequest = TGrpcRequestOperationCall<Ydb::Query::RollbackTransactionRequest,
    Ydb::Query::RollbackTransactionResponse>;

TString GetTransactionModeName(const Ydb::Query::TransactionSettings& settings) {
    switch (settings.tx_mode_case()) {
        case Ydb::Query::TransactionSettings::kSerializableReadWrite:
            return "SerializableReadWrite";
        case Ydb::Query::TransactionSettings::kOnlineReadOnly:
            return "OnlineReadOnly";
        case Ydb::Query::TransactionSettings::kStaleReadOnly:
            return "StaleReadOnly";
        case Ydb::Query::TransactionSettings::kSnapshotReadOnly:
            return "SnapshotReadOnly";
        default:
            return "Unknown";
    }
}

class TBeginTransactionRPC : public  TActorBootstrapped<TBeginTransactionRPC> {
public:
    TBeginTransactionRPC(IRequestNoOpCtx* msg)
        : Request(msg) {}

    void Bootstrap(const TActorContext&) {
        BeginTransactionImpl();
        Become(&TBeginTransactionRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void BeginTransactionImpl() {
        const auto req = TEvBeginTransactionRequest::GetProtoRequest(Request);
        const auto traceId = Request->GetTraceId();

        AuditContextAppend(Request.get(), *req);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request);
        SetDatabase(ev, *Request);
        ev->Record.MutableRequest()->SetClientAddress(Request->GetPeerName());

        if (CheckSession(req->session_id(), Request.get())) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST);
        }

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (!req->has_tx_settings()) {
            Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty tx_settings."));
            return Reply(Ydb::StatusIds::BAD_REQUEST);
        }

        switch (req->tx_settings().tx_mode_case()) {
            case Ydb::Query::TransactionSettings::kOnlineReadOnly:
            case Ydb::Query::TransactionSettings::kStaleReadOnly: {
                Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                    << "Failed to begin transaction: open transactions not supported for transaction mode "
                    << GetTransactionModeName(req->tx_settings())
                    << ", use tx_control field in ExecuteDataQueryRequest to begin transaction with this mode."));
                return Reply(Ydb::StatusIds::BAD_REQUEST);
            }
            case Ydb::Query::TransactionSettings::TX_MODE_NOT_SET:
                Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                    << "Failed to begin transaction: tx mode was not set"));
                return Reply(Ydb::StatusIds::BAD_REQUEST);
            case Ydb::Query::TransactionSettings::kSerializableReadWrite:
                ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
                break;
            case Ydb::Query::TransactionSettings::kSnapshotReadOnly:
                ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_snapshot_read_only();
                break;
        }

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetRef();
        FillCommonKqpRespFields(record, Request.get());

        auto beginTxResult = TEvBeginTransactionRequest::AllocateResult<Ydb::Query::BeginTransactionResponse>(Request);
        beginTxResult->set_status(record.GetYdbStatus());
        if (record.HasResponse()) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(issueMessage, issues);
            Request->RaiseIssues(issues);
            if (kqpResponse.HasTxMeta()) {
                beginTxResult->mutable_tx_meta()->set_id(kqpResponse.GetTxMeta().id());
            }
            *beginTxResult->mutable_issues() = issueMessage;
        }

        Reply(record.GetYdbStatus(), beginTxResult);
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);

        Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message));
        Reply(Ydb::StatusIds::INTERNAL_ERROR);
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TBeginTransactionRPC in state " << state << " received unexpected event " <<
            ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        this->PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NProtoBuf::Message* resp) {
        Request->Reply(resp, status);
        this->PassAway();
    }

    std::shared_ptr<IRequestNoOpCtx> Request;
};

class TFinishTransactionRPC : public  TActorBootstrapped<TFinishTransactionRPC> {
public:
    TFinishTransactionRPC(IRequestNoOpCtx* msg)
        : Request(msg)
    {}

    void Bootstrap(const TActorContext&) {
        FinishTransactionImpl();
        Become(&TFinishTransactionRPC::StateWork);
    }

private:
    virtual std::pair<TString, TString> GetReqData() const = 0;
    virtual void Fill(NKikimrKqp::TQueryRequest* req) const = 0;
    virtual NProtoBuf::Message* CreateResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) const = 0;

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                default:
                    UnexpectedEvent(__func__, ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    void FinishTransactionImpl() {
        const auto traceId = Request->GetTraceId();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request);
        SetDatabase(ev, *Request);
        ev->Record.MutableRequest()->SetClientAddress(Request->GetPeerName());

        const auto& [sessionId, txId] = GetReqData();

        if (CheckSession(sessionId, Request.get())) {
            ev->Record.MutableRequest()->SetSessionId(sessionId);
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST);
        }

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (!txId) {
            NYql::TIssues issues;
            Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty transaction id."));
            return Reply(Ydb::StatusIds::BAD_REQUEST);
        }

        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(txId);

        Fill(ev->Record.MutableRequest());

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetRef();
        FillCommonKqpRespFields(record, Request.get());

        NYql::TIssues issues;
        if (record.HasResponse()) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();
            NYql::IssuesFromMessage(issueMessage, issues);
            Request->RaiseIssues(issues);
        }

        Reply(record.GetYdbStatus(), CreateResult(record.GetYdbStatus(), issues));
    }

    void InternalError(const TString& message) {
        ALOG_ERROR(NKikimrServices::RPC_REQUEST, "Internal error, message: " << message);

        Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, message));
        Reply(Ydb::StatusIds::INTERNAL_ERROR);
    }

    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TFinishTransactionRPC in state " << state << " received unexpected event " <<
            ev->GetTypeName() << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        this->PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, NProtoBuf::Message* resp) {
        Request->Reply(resp, status);
        this->PassAway();
    }

protected:
    std::shared_ptr<IRequestNoOpCtx> Request;
};

class TCommitTransactionRPC : public TFinishTransactionRPC {
public:
    using TFinishTransactionRPC::TFinishTransactionRPC;

private:
    std::pair<TString, TString> GetReqData() const override {
        const auto req = TEvCommitTransactionRequest::GetProtoRequest(Request);
        AuditContextAppend(Request.get(), *req);
        return {req->session_id(), req->tx_id()};
    }

    void Fill(NKikimrKqp::TQueryRequest* req) const override {
        req->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
        req->MutableTxControl()->set_commit_tx(true);
    }

    NProtoBuf::Message* CreateResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) const override {
        auto result = TEvCommitTransactionRequest::AllocateResult<Ydb::Query::CommitTransactionResponse>(Request);
        result->set_status(status);
        NYql::IssuesToMessage(issues, result->mutable_issues());
        return result;
    }
};

class TRollbackTransactionRPC : public TFinishTransactionRPC {
public:
    using TFinishTransactionRPC::TFinishTransactionRPC;

private:
    std::pair<TString, TString> GetReqData() const override {
        const auto req = TEvRollbackTransactionRequest::GetProtoRequest(Request);
        AuditContextAppend(Request.get(), *req);
        return {req->session_id(), req->tx_id()};
    }

    void Fill(NKikimrKqp::TQueryRequest* req) const override {
        req->SetAction(NKikimrKqp::QUERY_ACTION_ROLLBACK_TX);
    }

    NProtoBuf::Message* CreateResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) const override {
        auto result = TEvRollbackTransactionRequest::AllocateResult<Ydb::Query::RollbackTransactionResponse>(Request);
        result->set_status(status);
        NYql::IssuesToMessage(issues, result->mutable_issues());
        return result;
    }
};

}

namespace NQuery {

void DoBeginTransaction(std::unique_ptr<IRequestNoOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(new TBeginTransactionRPC(ctx.release()));
}

void DoCommitTransaction(std::unique_ptr<IRequestNoOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(new TCommitTransactionRPC(ctx.release()));
}

void DoRollbackTransaction(std::unique_ptr<IRequestNoOpCtx> ctx, const IFacilityProvider& provider) {
    provider.RegisterActor(new TRollbackTransactionRPC(ctx.release()));
}

}

}
