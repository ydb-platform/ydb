#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"
#include "service_table.h"
#include "audit_dml_operations.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvRollbackTransactionRequest = TGrpcRequestOperationCall<Ydb::Table::RollbackTransactionRequest,
    Ydb::Table::RollbackTransactionResponse>;

class TRollbackTransactionRPC : public TRpcKqpRequestActor<TRollbackTransactionRPC, TEvRollbackTransactionRequest> {
    using TBase = TRpcKqpRequestActor<TRollbackTransactionRPC, TEvRollbackTransactionRequest>;

public:
    TRollbackTransactionRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        RollbackTransactionImpl(ctx);
        Become(&TRollbackTransactionRPC::StateWork);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default: TBase::StateWork(ev);
        }
    }

private:
    void RollbackTransactionImpl(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        TString sessionId;
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);

        if (CheckSession(req->session_id(), Request_.get())) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (!req->tx_id()) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty transaction id."));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_ROLLBACK_TX);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(req->tx_id());

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();

            // RollbackTransaction does not have specific Result, use RollbackTransactionResponse as no-op type substitute
            AuditContextAppend(Request_.get(), *GetProtoRequest(), Ydb::Table::RollbackTransactionResponse());

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }

    void ReplyWithResult(StatusIds::StatusCode status,
                         const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
                         const TActorContext& ctx) {
        NYql::TIssues issues;
        IssuesFromMessage(message, issues);
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }
};

void DoRollbackTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRollbackTransactionRPC(p.release()));
}

template<>
IActor* TEvRollbackTransactionRequest::CreateRpcActor(IRequestOpCtx* msg) {
    return new TRollbackTransactionRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
