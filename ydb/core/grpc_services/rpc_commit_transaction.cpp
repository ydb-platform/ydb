#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_integrity_trails.h>

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

using TEvCommitTransactionRequest = TGrpcRequestOperationCall<Ydb::Table::CommitTransactionRequest,
Ydb::Table::CommitTransactionResponse>;

class TCommitTransactionRPC : public TRpcKqpRequestActor<TCommitTransactionRPC, TEvCommitTransactionRequest> {
    using TBase = TRpcKqpRequestActor<TCommitTransactionRPC, TEvCommitTransactionRequest>;

public:
    TCommitTransactionRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        CommitTransactionImpl(ctx);
        Become(&TCommitTransactionRPC::StateWork);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default: TBase::StateWork(ev);
        }
    }

private:
    void CommitTransactionImpl(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        AuditContextAppend(Request_.get(), *req);
        NDataIntegrity::LogIntegrityTrails(traceId, *req, ctx);

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

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(req->tx_id());
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        ev->Record.MutableRequest()->SetStatsMode(GetKqpStatsMode(req->collect_stats()));
        ev->Record.MutableRequest()->SetCollectStats(req->collect_stats());

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        NDataIntegrity::LogIntegrityTrails(Request_->GetTraceId(), *GetProtoRequest(), ev, ctx);

        const auto& record = ev->Get()->Record.GetRef();
        SetCost(record.GetConsumedRu());
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();

            auto commitResult = TEvCommitTransactionRequest::AllocateResult<Ydb::Table::CommitTransactionResult>(Request_);

            if (kqpResponse.HasQueryStats()) {
                FillQueryStats(*commitResult->mutable_query_stats(), kqpResponse);
            }

            AuditContextAppend(Request_.get(), *GetProtoRequest(), *commitResult);

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, *commitResult, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }
};

void DoCommitTransactionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCommitTransactionRPC(p.release()));
}

template<>
IActor* TEvCommitTransactionRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TCommitTransactionRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
