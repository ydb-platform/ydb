#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"
#include "service_table.h"

#include <ydb/core/protos/console_config.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <google/protobuf/text_format.h>
#include <library/cpp/yson/writer.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvExplainDataQueryRequest = TGrpcRequestOperationCall<Ydb::Table::ExplainDataQueryRequest,
    Ydb::Table::ExplainDataQueryResponse>;

class TExplainDataQueryRPC : public TRpcKqpRequestActor<TExplainDataQueryRPC, TEvExplainDataQueryRequest> {
    using TBase = TRpcKqpRequestActor<TExplainDataQueryRPC, TEvExplainDataQueryRequest>;

public:
    TExplainDataQueryRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TExplainDataQueryRPC::StateWork);
        Proceed(ctx);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Proceed(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);
        ev->Record.MutableRequest()->SetClientAddress(Request_->GetPeerName());

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (CheckSession(req->session_id(), Request_.get())) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        if (!CheckQuery(req->yql_text(), Request_.get())) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(req->yql_text());
        ev->Record.MutableRequest()->SetCollectDiagnostics(req->Getcollect_full_diagnostics());

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();

            Ydb::Table::ExplainQueryResult queryResult;
            queryResult.set_query_ast(kqpResponse.GetQueryAst());
            queryResult.set_query_plan(kqpResponse.GetQueryPlan());
            queryResult.set_query_full_diagnostics(kqpResponse.GetQueryDiagnostics());

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, queryResult, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }
};

void DoExplainDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExplainDataQueryRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
