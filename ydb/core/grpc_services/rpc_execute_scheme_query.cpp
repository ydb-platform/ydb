#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"
#include "service_table.h"

#include <ydb/core/protos/console_config.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvExecuteSchemeQueryRequest = TGrpcRequestOperationCall<Ydb::Table::ExecuteSchemeQueryRequest,
    Ydb::Table::ExecuteSchemeQueryResponse>;

class TExecuteSchemeQueryRPC : public TRpcKqpRequestActor<TExecuteSchemeQueryRPC, TEvExecuteSchemeQueryRequest> {
    using TBase = TRpcKqpRequestActor<TExecuteSchemeQueryRPC, TEvExecuteSchemeQueryRequest>;

public:
    TExecuteSchemeQueryRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TExecuteSchemeQueryRPC::StateWork);
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
        const auto requestType = Request_->GetRequestType();

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

        if (requestType) {
            ev->Record.SetRequestType(requestType.GetRef());
        }

        if (!CheckQuery(req->yql_text(), Request_.get())) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(req->yql_text());

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();

            Reply(Ydb::StatusIds::SUCCESS, issueMessage, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }
};

void DoExecuteSchemeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExecuteSchemeQueryRPC(p.release()));
}

template<>
IActor* TEvExecuteSchemeQueryRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TExecuteSchemeQueryRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
