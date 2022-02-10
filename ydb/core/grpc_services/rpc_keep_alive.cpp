#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_common.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

class TKeepAliveRPC : public TRpcKqpRequestActor<TKeepAliveRPC, TEvKeepAliveRequest> {
    using TBase = TRpcKqpRequestActor<TKeepAliveRPC, TEvKeepAliveRequest>;

public:
    TKeepAliveRPC(TEvKeepAliveRequest* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        KeepAliveImpl(ctx);
        Become(&TKeepAliveRPC::StateWork);
    }
private:
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) { 
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvPingSessionResponse, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void KeepAliveImpl(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvPingSessionRequest>();

        NYql::TIssues issues;
        if (CheckSession(req->session_id(), issues)) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        ev->Record.MutableRequest()->SetTimeoutMs(GetOperationTimeout().MilliSeconds());
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            Ydb::Table::KeepAliveResult result;
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        } else {
            return OnProcessError(record, ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvPingSessionResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        if (record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            Ydb::Table::KeepAliveResult result;
            result.set_session_status(record.GetResponse().GetSessionStatus());
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
        } else {
            OnKqpError(record, ctx);
        }
    }

    void ReplyWithResult(StatusIds::StatusCode status, const Ydb::Table::KeepAliveResult& result,
        const TActorContext& ctx)
    {
        Request().SendResult(result, status);
        Die(ctx);
    }
};

void TGRpcRequestProxy::Handle(TEvKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TKeepAliveRPC(ev->Release().Release()));
}

} // namespace NGRpcService
} // namespace NKikimr
