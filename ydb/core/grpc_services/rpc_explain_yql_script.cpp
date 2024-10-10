#include "service_yql_scripting.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"

#include <ydb/public/api/protos/ydb_scripting.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvExplainYqlScriptRequest =
    TGrpcRequestOperationCall<Ydb::Scripting::ExplainYqlRequest, Ydb::Scripting::ExplainYqlResponse>;

using namespace Ydb;

class TExplainYqlScriptRPC : public TRpcKqpRequestActor<TExplainYqlScriptRPC, TEvExplainYqlScriptRequest> {
    using TBase = TRpcKqpRequestActor<TExplainYqlScriptRPC, TEvExplainYqlScriptRequest>;

public:
    using TResult = Ydb::Scripting::ExplainYqlResult;

    TExplainYqlScriptRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TExplainYqlScriptRPC::StateWork);
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

        ev->Record.MutableRequest()->SetCancelAfterMs(GetCancelAfter().MilliSeconds());
        ev->Record.MutableRequest()->SetTimeoutMs(GetOperationTimeout().MilliSeconds());

        auto& script = req->script();
        NYql::TIssues issues;
        if (!CheckQuery(script, issues)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        switch (req->mode()) {
            // KIKIMR-10990
            //case Ydb::Scripting::ExplainYqlRequest_Mode_PARSE:
            //    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_PARSE);
            //    break;
            case Ydb::Scripting::ExplainYqlRequest_Mode_VALIDATE:
                ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_VALIDATE);
                break;
            case Ydb::Scripting::ExplainYqlRequest_Mode_PLAN:
                ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
                break;
            default:
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                        << "Unknown explain mode"));
                return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
        ev->Record.MutableRequest()->SetQuery(script);
        ev->Record.MutableRequest()->SetKeepSession(false);

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();
            const auto& queryParameters = kqpResponse.GetQueryParameters();

            Ydb::Scripting::ExplainYqlResult queryResult;
            queryResult.set_plan(kqpResponse.GetQueryPlan());
            for (const auto& queryParameter : queryParameters) {
                Ydb::Type parameterType;
                try {
                    ConvertMiniKQLTypeToYdbType(queryParameter.GetType(), parameterType);
                } catch (const std::exception& ex) {
                    NYql::TIssues issues;
                    issues.AddIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
                }
                queryResult.mutable_parameters_types()->insert({ queryParameter.GetName(), parameterType });
            }

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, queryResult, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }
};

void DoExplainYqlScript(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExplainYqlScriptRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
