#include "service_yql_scripting.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"

#include <ydb/public/api/protos/ydb_scripting.pb.h>

namespace NKikimr {
namespace NGRpcService {

using Ydb::Scripting::ExecuteYqlRequest;
using Ydb::Scripting::ExecuteYqlResponse;
using TEvExecuteYqlScriptRequest = TGrpcRequestOperationCall<ExecuteYqlRequest, ExecuteYqlResponse>;

using namespace Ydb;

class TExecuteYqlScriptRPC : public TRpcKqpRequestActor<TExecuteYqlScriptRPC, TEvExecuteYqlScriptRequest> {
    using TBase = TRpcKqpRequestActor<TExecuteYqlScriptRPC, TEvExecuteYqlScriptRequest>;

public:
    using TResult = Ydb::Scripting::ExecuteYqlResult;

    TExecuteYqlScriptRPC(IRequestOpCtx* msg)
        : TBase(msg)
    {
        // StreamExecuteYqlScript allows write in to table.
        // But CanselAfter should not trigger cancelation if we chage table
        // This logic is broken - just disable CancelAfter_ for a while
        CancelAfter_ = TDuration();
    }

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TExecuteYqlScriptRPC::StateWork);
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

        auto script = req->script();

        NYql::TIssues issues;
        if (!CheckQuery(script, issues)) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        ::Ydb::Operations::OperationParams operationParams;

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(
            NKikimrKqp::QUERY_ACTION_EXECUTE,
            NKikimrKqp::QUERY_TYPE_SQL_SCRIPT,
            SelfId(),
            Request_,
            TString(), //sessionId
            std::move(script),
            TString(), //queryId
            nullptr, //tx_control
            &req->parameters(),
            req->collect_stats(),
            nullptr, // query_cache_policy
            req->has_operation_params() ? &req->operation_params() : nullptr,
            false, // keep session
            false, // use cancelAfter
            req->syntax()
        );

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        SetCost(record.GetConsumedRu());
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return OnGenericQueryResponseError(record, ctx);
        }

        const auto& kqpResponse = record.GetResponse();
        const auto& issueMessage = kqpResponse.GetQueryIssues();

        auto queryResult = TEvExecuteYqlScriptRequest::AllocateResult<TResult>(Request_);

        try {
            NKqp::ConvertKqpQueryResultsToDbResult(kqpResponse, queryResult);
        } catch (const std::exception& ex) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::ExceptionToIssue(ex));
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
        }

        if (kqpResponse.HasQueryStats()) {
            FillQueryStats(*queryResult->mutable_query_stats(), kqpResponse);
        } else if (kqpResponse.HasQueryPlan()) {
            queryResult->mutable_query_stats()->set_query_plan(kqpResponse.GetQueryPlan());
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, *queryResult, ctx);
    }
};

void DoExecuteYqlScript(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TExecuteYqlScriptRPC(p.release()));
}

template<>
IActor* TEvExecuteYqlScriptRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TExecuteYqlScriptRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
