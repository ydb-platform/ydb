#include "fq_local_grpc_events.h"
#include "rpc_base.h"

namespace NKikimr::NGRpcService::NYdbOverFq {

class ExplainDataQueryRPC
    : public TRpcBase<ExplainDataQueryRPC, Ydb::Table::ExplainDataQueryRequest, Ydb::Table::ExplainDataQueryResponse> {
public:
    using TBase = TRpcBase<ExplainDataQueryRPC, Ydb::Table::ExplainDataQueryRequest, Ydb::Table::ExplainDataQueryResponse>;
    static constexpr std::string_view RpcName = "ExplainDataQuery";

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        // plan: CreateQuery -> WaitForTermination -> DescribeQuery -> Reply
        CreateQuery(ctx);
    }

    void CreateQuery(const TActorContext& ctx) {
        TBase::CreateQuery(GetProtoRequest()->yql_text(), FederatedQuery::ExecuteMode::EXPLAIN, ctx);
    }

    void Handle(const FederatedQuery::CreateQueryResult& result, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "YdbOverFq::ExplainDataQuery actorId: " << SelfId().ToString() << ", created query: " << result.query_id());

        WaitForTermination(result.query_id(), ctx);
    }

    // WaitForTermination
    void OnQueryTermination(const TString& queryId, FederatedQuery::QueryMeta_ComputeStatus status, const TActorContext& ctx) {
        if (status != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            TString errorMsg = TStringBuilder{} << "created query " << queryId << " finished with non-success status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(status);
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "YdbOverFq::ExplainDataQuery actorId: " << SelfId().ToString() << ", error: " << errorMsg);
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, errorMsg, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "YdbOverFq::ExplainDataQuery actorId: " << SelfId().ToString() << ", queryId: " << queryId <<
            ", finished query execution");

        TBase::DescribeQuery(queryId, ctx);
    }

    // DescribeQuery
    void Handle(const FederatedQuery::DescribeQueryResult& result, const TActorContext& ctx) {
        Ydb::Table::ExplainQueryResult response;
        response.set_query_ast(result.query().ast().data());
        response.set_query_plan(result.query().plan().json());
        response.set_query_full_diagnostics(result.query().statistics().json());
        ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, response, ctx);
    }
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExplainDataQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ExplainDataQueryRPC(p.release(), grpcProxyId));
    };
}
}
