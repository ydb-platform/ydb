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
        SRC_LOG_T(result.query_id(), "created query");

        WaitForTermination(result.query_id(), ctx);
    }

    // WaitForTermination
    void OnQueryTermination(const TString& queryId, FederatedQuery::QueryMeta_ComputeStatus status, const TActorContext& ctx) {
        SRC_LOG_I(queryId, "finished query execution with status " << FederatedQuery::QueryMeta::ComputeStatus_Name(status));

        // Whether query is successful or not, we want to call DescribeQuery
        //   to get either AST and statistics or for issues
        TBase::DescribeQuery(queryId, ctx);
    }

    // DescribeQuery
    void Handle(const FederatedQuery::DescribeQueryResult& result, const TActorContext& ctx) {
        auto status = result.query().meta().status();
        if (status != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            TString errorMsg = TStringBuilder{} << "created query " << result.query().meta().common().id() <<
                " finished with non-success status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(status);
            SRC_LOG_I(result.query().meta().common().id(), "error: " << errorMsg);

            NYql::TIssues issues;
            issues.AddIssue(std::move(errorMsg));
            NYql::IssuesFromMessage(result.query().issue(), issues);
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, issues, ctx);
            return;
        }

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
