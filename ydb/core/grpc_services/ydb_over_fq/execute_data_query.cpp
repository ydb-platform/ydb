#include "fq_local_grpc_events.h"
#include "rpc_base.h"
#include "service.h"

#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

class ExecuteDataQueryRPC
    : public TRpcBase<
        ExecuteDataQueryRPC, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse> {
public:
    using TBase = TRpcBase<
        ExecuteDataQueryRPC, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>;
    static constexpr std::string_view RpcName = "ExecuteDataQuery";

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        CreateQuery(ctx);
    }

    // overall algorithm: CreateQuery -> WaitForExecution -> GatherResultSetSizes -> GatherResultSets -> reply

    // CreateQueryImpl

    void CreateQuery(const TActorContext& ctx) {
        if (!GetProtoRequest()->query().has_yql_text()) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                            "YdbOverFq::ExecuteDataQuery actorId: " << SelfId().ToString() << ", got request with id instead of text");
            Reply(
                Ydb::StatusIds::BAD_REQUEST,
                "query id in ExecuteDataQuery is not supported",
                NKikimrIssues::TIssuesIds::EIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                ctx);
            return;
        }
        const auto& text = GetProtoRequest()->query().yql_text();

        TBase::CreateQuery(text, FederatedQuery::ExecuteMode::RUN, ctx);
    }

    void Handle(const FederatedQuery::CreateQueryResult& result, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "YdbOverFq::ExecuteDataQuery actorId: " << SelfId().ToString() << ", created query: " << result.query_id());

        WaitForTermination(result.query_id(), ctx);
    }

    // WaitForExecutionImpl

    void OnQueryTermination(const TString& queryId, FederatedQuery::QueryMeta_ComputeStatus status, const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "YdbOverFq::ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << queryId <<
            ", finished query execution with status " << FederatedQuery::QueryMeta::ComputeStatus_Name(status));

        // Whether query is successful or not, we want to call DescribeQuery
        //   to get either ResultSet size or issues
        TBase::DescribeQuery(queryId, ctx);
    }

    // GatherResultSetSizesImpl

    void Handle(const FederatedQuery::DescribeQueryResult& result, const TActorContext& ctx) {
        auto status = result.query().meta().status();
        if (status != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            TString errorMsg = TStringBuilder{} << "created query " << result.query().meta().common().id() <<
                " finished with non-success status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(status);
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "YdbOverFq::ExecuteDataQuery actorId: " << SelfId().ToString() << ", error: " << errorMsg);

            NYql::TIssues issues;
            issues.AddIssue(std::move(errorMsg));
            NYql::IssuesFromMessage(result.query().issue(), issues);
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, issues, ctx);
            return;
        }

        ResultSetSizes_.reserve(result.query().result_set_meta_size());

        for (const auto& meta : result.query().result_set_meta()) {
            ResultSetSizes_.push_back(meta.rows_count());
        }

        GatherResultSets(result.query().meta().common().id(), ctx);
    }

    // GatherResultSetsImpl

    STRICT_STFUNC(GatherResultSetsState,
        HFunc(TEvFqGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvFqGetResultDataResponse, TBase::HandleResponse<FederatedQuery::GetResultDataRequest>);
    )

    void GatherResultSets(const TString& queryId, const TActorContext& ctx) {
        Become(&ExecuteDataQueryRPC::GatherResultSetsState);
        QueryId_ = queryId;
        MakeLocalCall(CreateResultSetRequest(queryId, 0, 0), ctx);
    }

    FederatedQuery::GetResultDataRequest CreateResultSetRequest(const TString& queryId, i32 index, i64 offset) {
        FederatedQuery::GetResultDataRequest msg;

        constexpr i64 RowsLimit = 1000;

        msg.set_query_id(queryId);
        msg.set_result_set_index(index);
        msg.set_offset(offset);
        msg.set_limit(RowsLimit);
        return msg;
    }

    void HandleResultSetRequest(typename TEvFqGetResultDataRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall(std::move(ev->Get()->Message), ctx);
    }

    void Handle(const FederatedQuery::GetResultDataResult& result, const TActorContext& ctx) {
        Y_ABORT_UNLESS(CurrentResultSet_ <= static_cast<i64>(ResultSets_.size()));

        Ydb::ResultSet* resultSet = nullptr;
        if (CurrentResultSet_ == static_cast<i64>(ResultSets_.size())) {
            ResultSets_.push_back(result.result_set());
            resultSet = &ResultSets_.back();
        } else {
            resultSet = &ResultSets_.back();
            for (const auto& srcRow : result.result_set().rows()) {
                *resultSet->add_rows() = srcRow;
            }
        }

        if (resultSet->rows_size() < ResultSetSizes_[CurrentResultSet_]) {
            MakeLocalCall(CreateResultSetRequest(QueryId_, CurrentResultSet_, resultSet->rows_size()), ctx);
        } else {
            Y_ABORT_UNLESS(resultSet->rows_size() == ResultSetSizes_[CurrentResultSet_]);
            if (++CurrentResultSet_ < static_cast<i64>(ResultSetSizes_.size())) {
                MakeLocalCall(CreateResultSetRequest(QueryId_, CurrentResultSet_, 0), ctx);
            } else {
                SendReply(ctx);
            }
        }
    }

    // reply

    void SendReply(const TActorContext& ctx) {
        Ydb::Table::ExecuteQueryResult result;
        for (const auto& resultSet : ResultSets_) {
            *result.add_result_sets() = resultSet;
        }
        result.mutable_query_meta()->set_id(QueryId_);
        ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, result, ctx);
    }

private:
    TString QueryId_;
    std::vector<i64> ResultSetSizes_;
    i32 CurrentResultSet_ = 0;
    std::vector<Ydb::ResultSet> ResultSets_;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ExecuteDataQueryRPC(p.release(), grpcProxyId));
    };
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService