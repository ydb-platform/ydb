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
        Become(&ExecuteDataQueryRPC::CreateQueryState);
        CreateQuery(ctx);
    }

    // overall algorithm: CreateQuery -> WaitForExecution -> GatherResultSetSizes -> GatherResultSets -> reply

    // CreateQueryImpl

    STRICT_STFUNC(CreateQueryState,
        HFunc(TEvFqCreateQueryResponse, TBase::HandleResponse<FederatedQuery::CreateQueryRequest>);
    )

    void CreateQuery(const TActorContext& ctx) {
        if (!GetProtoRequest()->query().has_yql_text()) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", got request with id instead of text");
            Reply(
                Ydb::StatusIds::BAD_REQUEST,
                "query id in ExecuteDataQuery is not supported",
                NKikimrIssues::TIssuesIds::EIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                ctx);
            return;
        }
        const auto& text = GetProtoRequest()->query().yql_text();

        FederatedQuery::CreateQueryRequest req;
        req.set_execute_mode(FederatedQuery::ExecuteMode::RUN);
        auto& queryContent = *req.mutable_content();
        queryContent.set_type(FederatedQuery::QueryContent_QueryType_ANALYTICS);
        queryContent.set_name("Generated query from within");
        queryContent.mutable_text()->assign(text);
        queryContent.set_automatic(true);
        auto& acl = *queryContent.mutable_acl();
        acl.set_visibility(FederatedQuery::Acl_Visibility::Acl_Visibility_SCOPE);

        LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                        "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", creating query");

        MakeLocalCall(std::move(req), ctx);
    }

    void Handle(const FederatedQuery::CreateQueryResult& result, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", created query: " << result.query_id());

        WaitForExecution(result.query_id(), ctx);
    }

    // WaitForExecutionImpl

    STRICT_STFUNC(WaitForExecutionState,
        HFunc(TEvFqGetQueryStatusRequest, HandleStatusRequest);
        HFunc(TEvFqGetQueryStatusResponse, TBase::HandleResponse<FederatedQuery::GetQueryStatusRequest>);
    )

    void WaitForExecution(const TString& queryId, const TActorContext& ctx) {
        QueryId_ = queryId;
        auto reqEv = CreateStatusRequest();

        Become(&ExecuteDataQueryRPC::WaitForExecutionState);
        ctx.Send(SelfId(), reqEv);
    }

    using WaitRetryPolicy = IRetryPolicy<FederatedQuery::QueryMeta::ComputeStatus>;

    static WaitRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return WaitRetryPolicy::GetExponentialBackoffPolicy([](FederatedQuery::QueryMeta::ComputeStatus status) {
            return NFq::IsTerminalStatus(status) ? ERetryErrorClass::NoRetry : ERetryErrorClass::ShortRetry;
        }, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5))->CreateRetryState();
    }

    TEvFqGetQueryStatusRequest* CreateStatusRequest() {
        FederatedQuery::GetQueryStatusRequest req;
        req.set_query_id(QueryId_);
        return new TEvFqGetQueryStatusRequest(std::move(req));
    }

    void HandleStatusRequest(typename TEvFqGetQueryStatusRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall(std::move(ev->Get()->Message), ctx);
    }

    void Handle(const FederatedQuery::GetQueryStatusResult& result, const TActorContext& ctx) {
        if (!NFq::IsTerminalStatus(result.status())) {
            LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", still waiting for query: " << QueryId_ <<
                ", current status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            auto delay = WaitRetryState_->GetNextRetryDelay(result.status());
            if (!delay) [[unlikely]] {
                Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR,
                    TStringBuilder{} << "Created query " << QueryId_ << ", couldn't wait for finish, final status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            }
            ctx.Schedule(*delay, CreateStatusRequest());
            return;
        }

        if (result.status() != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId_ <<
                ", finished with bad status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR,
                TStringBuilder{} << "Created query " << QueryId_ << " finished with non-success status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId_ <<
            ", finished query execution");

        GatherResultSetSizes(ctx);
    }

    // GatherResultSetSizesImpl

    STRICT_STFUNC(GatherResultSetSizesState,
        HFunc(TEvFqDescribeQueryResponse, TBase::HandleResponse<FederatedQuery::DescribeQueryRequest>);
    )

    void GatherResultSetSizes(const TActorContext& ctx) {
        FederatedQuery::DescribeQueryRequest req;
        req.set_query_id(QueryId_);

        Become(&ExecuteDataQueryRPC::GatherResultSetSizesState);
        MakeLocalCall(std::move(req), ctx);
    }

    void Handle(const FederatedQuery::DescribeQueryResult& result, const TActorContext& ctx) {
        ResultSetSizes_.reserve(result.query().result_set_meta_size());

        for (const auto& meta : result.query().result_set_meta()) {
            ResultSetSizes_.push_back(meta.rows_count());
        }

        GatherResultSets(ctx);
    }

    // GatherResultSetsImpl

    STRICT_STFUNC(GatherResultSetsState,
        HFunc(TEvFqGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvFqGetResultDataResponse, TBase::HandleResponse<FederatedQuery::GetResultDataRequest>);
    )

    void GatherResultSets(const TActorContext& ctx) {
        Become(&ExecuteDataQueryRPC::GatherResultSetsState);
        MakeLocalCall(CreateResultSetRequest(0, 0), ctx);
    }

    FederatedQuery::GetResultDataRequest CreateResultSetRequest(i32 index, i64 offset) {
        FederatedQuery::GetResultDataRequest msg;

        constexpr i64 RowsLimit = 1000;

        msg.set_query_id(QueryId_);
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
            MakeLocalCall(CreateResultSetRequest(CurrentResultSet_, resultSet->rows_size()), ctx);
        } else {
            Y_ABORT_UNLESS(resultSet->rows_size() == ResultSetSizes_[CurrentResultSet_]);
            if (++CurrentResultSet_ < static_cast<i64>(ResultSetSizes_.size())) {
                MakeLocalCall(CreateResultSetRequest(CurrentResultSet_, 0), ctx);
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
    WaitRetryPolicy::IRetryState::TPtr WaitRetryState_ = CreateRetryState();
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId = std::move(grpcProxyId)](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ExecuteDataQueryRPC(p.release(), grpcProxyId));
    };
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService