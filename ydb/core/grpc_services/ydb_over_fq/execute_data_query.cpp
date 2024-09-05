#include "fq_local_grpc_events.h"
#include "rpc_base.h"
#include "service.h"

#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

template <typename TDerived, typename TReq, typename TResp, bool IsStreaming>
class ExecuteDataQueryRPCBase
    : public TRpcBase<
        TDerived, TReq, TResp, !IsStreaming> {
public:
    using TBase = TRpcBase<
        TDerived, TReq, TResp, !IsStreaming>;

    using TBase::TBase;
    using TBase::SelfId;
    using typename TBase::TLogCtx;

    void Bootstrap(const TActorContext& ctx) {
        CreateQuery(ctx);
    }

    // overall algorithm: CreateQuery -> WaitForExecution -> GatherResultSetSizes -> GatherResultSets -> reply

    // CreateQueryImpl

    void CreateQuery(const TActorContext& ctx) {
        if (!TBase::GetProtoRequest()->query().has_yql_text()) {
            SRC_LOG_I("got request with id instead of text");
            TBase::Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder {} << "query id in " << TDerived::RpcName << " is not supported",
                NKikimrIssues::TIssuesIds::EIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                ctx);
            return;
        }
        const auto& text = TBase::GetProtoRequest()->query().yql_text();

        TBase::CreateQuery(text, FederatedQuery::ExecuteMode::RUN, ctx);
    }

    void Handle(const FederatedQuery::CreateQueryResult& result, const TActorContext& ctx) {
        SRC_LOG_T("created query: " << result.query_id());

        TBase::WaitForTermination(result.query_id(), ctx);
    }

    // WaitForExecutionImpl

    void OnQueryTermination(const TString& queryId, FederatedQuery::QueryMeta_ComputeStatus status, const TActorContext& ctx) {
        SRC_LOG_I(queryId, "finished query execution with status " << FederatedQuery::QueryMeta::ComputeStatus_Name(status));

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
            SRC_LOG_I("error: " << errorMsg);

            NYql::TIssues issues;
            issues.AddIssue(std::move(errorMsg));
            NYql::IssuesFromMessage(result.query().issue(), issues);
            TBase::Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, issues, ctx);
            return;
        }

        ResultSetSizes_.reserve(result.query().result_set_meta_size());

        for (const auto& meta : result.query().result_set_meta()) {
            ResultSetSizes_.push_back(meta.rows_count());
        }

        static_cast<TDerived*>(this)->HandleResultSets(result.query().meta().common().id(), ctx);
    }

    // HandleResultSetsImpl

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
        TBase::MakeLocalCall(std::move(ev->Get()->Message), ctx);
    }

protected:
    std::vector<i64> ResultSetSizes_;
};

class ExecuteDataQueryRPC
    : public ExecuteDataQueryRPCBase<ExecuteDataQueryRPC, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse, false> {
public:
    using TBase = ExecuteDataQueryRPCBase<
        ExecuteDataQueryRPC, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse, false>;
    static constexpr std::string_view RpcName = "ExecuteDataQuery";

    using TBase::TBase;
    using TBase::Handle;
    // GatherResultSetsImpl

    STRICT_STFUNC(GatherResultSetsState,
        HFunc(TEvFqGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvFqGetResultDataResponse, TBase::template HandleResponse<FederatedQuery::GetResultDataRequest>);
    )

    void HandleResultSets(const TString& queryId, const TActorContext& ctx) {
        if (ResultSetSizes_.empty()) {
            SendReply(ctx);
            return;
        }

        Become(&ExecuteDataQueryRPC::GatherResultSetsState);
        QueryId_ = queryId;
        MakeLocalCall(CreateResultSetRequest(queryId, 0, 0), ctx);
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
        TBase::ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, result, ctx);
    }
private:
    TString QueryId_;
    std::vector<Ydb::ResultSet> ResultSets_;
    i32 CurrentResultSet_ = 0;
};

class StreamExecuteScanQueryRPC
    : public ExecuteDataQueryRPCBase<StreamExecuteScanQueryRPC, Ydb::Table::ExecuteScanQueryRequest, Ydb::Table::ExecuteScanQueryPartialResponse, true> {
public:
    using TBase = ExecuteDataQueryRPCBase<
        StreamExecuteScanQueryRPC, Ydb::Table::ExecuteScanQueryRequest, Ydb::Table::ExecuteScanQueryPartialResponse, true>;
    static constexpr std::string_view RpcName = "StreamExecuteScanQuery";

    using TBase::TBase;
    using TBase::Handle;
    using typename TBase::TLogCtx;

    // StreamResultSetsImpl

    STRICT_STFUNC(StreamResultSetsState,
        HFunc(TEvFqGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvFqGetResultDataResponse, TBase::template HandleResponse<FederatedQuery::GetResultDataRequest>);
    )

    void HandleResultSets(const TString& queryId, const TActorContext& ctx) {
        if (ResultSetSizes_.size() > 1) {
            NYql::TIssues issues;
            issues.AddIssue("Scan query should have a single result set.");
            issues.back().SetCode(NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, NYql::TSeverityIds::S_ERROR);
            Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
            SRC_LOG_I(queryId, "failed: got " << ResultSetSizes_.size() << " result sets");
            return;
        }

        Become(&StreamExecuteScanQueryRPC::StreamResultSetsState);
        QueryId_ = queryId;
        MakeLocalCall(CreateResultSetRequest(queryId, 0, 0), ctx);
    }

    void Handle(const FederatedQuery::GetResultDataResult& result, const TActorContext& ctx) {
        Ydb::Table::ExecuteScanQueryPartialResponse response;
        response.set_status(Ydb::StatusIds::SUCCESS);
        *response.mutable_result()->mutable_result_set() = result.result_set();

        TString respSerialized;
        if (!response.SerializeToString(&respSerialized)) {
            NYql::TIssues issues;
            auto issueMsg = TStringBuilder{} << "failed to serialize ResultSet[" << CurrentResultSet_ << "][" <<
                SentRowsInCurrRS_ << ":" << (SentRowsInCurrRS_ + result.result_set().rows_size()) << "]";
            issues.AddIssue(issueMsg);
            issues.back().SetCode(NYql::TIssuesIds::CORE_EXEC, NYql::TSeverityIds::S_ERROR);
            SRC_LOG_I("error: " << issueMsg);
            Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
            return;
        }

        Request_->SendSerializedResult(std::move(respSerialized), Ydb::StatusIds::SUCCESS);
        SentRowsInCurrRS_ += result.result_set().rows_size();

        if (SentRowsInCurrRS_ < ResultSetSizes_[CurrentResultSet_]) {
            SRC_LOG_T("RS[" << CurrentResultSet_ << "][" << (SentRowsInCurrRS_ - result.result_set().rows_size()) <<
                ":" << SentRowsInCurrRS_ << "], still got " << (ResultSetSizes_[CurrentResultSet_] - CurrentResultSet_)
            );
            MakeLocalCall(CreateResultSetRequest(QueryId_, CurrentResultSet_, SentRowsInCurrRS_), ctx);
        } else {
            SRC_LOG_T("RS[" << CurrentResultSet_ << "][" <<
                (SentRowsInCurrRS_ - result.result_set().rows_size()) << ":" << SentRowsInCurrRS_ << "], fully sent"
            );

            Y_ABORT_UNLESS(SentRowsInCurrRS_ == ResultSetSizes_[CurrentResultSet_]);
            ++CurrentResultSet_;
            SentRowsInCurrRS_ = 0;
            if (CurrentResultSet_ < static_cast<i64>(ResultSetSizes_.size())) {
                MakeLocalCall(CreateResultSetRequest(QueryId_, CurrentResultSet_, SentRowsInCurrRS_), ctx);
            } else {
                SRC_LOG_T("finish");
                Request_->FinishStream(Ydb::StatusIds::SUCCESS);
                this->Die(ctx);
            }
        }
    }

private:
    TString QueryId_;
    i32 CurrentResultSet_ = 0;
    i64 SentRowsInCurrRS_ = 0;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ExecuteDataQueryRPC(p.release(), grpcProxyId));
    };
}

std::function<void(std::unique_ptr<IRequestNoOpCtx>, const IFacilityProvider&)> GetStreamExecuteScanQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new StreamExecuteScanQueryRPC(p.release(), grpcProxyId));
    };
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService