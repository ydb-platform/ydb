#include "service.h"

#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

using CreateQueryRpc = TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>;
using GetQueryStatusRpc = TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>;
using DescribeQueryRpc = TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>;
using GetResultDataRpc = TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>;

template<typename TReq, typename TResp>
class TLocalGrpcContext
    : public NYdbGrpc::IRequestContextBase {
public:
    using TRequest = TReq;
    using TResponse = TResp;

    TLocalGrpcContext(
        TReq&& request, std::shared_ptr<IRequestCtx> baseRequest,
        std::function<void(const TResponse&)> replyCallback)
        : Request_{std::move(request)}
        , Scope_{"yandexcloud:/" + baseRequest->GetDatabaseName().GetOrElse("/")}
        , ReplyCallback_{std::move(replyCallback)}
        , BaseRequest_{std::move(baseRequest)}
        , AuthState_{/*needAuth*/true}
    {}
private:
    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) {
        TResp resp;
        NGRpcService::TCommonResponseFiller<TResp, true>::Fill(resp, IssueManager_.GetIssues(), nullptr, status);
        ReplyCallback_(resp);
    }

public:
    //! Get pointer to the request's message.
    const NProtoBuf::Message* GetRequest() const override {
        return &Request_;
    }

    //! Get mutable pointer to the request's message.
    NProtoBuf::Message* GetRequestMut() override {
        return &Request_;
    }

    //! Get current auth state
    NYdbGrpc::TAuthState& GetAuthState() override {
        return AuthState_;
    }

    //! Send common response (The request shoult be created for protobuf response type)
    //! Implementation can swap protobuf message
    void Reply(NProtoBuf::Message* proto, ui32 status = 0) override {
        Y_UNUSED(status);
        TResp* resp = dynamic_cast<TResp*>(proto);
        Y_ABORT_UNLESS(resp);
        ReplyCallback_(*resp);
    }

    //! Send serialised response (The request shoult be created for bytes response type)
    //! Implementation can swap ByteBuffer

    //! ctrl - controll stream behaviour. Ignored in case of unary call
    void Reply(grpc::ByteBuffer* resp, ui32 status = 0, EStreamCtrl ctrl = EStreamCtrl::CONT) override {
        Y_UNUSED(resp, status, ctrl);
        Y_ABORT("TLocalGrpcContext::Reply for stream is unimplemented");
    }

    //! Send grpc UNAUTHENTICATED status
    void ReplyUnauthenticated(const TString& in) override {
        ReplyError(grpc::UNAUTHENTICATED, in);
    }

    //! Send grpc error
    void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details = "") override {
        NYql::TIssue issue{TStringBuilder() << "grpc code: " << code << ", msg: " << msg << " (" << details << ")"};
        issue.SetCode(code, NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
        RaiseIssue(issue);
        ReplyWithYdbStatus(Ydb::StatusIds::GENERIC_ERROR);
    }

    //! Returns deadline (server epoch related) if peer set it on its side, or Instanse::Max() otherwise
    TInstant Deadline() const override {
        return BaseRequest_->GetDeadline();
    }

    //! Returns available peer metadata keys
    TSet<TStringBuf> GetPeerMetaKeys() const override {
        Y_ABORT("TLocalGrpcContext::GetPeerMetaKeys unimplemented");
        return {};
    }

    //! Returns peer optional metavalue
    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        if (key == "x-ydb-fq-project") {
            return {Scope_};
        }
        auto value = BaseRequest_->GetPeerMetaValues(TString{key});
        if (value) {
            return {std::move(*value)};
        }
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        return BaseRequest_->FindClientCert();
    }

    //! Returns request compression level
    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    //! Returns protobuf arena allocator associated with current request
    //! Lifetime of the arena is lifetime of the context
    google::protobuf::Arena* GetArena() override {
        return &Arena_;
    }

    //! Add trailing metadata in to grpc context
    //! The metadata will be send at the time of rpc finish
    void AddTrailingMetadata(const TString& key, const TString& value) override {
        Y_UNUSED(key, value);
    }

    //! Use validated database name for counters
    void UseDatabase(const TString& database) override {
        Y_UNUSED(database);
    }

    // Streaming part

    void SetNextReplyCallback(TOnNextReply&& cb) override {
        Y_UNUSED(cb);
    }
    void FinishStreamingOk() override {}
    TAsyncFinishResult GetFinishFuture() override { return {}; }
    TString GetPeer() const override { return {}; }
    bool SslServer() const override { return false; }
    bool IsClientLost() const override { return false; }

public:
    const TReq& GetRequestProto() const {
        auto typedReq = dynamic_cast<const TReq*>(GetRequest());
        Y_ABORT_UNLESS(typedReq);
        return *typedReq;
    }
private:
    void RaiseIssue(const NYql::TIssue& issue) {
        IssueManager_.RaiseIssue(issue);
    }

private:
    TReq Request_;
    TString Scope_;
    std::function<void(const TResponse&)> ReplyCallback_;

    std::shared_ptr<IRequestCtx> BaseRequest_;
    NYdbGrpc::TAuthState AuthState_;

    NYql::TIssueManager IssueManager_;
    google::protobuf::Arena Arena_;
};


namespace {

std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest(
    FederatedQuery::CreateQueryRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::CreateQueryResponse&)> replyCallback) {
    auto context = new TLocalGrpcContext<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>{
        std::move(request), std::move(baseRequest), std::move(replyCallback)
    };
    return CreateFederatedQueryCreateQueryRequestOperationCall(context);
};

std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest(
    FederatedQuery::GetQueryStatusRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::GetQueryStatusResponse&)> replyCallback) {
    auto context = new TLocalGrpcContext<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>{
        std::move(request), std::move(baseRequest), std::move(replyCallback)
    };
    return CreateFederatedQueryGetQueryStatusRequestOperationCall(context);
};

std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest(
    FederatedQuery::DescribeQueryRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::DescribeQueryResponse&)> replyCallback) {
    auto context = new TLocalGrpcContext<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>{
        std::move(request), std::move(baseRequest), std::move(replyCallback)
    };
    return CreateFederatedQueryDescribeQueryRequestOperationCall(context);
};

std::unique_ptr<TEvProxyRuntimeEvent> MakeRequest(
    FederatedQuery::GetResultDataRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::GetResultDataResponse&)> replyCallback) {
    auto context = new TLocalGrpcContext<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>{
        std::move(request), std::move(baseRequest), std::move(replyCallback)
    };
    return CreateFederatedQueryGetResultDataRequestOperationCall(context);
};

}

class ExecuteDataQueryRPC : public TRpcOperationRequestActor<
    ExecuteDataQueryRPC, TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>> {
public:
    using TBase = TRpcOperationRequestActor<
        ExecuteDataQueryRPC,
        TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        Become(&ExecuteDataQueryRPC::CreateQueryState);
        CreateQuery(ctx);
    }

    ExecuteDataQueryRPC(IRequestOpCtx* request, TActorId grpcProxyId)
        : TBase{request}
        , GrpcProxyId_{std::move(grpcProxyId)}
    {}

private:
    // overall algorithm: CreateQuery -> WaitForExecution -> GatherResultSetSizes -> GatherResultSets -> SendReply

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

        LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                        "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", creating query");

        MakeLocalCall<CreateQueryRpc, TEvCreateQueryResponse>(std::move(req), ctx);
    }

    void WaitForExecution(const TString& queryId, const TActorContext& ctx) {
        QueryId_ = queryId;
        auto reqEv = CreateStatusRequest();

        Become(&ExecuteDataQueryRPC::WaitForExecutionState);
        ctx.Send(SelfId(), reqEv);
    }

    void GatherResultSetSizes(const TActorContext& ctx) {
        FederatedQuery::DescribeQueryRequest req;
        req.set_query_id(QueryId_);

        Become(&ExecuteDataQueryRPC::GatherResultSetSizesState);
        MakeLocalCall<DescribeQueryRpc, TEvDescribeQueryResponse>(std::move(req), ctx);
    }

    void GatherResultSets(const TActorContext& ctx) {
        Become(&ExecuteDataQueryRPC::GatherResultSetsState);
        ctx.Send(SelfId(), CreateResultSetRequest(0, 0));
    }

    void SendReply(const TActorContext& ctx) {
        Ydb::Table::ExecuteQueryResult result;
        for (const auto& resultSet : ResultSets_) {
            *result.add_result_sets() = resultSet;
        }
        result.mutable_query_meta()->set_id(QueryId_);
        ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, result, ctx);
    }

    // CreateQueryImpl

    STRICT_STFUNC(CreateQueryState,
        HFunc(TEvCreateQueryResponse, HandleCreatedQuery);
    )

    void HandleCreatedQuery(typename TEvCreateQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "CreateQuery", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::CreateQueryResult result;
        resp.operation().result().UnpackTo(&result);

        LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", created query: " << result.query_id());

        WaitForExecution(result.query_id(), ctx);
    }

    // WaitForExecutionImpl

    STRICT_STFUNC(WaitForExecutionState,
        HFunc(TEvGetQueryStatusRequest, HandleStatusRequest);
        HFunc(TEvGetQueryStatusResponse, HandleStatusResponse);
    )

    TEvGetQueryStatusRequest* CreateStatusRequest() {
        FederatedQuery::GetQueryStatusRequest req;
        req.set_query_id(QueryId_);
        return new TEvGetQueryStatusRequest(std::move(req));
    }

    void HandleStatusRequest(typename TEvGetQueryStatusRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall<GetQueryStatusRpc, TEvGetQueryStatusResponse>(std::move(ev->Get()->Message), ctx);
    }

    void HandleStatusResponse(typename TEvGetQueryStatusResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "GetQueryStatus", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::GetQueryStatusResult result;
        resp.operation().result().UnpackTo(&result);

        if (!NFq::IsTerminalStatus(result.status())) {
            LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", still waiting for query: " << QueryId_ <<
                ", current status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            ctx.Schedule(TDuration::Seconds(3), CreateStatusRequest());
            return;
        }

        if (result.status() != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId_ <<
                ", finished with bad status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR,
                TString{TStringBuilder{} << "Created query " << QueryId_ << " finished with non-success status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(result.status())}, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId_ <<
            ", finished query execution");

        GatherResultSetSizes(ctx);
    }

    // GatherResultSetSizesImpl

    STRICT_STFUNC(GatherResultSetSizesState,
        HFunc(TEvDescribeQueryResponse, HandleDescribeResponse);
    )

    void HandleDescribeResponse(typename TEvDescribeQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "DescribeQuery", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::DescribeQueryResult result;
        resp.operation().result().UnpackTo(&result);
        ResultSetSizes_.reserve(result.query().result_set_meta_size());

        for (const auto& meta : result.query().result_set_meta()) {
            ResultSetSizes_.push_back(meta.rows_count());
        }

        GatherResultSets(ctx);
    }

    // GatherResultSetsImpl

    STRICT_STFUNC(GatherResultSetsState,
        HFunc(TEvGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvGetResultDataResponse, HandleResultSetPart);
    )

    TEvGetResultDataRequest* CreateResultSetRequest(i32 index, i64 offset) {
        auto req = new TEvGetResultDataRequest();
        auto& msg = req->Message;

        constexpr i64 RowsLimit = 333;

        msg.set_query_id(QueryId_);
        msg.set_result_set_index(index);
        msg.set_offset(offset);
        msg.set_limit(RowsLimit);
        return req;
    }

    void HandleResultSetRequest(typename TEvGetResultDataRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall<GetResultDataRpc, TEvGetResultDataResponse>(std::move(ev->Get()->Message), ctx);
    }

    void HandleResultSetPart(typename TEvGetResultDataResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "GetResultData", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::GetResultDataResult result;
        resp.operation().result().UnpackTo(&result);

        Y_ABORT_UNLESS(CurrentResultSet_ <= static_cast<i64>(ResultSets_.size()));

        Ydb::ResultSet* resultSet = nullptr;
        if (CurrentResultSet_ == static_cast<i64>(ResultSets_.size())) {
            ResultSets_.push_back(result.result_set());
            resultSet = &ResultSets_.back();
        } else {
            // TODO: validate columns
            resultSet = &ResultSets_.back();
            for (const auto& srcRow : result.result_set().rows()) {
                *resultSet->add_rows() = srcRow;
            }
        }

        if (resultSet->rows_size() < ResultSetSizes_[CurrentResultSet_]) {
            ctx.Send(SelfId(), CreateResultSetRequest(CurrentResultSet_, resultSet->rows_size()));
        } else {
            Y_ABORT_UNLESS(resultSet->rows_size() == ResultSetSizes_[CurrentResultSet_]);
            if (++CurrentResultSet_ < static_cast<i64>(ResultSetSizes_.size())) {
                ctx.Send(SelfId(), CreateResultSetRequest(CurrentResultSet_, 0));
            } else {
                SendReply(ctx);
            }
        }
    }

    // helpers

    template <typename TRpc, typename TEvResponse>
    void MakeLocalCall(typename TRpc::TRequest&& req, const TActorContext& ctx) {
        using TResp = typename TRpc::TResponse;

        auto baseRequest = std::dynamic_pointer_cast<TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>>(Request_);
        auto localRequest = MakeRequest(std::move(req), baseRequest, [as = ctx.ActorSystem(), selfId = SelfId()](const TResp& response) {
                as->Send(selfId, new TEvent<TResp>(response));
            });
        ctx.Send(GrpcProxyId_, localRequest.release());
    }

    // if status is not success, replies error, returns true
    bool HandleFailure(const Ydb::Operations::Operation& operation, std::string_view opName, const TActorContext& ctx) {
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);
        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", failed to " << opName << ", status: " <<
            Ydb::StatusIds::StatusCode_Name(operation.status()) << ", issues: " << issues.ToOneLineString());
        Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, operation.issues(), ctx);
        return true;
    }

    TString QueryId_;
    std::vector<i64> ResultSetSizes_;
    i32 CurrentResultSet_ = 0;
    std::vector<Ydb::ResultSet> ResultSets_;
    TActorId GrpcProxyId_;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetExecuteDataQueryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId = std::move(grpcProxyId)](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ExecuteDataQueryRPC(p.release(), grpcProxyId));
    };
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService