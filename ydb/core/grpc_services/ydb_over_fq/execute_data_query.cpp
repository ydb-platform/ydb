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
class TLocalGrpcRequest
    : public NRpcService::TLocalRpcCtx<TGrpcFqRequestOperationCall<TReq, TResp>, std::function<void(TResp)>>
    , public TEvProxyRuntimeEvent
    , public TFqPermissionsBase<TReq> {
public:
    using TRequest = TReq;
    using TResponse = TResp;
    using TPermissions = TFqPermissionsBase<TReq>;
    using TRpc = TGrpcFqRequestOperationCall<TReq, TResp>;
    using TBase = NRpcService::TLocalRpcCtx<TRpc, std::function<void(TResp)>>;

    TLocalGrpcRequest(
        TReq&& request, std::shared_ptr<IRequestCtx> baseRequest,
        std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> passCallback,
        NFederatedQuery::TPermissionsFunc<TReq> permissions,
        std::function<void(const TResp&)> replyCallback)
        : TBase{
            std::move(request), std::move(replyCallback), baseRequest->GetDatabaseName().GetOrElse(""),
            /*serialized token*/baseRequest->GetSerializedToken().Empty() ? Nothing() : TMaybe<TString>(baseRequest->GetSerializedToken()),
            /*requestType*/ Nothing(), /*internalCall*/ true}
        , TEvProxyRuntimeEvent{}
        , TPermissions{std::move(permissions)}
        , Scope_{"yandexcloud:/" + baseRequest->GetDatabaseName().GetOrElse("/")}
        , PassCallback_{std::move(passCallback)}
        , BaseRequest_{std::move(baseRequest)}
        , AuthState_{/*needAuth*/true}
    {}
private:
    void ReplyUnavaliable() override {
        TBase::ReplyWithRpcStatus(grpc::StatusCode::UNAVAILABLE);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TBase::ReplyWithYdbStatus(status);
    }

public:
    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        if (key == "x-ydb-fq-project") {
            return Scope_;
        }
        return BaseRequest_->GetPeerMetaValues(key);
    }

    // auth
    const TMaybe<TString> GetYdbToken() const override {
        return GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER);
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        AuthState_.State = state;
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        TBase::SetInternalToken(token);
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState_;
    }

    void ReplyUnauthenticated(const TString& msg = "") override {
        TBase::ReplyWithRpcStatus(grpc::StatusCode::UNAUTHENTICATED, msg);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        TBase::RaiseIssue(issue);
    }
    void RaiseIssues(const NYql::TIssues& issues) override {
        TBase::RaiseIssues(issues);
    }

    //tracing
    void StartTracing(NWilson::TSpan&& span) override {
        Span_ = std::move(span);
    }
    void LegacyFinishSpan() override {
        Span_.End();
    }

    // validation
    bool Validate(TString&) override {
        return true;
    }

    // counters
    void SetCounters(IGRpcProxyCounters::TPtr counters) override {
        Counters_ = counters;
    }
    IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters_;
    }
    void UseDatabase(const TString& database) override {
        Counters_->UseDatabase(database);
    }

    // rate limiting

    // This method allows to set hook for unary call.
    // The hook will be called at the reply time
    // TRespHookCtx::Ptr will be passed in to the hook, it is allow
    // to store the ctx somwhere to delay reply and then call Pass() to send response.
    void SetRespHook(TRespHook&& hook) override {
        RespHook_ = std::move(hook);
    }
    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath_ = std::move(path);
    }
    TRateLimiterMode GetRlMode() const override {
        // TODO: is ok?
        return TRateLimiterMode::Off;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface* iface) override {
        auto entries = TPermissions::FillSids(Scope_, GetRequestProto());
        if (entries.empty()) {
            return false;
        }
        iface->SetEntries(entries);
        return true;
    }

    // Pass request for next processing
    void Pass(const IFacilityProvider& facility) override {
        Span_.End();

        try {
            PassCallback_(std::unique_ptr<IRequestOpCtx>(this), facility);
        } catch (const std::exception& ex) {
            this->RaiseIssue(NYql::TIssue{TStringBuilder() << "exception in local grpc: " << ex.what()});
            this->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    // audit
    void SetAuditLogHook(TAuditLogHook&&) override {
        Y_ABORT("unimplemented for TLocalGrpcRequest");
    }
    void SetDiskQuotaExceeded(bool) override {
        // unimplemented
    }

    const TReq& GetRequestProto() const {
        auto typedReq = dynamic_cast<const TReq*>(TBase::GetRequest());
        Y_ABORT_UNLESS(typedReq);
        return *typedReq;
    }

private:
    TString Scope_;
    std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> PassCallback_;

    std::shared_ptr<IRequestCtx> BaseRequest_;
    NYdbGrpc::TAuthState AuthState_;
    NWilson::TSpan Span_;
    IGRpcProxyCounters::TPtr Counters_;
    // rate limiting
    TRespHook RespHook_;
    TMaybe<NRpcService::TRlPath> RlPath_;
};

namespace {

TLocalGrpcRequest<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>* MakeRequest(
    FederatedQuery::CreateQueryRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::CreateQueryResponse&)> replyCallback) {
    return new TLocalGrpcRequest<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>{
        std::move(request), std::move(baseRequest), &DoFederatedQueryCreateQueryRequest, GetFederatedQueryCreateQueryPermissions(), std::move(replyCallback)
    };
};

TLocalGrpcRequest<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>* MakeRequest(
    FederatedQuery::GetQueryStatusRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::GetQueryStatusResponse&)> replyCallback) {
    return new TLocalGrpcRequest<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>{
        std::move(request), std::move(baseRequest), &DoFederatedQueryGetQueryStatusRequest, GetFederatedQueryGetQueryStatusPermissions(), std::move(replyCallback)
    };
};

TLocalGrpcRequest<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>* MakeRequest(
    FederatedQuery::DescribeQueryRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::DescribeQueryResponse&)> replyCallback) {
    return new TLocalGrpcRequest<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>{
        std::move(request), std::move(baseRequest), &DoFederatedQueryDescribeQueryRequest, GetFederatedQueryDescribeQueryPermissions(), std::move(replyCallback)
    };
};

TLocalGrpcRequest<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>* MakeRequest(
    FederatedQuery::GetResultDataRequest&& request,
    std::shared_ptr<IRequestCtx> baseRequest,
    std::function<void(const FederatedQuery::GetResultDataResponse&)> replyCallback) {
    return new TLocalGrpcRequest<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>{
        std::move(request), std::move(baseRequest), &DoFederatedQueryGetResultDataRequest, GetFederatedQueryGetResultDataPermissions(), std::move(replyCallback)
    };
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

        Y_ABORT_UNLESS(CurrentResultSet_ >= static_cast<i64>(ResultSets_.size()));

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
        ctx.Send(GrpcProxyId_, localRequest);
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