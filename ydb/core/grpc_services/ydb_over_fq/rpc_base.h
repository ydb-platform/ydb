#pragma once

#include "fq_local_grpc_events.h"

#include <library/cpp/retry/retry_policy.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

#define SRC_LOG_T(s, ...) LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_D(s, ...) LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_I(s, ...) LOG_INFO_S(ctx,  NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_W(s, ...) LOG_WARN_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_N(s, ...) LOG_NOTICE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_E(s, ...) LOG_ERROR_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)
#define SRC_LOG_C(s, ...) LOG_CRIT_S(ctx,  NKikimrServices::FQ_INTERNAL_SERVICE, LogCtx(__VA_ARGS__) << s)

namespace NKikimr::NGRpcService::NYdbOverFq {

template<typename TDerived, typename TReq>
class TRpcStreamingBase : public TActorBootstrapped<TDerived> {
public:
    TRpcStreamingBase(IRequestNoOpCtx* request)
        : Request_{request}
    {}

    using TRequest = typename TReq::TRequest;
    using TResponse = typename TReq::TResponse;

    void Reply(Ydb::StatusIds::StatusCode status, TString issueMsg, NKikimrIssues::TIssuesIds::EIssueCode issueCode, const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(issueCode, std::move(issueMsg)));
        Reply(status, issues, ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        if (status == Ydb::StatusIds::SUCCESS) {
            FinishStream(status, ctx);
            return;
        }

        Request_->RaiseIssues(issues);
        TResponse response;
        response.set_status(status);
        NYql::IssuesToMessage(issues, response.mutable_issues());

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&serialized);
        Request_->SendSerializedResult(std::move(serialized), status);
        FinishStream(status, ctx);
    }

    const TRequest* GetProtoRequest() const noexcept {
        return TReq::GetProtoRequest(Request_);
    }

    TRequest* GetProtoRequestMut() noexcept {
        return TReq::GetProtoRequestMut(Request_);
    }

    IRequestNoOpCtx& Request() noexcept { return *Request_; }

private:
    void FinishStream(Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
        Request_->FinishStream(status);
        this->Die(ctx);
    }

protected:
    std::shared_ptr<IRequestNoOpCtx> Request_ = nullptr;
};


template <typename TDerived, typename TReq, typename TResp, bool IsOp = true>
class TRpcBase
    : public std::conditional_t<IsOp,
            TRpcOperationRequestActor<TDerived, TGrpcRequestOperationCall<TReq, TResp>>,
            TRpcStreamingBase<TDerived, TGrpcRequestNoOperationCall<TReq, TResp>>
        >
    , public NLocalGrpc::TCaller {
public:
    using TBase = std::conditional_t<IsOp,
            TRpcOperationRequestActor<TDerived, TGrpcRequestOperationCall<TReq, TResp>>,
            TRpcStreamingBase<TDerived, TGrpcRequestNoOperationCall<TReq, TResp>>>;
    using TRequestCtx = std::conditional_t<IsOp, IRequestOpCtx, IRequestNoOpCtx>;

    TRpcBase(TRequestCtx* request, TActorId grpcProxyId)
        : TBase{request}
        , TCaller{grpcProxyId}
    {}

    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::GetProtoRequest;
    using TBase::SelfId;
    using TBase::Request_;

protected:
    TStringBuilder LogCtx(TStringBuf queryId = "") {
        auto builder = TStringBuilder{} << "YdbOverFq::" << TDerived::RpcName << " actorId: " << SelfId().ToString();
        if (!queryId.empty()) {
            builder << " queryId: " << queryId;
        } else if (!QueryId_.empty()) {
            builder << " queryId: " << QueryId_;
        }
        builder << ' ';
        return builder;
    }

    // create query

    STRICT_STFUNC(CreateQueryState,
        HFunc(TEvFqCreateQueryResponse, HandleResponse<FederatedQuery::CreateQueryRequest>);
    )

    void CreateQuery(const TString& text, FederatedQuery::ExecuteMode executeMode, const TActorContext& ctx) {
        FederatedQuery::CreateQueryRequest req;
        req.set_execute_mode(executeMode);
        auto& queryContent = *req.mutable_content();
        queryContent.set_type(FederatedQuery::QueryContent_QueryType_ANALYTICS);
        queryContent.set_name("Query from YDB SDK");
        queryContent.set_text(text);
        queryContent.set_automatic(true);
        auto& acl = *queryContent.mutable_acl();
        acl.set_visibility(FederatedQuery::Acl_Visibility::Acl_Visibility_SCOPE);

        SRC_LOG_T("creating query");

        Become(&TRpcBase::CreateQueryState);
        MakeLocalCall(std::move(req), ctx);
    }

    // wait for query termination

    STRICT_STFUNC(WaitForTerminationState,
        HFunc(TEvFqGetQueryStatusRequest, HandleStatusRequest);
        HFunc(TEvFqGetQueryStatusResponse, HandleStatusResponse);
    )

    using WaitRetryPolicy = IRetryPolicy<FederatedQuery::QueryMeta::ComputeStatus>;

    static WaitRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return WaitRetryPolicy::GetExponentialBackoffPolicy([](FederatedQuery::QueryMeta::ComputeStatus status) {
            return NFq::IsTerminalStatus(status) ? ERetryErrorClass::NoRetry : ERetryErrorClass::ShortRetry;
        }, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5))->CreateRetryState();
    }

    TEvFqGetQueryStatusRequest* CreateStatusRequest(const TString& queryId) {
        FederatedQuery::GetQueryStatusRequest req;
        req.set_query_id(queryId);
        return new TEvFqGetQueryStatusRequest(std::move(req));
    }

    // will call TDerived::OnQueryTermination(const TString& queryId, FederatedQuery::QueryMeta_ComputeStatus, const TActorContext&)
    void WaitForTermination(const TString& queryId, const TActorContext& ctx) {
        WaitRetryState_ = CreateRetryState();
        QueryId_ = queryId;
        Become(&TRpcBase::WaitForTerminationState);
        ctx.Send(SelfId(), CreateStatusRequest(QueryId_));
    }

    void HandleStatusRequest(typename TEvFqGetQueryStatusRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall(std::move(ev->Get()->Message), ctx);
    }

    void HandleStatusResponse(typename TEvFqGetQueryStatusResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), FederatedQuery::GetQueryStatusResponse::GetDescriptor()->name(), ctx)) {
            return;
        }

        FederatedQuery::GetQueryStatusResult result;
        resp.operation().result().UnpackTo(&result);

        if (!NFq::IsTerminalStatus(result.status())) {
            SRC_LOG_T("still waiting for query: " << QueryId_ <<
                ", current status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            auto delay = WaitRetryState_->GetNextRetryDelay(result.status());
            if (!delay) {
                TBase::Reply(Ydb::StatusIds_StatusCode_TIMEOUT,
                    TStringBuilder{} << "Created query " << QueryId_ << ", couldn't wait for finish, final status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            }
            ctx.Schedule(*delay, CreateStatusRequest(QueryId_));
            return;
        }

        static_cast<TDerived*>(this)->OnQueryTermination(QueryId_, result.status(), ctx);
    }

    // DescribeQuery

    STRICT_STFUNC(DescribeQueryState,
        HFunc(TEvFqDescribeQueryResponse, HandleResponse<FederatedQuery::DescribeQueryRequest>);
    )

    void DescribeQuery(const TString& queryId, const TActorContext& ctx) {
        FederatedQuery::DescribeQueryRequest req;
        req.set_query_id(queryId);

        Become(&TRpcBase::DescribeQueryState);
        MakeLocalCall(std::move(req), ctx);
    }

    // helpers

    // if status is not success, replies error, returns true
    bool HandleFailure(const Ydb::Operations::Operation& operation, std::string_view opName, const TActorContext& ctx) {
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);

        TString errorMsg = TStringBuilder{} << "failed to " << opName << " with status: " << Ydb::StatusIds::StatusCode_Name(operation.status());
        SRC_LOG_I(errorMsg << ", issues: " << issues.ToOneLineString());
        issues.AddIssue(errorMsg);

        TBase::Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, issues, ctx);
        return true;
    }

    template <typename TGrpcReq>
    void HandleResponse(typename NLocalGrpc::TEvent<typename NLocalGrpc::TCall<TGrpcReq>::TResponse>::TPtr& ev, const TActorContext& ctx) {
        using TCall = NLocalGrpc::TCall<TGrpcReq>;
        using TResponse = typename TCall::TResponse;
        using TResult = typename TCall::TResult;

        const TResponse& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), TResponse::GetDescriptor()->name(), ctx)) {
            return;
        }

        TResult result;
        resp.operation().result().UnpackTo(&result);

        static_cast<TDerived*>(this)->Handle(result, ctx);
    }

    template <typename TRequest>
    void MakeLocalCall(TRequest&& req, const TActorContext& ctx) {
        TCaller::MakeLocalCall(std::forward<TRequest>(req), Request_, ctx);
    }

private:
    WaitRetryPolicy::IRetryState::TPtr WaitRetryState_;
    TString QueryId_;
};

} // namespace NKikimr::NGRpcService::NYdbOverFq
