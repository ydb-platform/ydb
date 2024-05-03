#pragma once

#include "defs.h"
#include "grpc_request_proxy.h"
#include "cancelation/cancelation.h"
#include "cancelation/cancelation_event.h"
#include "rpc_common/rpc_common.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/wilson.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/core/actorlib_impl/long_timer.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NGRpcService {

template <typename TDerived, typename TRequest, bool IsOperation>
class TRpcRequestWithOperationParamsActor : public TActorBootstrapped<TDerived> {
private:
    typedef TActorBootstrapped<TDerived> TBase;
    typedef typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type TRequestBase;

    template<typename TIn, typename TOut>
    void Fill(const TIn* in, TOut* out) {
        auto& operationParams = in->operation_params();
        out->OperationTimeout_ = GetDuration(operationParams.operation_timeout());
        out->CancelAfter_ = GetDuration(operationParams.cancel_after());
        out->ReportCostInfo_ = operationParams.report_cost_info() == Ydb::FeatureFlag::ENABLED;
    }

    template<typename TOut>
    void Fill(const NProtoBuf::Message*, TOut*) {
    }

public:
    enum EWakeupTag {
        WakeupTagTimeout = 10,
        WakeupTagCancel = 11,
        WakeupTagGetConfig = 21,
        WakeupTagClientLost = 22,
    };

public:
    TRpcRequestWithOperationParamsActor(TRequestBase* request)
        : Request_(request)
    {
        Fill(GetProtoRequest(), this);
        //auto& operationParams = GetProtoRequest()->operation_params();
        //OperationTimeout_ = GetDuration(operationParams.operation_timeout());
        //CancelAfter_ = GetDuration(operationParams.cancel_after());
        //ReportCostInfo_ = operationParams.report_cost_info() == Ydb::FeatureFlag::ENABLED;
    }

    const typename TRequest::TRequest* GetProtoRequest() const {
        return TRequest::GetProtoRequest(Request_);
    }

    typename TRequest::TRequest* GetProtoRequestMut() {
        return TRequest::GetProtoRequestMut(Request_);
    }

    Ydb::Operations::OperationParams::OperationMode GetOperationMode() const {
        return GetProtoRequest()->operation_params().operation_mode();
    }

    void Bootstrap(const TActorContext &ctx) {
        HasCancel_ = static_cast<TDerived*>(this)->HasCancelOperation();

        if (OperationTimeout_) {
            OperationTimeoutTimer = CreateLongTimer(ctx, OperationTimeout_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagTimeout)),
                AppData(ctx)->UserPoolId);
        }

        if (HasCancel_ && CancelAfter_) {
            CancelAfterTimer = CreateLongTimer(ctx, CancelAfter_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagCancel)),
                AppData(ctx)->UserPoolId);
        }

        auto selfId = ctx.SelfID;
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto clientLostCb = [selfId, actorSystem]() {
            actorSystem->Send(selfId, new TRpcServices::TEvForgetOperation());
        };

        Request_->SetFinishAction(std::move(clientLostCb));
    }

    bool HasCancelOperation() {
        return false;
    }

    TRequestBase& Request() const {
        return *Request_;
    }

protected:
    TDuration GetOperationTimeout() {
        return OperationTimeout_;
    }

    TDuration GetCancelAfter() {
        return CancelAfter_;
    }

    void DestroyTimers() {
        auto& ctx = TlsActivationContext->AsActorContext();
        if (OperationTimeoutTimer) {
            ctx.Send(OperationTimeoutTimer, new TEvents::TEvPoisonPill);
        }
        if (CancelAfterTimer) {
            ctx.Send(CancelAfterTimer, new TEvents::TEvPoisonPill);
        }
    }

    void PassAway() override {
        DestroyTimers();
        TBase::PassAway();
    }

    TRequest* RequestPtr() {
        return static_cast<TRequest*>(Request_.get());
    }

protected:
    std::shared_ptr<TRequestBase> Request_;

    TActorId OperationTimeoutTimer;
    TActorId CancelAfterTimer;
    TDuration OperationTimeout_;
    TDuration CancelAfter_;
    bool HasCancel_ = false;
    bool ReportCostInfo_ = false;
};

template <typename TDerived, typename TRequest>
class TRpcOperationRequestActor : public TRpcRequestWithOperationParamsActor<TDerived, TRequest, true> {
private:
    typedef TRpcRequestWithOperationParamsActor<TDerived, TRequest, true> TBase;

public:

    TRpcOperationRequestActor(IRequestOpCtx* request)
        : TBase(request)
        , Span_(TWilsonGrpc::RequestActor, request->GetWilsonTraceId(),
                "RequestProxy.RpcOperationRequestActor", NWilson::EFlags::AUTO_END)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DEFERRABLE_RPC;
    }

    void OnCancelOperation(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

    void OnForgetOperation(const TActorContext& ctx) {
        // No client is waiting for the reply, but we have to issue fake reply
        // anyway before dying to make Grpc happy.
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Closing Grpc request, client should not see this message."));
        Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void OnOperationTimeout(const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Operation timeout."));
        Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);
    }

protected:
    void StateFuncBase(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TRpcServices::TEvForgetOperation, HandleForget);
            hFunc(TEvSubscribeGrpcCancel, HandleSubscribeiGrpcCancel);
            default: {
                NYql::TIssues issues;
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unexpected event received in TRpcOperationRequestActor::StateWork: "
                        << ev->GetTypeRewrite()));
                return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, TActivationContext::AsActorContext());
            }
        }
    }

protected:
    using TBase::Request_;

    void Reply(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message, const TActorContext& ctx)
    {
        NYql::TIssues issues;
        IssuesFromMessage(message, issues);
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message, NKikimrIssues::TIssuesIds::EIssueCode issueCode, const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(issueCode, message));
        Reply(status, issues, ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
        const TResult& result,
        const TActorContext& ctx)
    {
        Request_->SendResult(result, status, message);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
                         const TResult& result,
                         const TActorContext& ctx) {
        Request_->SendResult(result, status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void ReplyOperation(Ydb::Operations::Operation& operation)
    {
        Request_->SendOperation(operation);
        NWilson::EndSpanWithStatus(Span_, operation.status());
        this->PassAway();
    }

    void SetCost(ui64 ru) {
        Request_->SetRuHeader(ru);
        if (TBase::ReportCostInfo_) {
            Request_->SetCostInfo(ru);
        }
    }

protected:
    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
            case TBase::WakeupTagTimeout:
                static_cast<TDerived*>(this)->OnOperationTimeout(ctx);
                break;
            case TBase::WakeupTagCancel:
                static_cast<TDerived*>(this)->OnCancelOperation(ctx);
                break;
            default:
                break;
        }
    }

    void HandleForget(TRpcServices::TEvForgetOperation::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        static_cast<TDerived*>(this)->OnForgetOperation(ctx);
    }
private:
    void HandleSubscribeiGrpcCancel(TEvSubscribeGrpcCancel::TPtr& ev) {
        auto as = TActivationContext::ActorSystem();
        PassSubscription(ev->Get(), Request_.get(), as);
    }

protected:
    NWilson::TSpan Span_;
};

} // namespace NGRpcService
} // namespace NKikimr
