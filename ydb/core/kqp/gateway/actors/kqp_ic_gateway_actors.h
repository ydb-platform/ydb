#pragma once

#include <ydb/core/kqp/tracing/kqp_query_tracing.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <yql/essentials/providers/common/gateway/yql_provider_gateway.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


namespace NKikimr::NKqp {

template<typename TDerived, typename TRequest, typename TResponse, typename TResult>
class TRequestHandlerBase: public TActorBootstrapped<TDerived> {
public:
    using TCallbackFunc = std::function<void(NThreading::TPromise<TResult>, TResponse&&)>;
    using TBase = TRequestHandlerBase<TDerived, TRequest, TResponse, TResult>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_REQUEST_HANDLER;
    }

    TRequestHandlerBase(TRequest* request, NThreading::TPromise<TResult> promise, TCallbackFunc callback)
        : Request(request)
        , Promise(promise)
        , Callback(callback) {}

    void HandleError(const TString &error, const TActorContext &ctx) {
        Promise.SetValue(NYql::NCommon::ResultFromError<TResult>(error));
        this->Die(ctx);
    }

    virtual void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        Callback(std::move(Promise), std::move(*ev->Get()));
        this->Die(ctx);
    }

    void HandleUnexpectedEvent(const TString& requestType, ui32 eventType) {
        YDB_LOG_CRIT_COMP(NKikimrServices::KQP_GATEWAY, "TRequestHandlerBase, unexpected event",
            {"requestType", requestType},
            {"eventType", eventType});

        Promise.SetValue(NYql::NCommon::ResultFromError<TResult>(YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, TStringBuilder()
            << "Unexpected event in " << requestType << ": " << eventType)));
        this->PassAway();
    }

    void Handle(NKikimr::TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Tablet not available, status: " << (ui32)ev->Get()->Status, {}));
            this->Die(ctx);
        }
    }

    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            "Connection to tablet was lost.", {}));
        this->Die(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            "Failed to deliver request to destination.", {}));
        this->Die(ctx);
    }

    ~TRequestHandlerBase() override {
        if (Promise.Initialized() && !Promise.IsReady()) {
            Promise.TrySetValue(NYql::NCommon::ResultFromIssues<TResult>(
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                "Shutting down.", {}));
        }
    }

protected:
    THolder<TRequest> Request;
    // Note: Promise must be moved into Callback to avoid racing with
    // the destructor.
    NThreading::TPromise<TResult> Promise;
    TCallbackFunc Callback;
};

template<typename TRequest, typename TResponse, typename TResult>
class TActorRequestHandler: public TRequestHandlerBase<
    TActorRequestHandler<TRequest, TResponse, TResult>,
    TRequest,
    TResponse,
    TResult>
{
public:
    using TBase = typename TActorRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;

    using TStatusFunc = std::function<Ydb::StatusIds::StatusCode(const TResponse&)>;

    TActorRequestHandler(TActorId actorId, TRequest* request, NThreading::TPromise<TResult> promise,
            TCallbackFunc callback, NWilson::TSpan span = {}, TStatusFunc status = {})
        : TBase(request, promise, std::move(callback))
        , ActorId(actorId)
        , Span(std::move(span))
        , Status(std::move(status))
    {}

    ~TActorRequestHandler() override {
        if (Span) {
            Span.EndError("Request did not complete");
        }
    }

    void HandleResponse(typename TResponse::TPtr& ev, const TActorContext& ctx) override {
        if (Span) {
            EndQueryTraceSpan(Span, Status ? Status(*ev->Get()) : Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
        }
        TBase::HandleResponse(ev, ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(ActorId, this->Request.Release(), IEventHandle::FlagTrackDelivery, 0, Span.GetTraceId());

        this->Become(&TActorRequestHandler::AwaitState);
    }

    using TBase::Handle;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvents::TEvUndelivered, Handle);
        default:
            TBase::HandleUnexpectedEvent("TActorRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ActorId;
    NWilson::TSpan Span;
    TStatusFunc Status;
};

}
