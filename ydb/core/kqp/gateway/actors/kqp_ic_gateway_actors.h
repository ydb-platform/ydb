#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>

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
        Callback(Promise, std::move(*ev->Get()));
        this->Die(ctx);
    }

    void HandleUnexpectedEvent(const TString& requestType, ui32 eventType) {
        ALOG_CRIT(NKikimrServices::KQP_GATEWAY, "TRequestHandlerBase, unexpected event, request type: "
            << requestType << ", event type: " << eventType);

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

protected:
    THolder<TRequest> Request;
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

    TActorRequestHandler(TActorId actorId, TRequest* request, NThreading::TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback)
        , ActorId(actorId) {}

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(ActorId, this->Request.Release(), IEventHandle::FlagTrackDelivery);

        this->Become(&TActorRequestHandler::AwaitState);
    }

    using TBase::Handle;
    using TBase::HandleResponse;

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
};

}
