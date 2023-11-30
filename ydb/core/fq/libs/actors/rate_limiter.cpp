#include "proxy.h"

#include <ydb/core/fq/libs/private_client/events.h>
#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/retry/retry_policy.h>

namespace NFq {

namespace {

template <class TRequest, class TResponse>
class TRetryingActor : public NActors::TActorBootstrapped<TRetryingActor<TRequest, TResponse>> {
public:
    using IRetryPolicy = IRetryPolicy<const NYdb::TStatus&>;

    TRetryingActor(const NActors::TActorId& parent, const typename TRequest::TProto& proto)
        : RetryState(GetRetryPolicy()->CreateRetryState())
        , Proto(proto)
        , Parent(parent)
    {
    }

    void Bootstrap() {
        this->Become(&TRetryingActor::StateFunc);
        SendRequest();
    }

    void SendRequest() {
        this->Send(NFq::MakeInternalServiceActorId(), new TRequest(Proto));
    }

    void Wakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        RetryScheduled = false;
        SendRequest();
    }

    void Handle(typename TResponse::TPtr& ev) {
        const TMaybe<TDuration> delay = !Canceled ? RetryState->GetNextRetryDelay(ev->Get()->Status) : Nothing();
        if (delay) {
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup());
            RetryScheduled = true;
            LastResponse = std::move(ev);
        } else {
            this->Send(Parent, ev->Release().Release());
            this->PassAway();
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        Canceled = true;
        if (RetryScheduled && LastResponse) {
            Handle(LastResponse); // Will send last response to parent
        }
    }

    STRICT_STFUNC(
        StateFunc,
        hFunc(NActors::TEvents::TEvPoison, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Wakeup);
        hFunc(TResponse, Handle);
    )

    static ERetryErrorClass Retryable(const NYdb::TStatus& status) {
        if (status.IsSuccess()) {
            return ERetryErrorClass::NoRetry;
        }

        if (status.IsTransportError()) {
            return ERetryErrorClass::ShortRetry;
        }

        const NYdb::EStatus st = status.GetStatus();
        if (st == NYdb::EStatus::INTERNAL_ERROR
            || st == NYdb::EStatus::UNAVAILABLE
            || st == NYdb::EStatus::TIMEOUT
            || st == NYdb::EStatus::BAD_SESSION
            || st == NYdb::EStatus::SESSION_EXPIRED
            || st == NYdb::EStatus::SESSION_BUSY) {
            return ERetryErrorClass::ShortRetry;
        }

        if (st == NYdb::EStatus::OVERLOADED) {
            return ERetryErrorClass::LongRetry;
        }

        return ERetryErrorClass::NoRetry;
    }

    static const IRetryPolicy::TPtr& GetRetryPolicy() {
        static IRetryPolicy::TPtr policy = IRetryPolicy::GetExponentialBackoffPolicy(Retryable);
        return policy;
    }

private:
    const IRetryPolicy::IRetryState::TPtr RetryState;
    const typename TRequest::TProto Proto;
    const NActors::TActorId Parent;
    bool Canceled = false;
    bool RetryScheduled = false;
    typename TResponse::TPtr LastResponse;
};

} // namespace

NActors::IActor* CreateRateLimiterResourceCreator(
    const NActors::TActorId& parent,
    const TString& ownerId,
    const TString& queryId,
    const NYdb::NFq::TScope& scope,
    const TString& tenant)
{
    Fq::Private::CreateRateLimiterResourceRequest req;
    req.set_owner_id(ownerId);
    req.mutable_query_id()->set_value(queryId);
    req.set_scope(scope.ToString());
    req.set_tenant(tenant);
    return new TRetryingActor<NFq::TEvInternalService::TEvCreateRateLimiterResourceRequest, NFq::TEvInternalService::TEvCreateRateLimiterResourceResponse>(parent, req);
}

NActors::IActor* CreateRateLimiterResourceDeleter(
    const NActors::TActorId& parent,
    const TString& ownerId,
    const TString& queryId,
    const NYdb::NFq::TScope& scope,
    const TString& tenant)
{
    Fq::Private::DeleteRateLimiterResourceRequest req;
    req.set_owner_id(ownerId);
    req.mutable_query_id()->set_value(queryId);
    req.set_scope(scope.ToString());
    req.set_tenant(tenant);
    return new TRetryingActor<NFq::TEvInternalService::TEvDeleteRateLimiterResourceRequest, NFq::TEvInternalService::TEvDeleteRateLimiterResourceResponse>(parent, req);
}

} // namespace NFq
