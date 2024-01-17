#pragma once

#include "metrics.h"

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/retry/retry_policy.h>

namespace NFq {

template<typename TRequest, typename TResponse, typename ...TArgs>
class TRetryActor : public NActors::TActorBootstrapped<TRetryActor<TRequest, TResponse, TArgs...>> {
public:
    using TBase = NActors::TActorBootstrapped<TRetryActor<TRequest, TResponse, TArgs...>>;
    using TBase::Become;
    using TBase::PassAway;
    using TBase::SelfId;
    using TBase::Send;

    using IRetryPolicy = IRetryPolicy<const typename TResponse::TPtr&>;

    TRetryActor(const TComputeRequestCountersPtr& counters, const NActors::TActorId& sender, const NActors::TActorId& recipient, const TArgs&... args)
        : Sender(sender)
        , Recipient(recipient)
        , CreateMessage([=]() {
            return new TRequest(args...);
        })
        , RetryState(GetRetryPolicy()->CreateRetryState())
        , Delay(TDuration::Zero())
        , StartTime(TInstant::Now())
        , Counters(counters)
    {}

    TRetryActor(const TComputeRequestCountersPtr& counters, const TDuration& delay, const NActors::TActorId& sender, const NActors::TActorId& recipient, const TArgs&... args)
        : Sender(sender)
        , Recipient(recipient)
        , CreateMessage([=]() {
            return new TRequest(args...);
        })
        , RetryState(GetRetryPolicy()->CreateRetryState())
        , Delay(delay)
        , StartTime(TInstant::Now())
        , Counters(counters)
    {}

    void Bootstrap() {
        Counters->InFly->Inc();
        Become(&TRetryActor::StateFunc);
        NActors::TActivationContext::Schedule(Delay, new NActors::IEventHandle(Recipient, static_cast<const NActors::TActorId&>(SelfId()), CreateMessage()));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TResponse, Handle);
    )

    void Handle(const typename TResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        auto delay = RetryState->GetNextRetryDelay(ev);
        if (delay) {
            Counters->Retry->Inc();
            NActors::TActivationContext::Schedule(*delay, new NActors::IEventHandle(Recipient, static_cast<const NActors::TActorId&>(SelfId()), CreateMessage()));
            return;
        }
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (response.Status != NYdb::EStatus::SUCCESS) {
            Counters->Error->Inc();
        } else {
            Counters->Ok->Inc();
        }
        Send(ev->Forward(Sender));
        PassAway();
    }

    static const typename IRetryPolicy::TPtr& GetRetryPolicy() {
        static typename IRetryPolicy::TPtr policy = IRetryPolicy::GetExponentialBackoffPolicy([](const typename TResponse::TPtr& ev) {
            const auto& status = ev->Get()->Status;
            return RetryComputeClass(status);
        }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 5);
        return policy;
    }

    static bool IsTransportError(const NYdb::EStatus& status) {
        return static_cast<size_t>(status) >= NYdb::TRANSPORT_STATUSES_FIRST
            && static_cast<size_t>(status) <= NYdb::TRANSPORT_STATUSES_LAST;
    }

    static ERetryErrorClass RetryComputeClass(const NYdb::EStatus& status) {
        if (IsTransportError(status)) {
            return ERetryErrorClass::ShortRetry;
        }

        if (status == NYdb::EStatus::INTERNAL_ERROR
            || status == NYdb::EStatus::UNAVAILABLE
            || status == NYdb::EStatus::TIMEOUT
            || status == NYdb::EStatus::BAD_SESSION
            || status == NYdb::EStatus::SESSION_EXPIRED
            || status == NYdb::EStatus::UNDETERMINED
            || status == NYdb::EStatus::TRANSPORT_UNAVAILABLE
            || status == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED
            || status == NYdb::EStatus::CLIENT_INTERNAL_ERROR
            || status == NYdb::EStatus::CLIENT_OUT_OF_RANGE
            || status == NYdb::EStatus::CLIENT_DISCOVERY_FAILED) {
            return ERetryErrorClass::ShortRetry;
        }

        if (status == NYdb::EStatus::OVERLOADED
            || status == NYdb::EStatus::SESSION_BUSY
            || status == NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED
            || status == NYdb::EStatus::CLIENT_LIMITS_REACHED) {
            return ERetryErrorClass::LongRetry;
        }
        return ERetryErrorClass::NoRetry;
    }

    virtual ~TRetryActor() {
        Counters->InFly->Dec();
    }

private:
    NActors::TActorId Sender;
    NActors::TActorId Recipient;
    std::function<TRequest*()> CreateMessage;
    typename IRetryPolicy::IRetryState::TPtr RetryState;
    TDuration Delay;
    TInstant StartTime;
    TComputeRequestCountersPtr Counters;
};

} /* NFq */
