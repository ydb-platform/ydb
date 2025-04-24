#pragma once

#include "fwd.h"

#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

namespace NKikimr::NKqp::NScheduler {

// The proxy-object between any schedulable actor and the scheduler itself
struct TSchedulableTask {
    explicit TSchedulableTask(const NHdrf::TQueryPtr& query);
    ~TSchedulableTask();

    void IncreaseUsage(const TDuration& burstThrottle);
    void DecreaseUsage(const TDuration& burstUsage);

    NHdrf::TQueryPtr Query;
};

class TSchedulableActorHelper {
public:
    struct TOptions {
        TSchedulableTaskPtr SchedulableTask;
    };

protected:
    explicit TSchedulableActorHelper(TOptions&& options);

    static TMonotonic Now();
    bool IsSchedulable() const;

    void StartExecution(const TDuration& burstThrottle);
    void StopExecution();

    std::optional<TDuration> CalculateDelay(TMonotonic now) const;

    void AccountThrottledTime(TMonotonic now);

private:
    TSchedulableTaskPtr SchedulableTask;
    THPTimer Timer;
};

template <class TDerived>
class TSchedulableActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived>, private TSchedulableActorHelper {
    using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;
    static constexpr ui64 TAG_WAKEUP_RESUME = 201; // TODO: why this value for magic number?

public:
    template<typename ... TArgs>
    TSchedulableActorBase(TOptions options, TArgs&& ... args)
        : TBase(std::forward<TArgs>(args) ...)
        , TSchedulableActorHelper(std::move(options))
    {
    }

protected:
    void DoBootstrap() {
        // TODO: implement this
    }

    // Magic state function name to overload
    STATEFN(BaseStateFuncBody) {
        // we assume that exception handling is done in parents/descendants
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, TSchedulableActorBase<TDerived>::Handle);
            default:
                TBase::BaseStateFuncBody(ev);
        }
    }

    void PassAway() override {
        PassedAway = true;

        if (IsSchedulable()) {
            if (!Throttled) {
                StopExecution();
            }

            // TODO: do we need to send anything to scheduler?
        }

        TBase::PassAway();
    }

private:
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == TAG_WAKEUP_RESUME) {
            TBase::DoExecute();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }

    void DoExecuteImpl() override {
        if (!IsSchedulable()) {
            return TBase::DoExecuteImpl();
        }

        const auto now = Now();

        // TODO: account waiting on mailbox?

        if (auto delay = CalculateDelay(now)) {
            Throttled = true;
            StartThrottle = now;
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup(TAG_WAKEUP_RESUME));
            return;
        }

        TDuration burstThrottle;
        if (Throttled) {
            burstThrottle = now - StartThrottle;
        }
        Throttled = false;

        StartExecution(burstThrottle);
        TBase::DoExecuteImpl();
        if (!PassedAway) {
            StopExecution();
        }
    }

private:
    bool PassedAway = false;
    bool Throttled = false;
    TMonotonic StartThrottle;
};

} // namespace NKikimr::NKqp::NScheduler
