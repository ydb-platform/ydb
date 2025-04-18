#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

namespace NKikimr::NKqp::NScheduler {

class TPool;

struct TSchedulerEntity {
    explicit TSchedulerEntity(TPool* pool);
    ~TSchedulerEntity();

    TPool* const Pool;
    i64 Weight;
    double Vruntime = 0;
    double Vstart;

    double Vcurrent;

    TDuration MaxDelay;

    static constexpr double WakeupDelay = 1.1;
    TDuration LastExecutionTime;

    TDuration OverflowToleranceTimeout = TDuration::Seconds(1);

    static constexpr TDuration ActivationPenalty = TDuration::MicroSeconds(10);

    size_t Wakeups = 0;
    bool IsThrottled = false;

    void TrackTime(TDuration time, TMonotonic);
    void UpdateLastExecutionTime(TDuration time);

    TMaybe<TDuration> Delay(TMonotonic now, TPool* pool);
    TMaybe<TDuration> Delay(TMonotonic now);

    void MarkThrottled();
    void MarkResumed();
    void MarkResumed(TMonotonic now);
};

struct TComputeActorOptions {
    TMonotonic Now;
    NActors::TActorId SchedulerActorId;
    THolder<TSchedulerEntity> Handle;
    TString Pool = "";
    double Weight = 1;
    bool NoThrottle = true;
    TIntrusivePtr<TKqpCounters> Counters = nullptr;
};

struct TEvSchedulerUnregister : public TEventLocal<TEvSchedulerUnregister, TKqpComputeSchedulerEvents::EvUnregister> {
    THolder<TSchedulerEntity> SchedulerEntity;

    TEvSchedulerUnregister(THolder<TSchedulerEntity> entity)
        : SchedulerEntity(std::move(entity))
    {
    }
};

template<typename TDerived>
class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived> {
private:
    using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;

    static constexpr double SecToUsec = 1e6;

public:
    template<typename... TArgs>
    TSchedulableComputeActorBase(TComputeActorOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , SelfHandle(std::move(options.Handle))
        , SchedulerActorId(options.SchedulerActorId)
        , NoThrottle(options.NoThrottle)
        , Counters(options.Counters)
        , Pool(options.Pool)
        , Weight(options.Weight)
    {
        if (!NoThrottle) {
            Y_ABORT_UNLESS(Counters);
            Y_ABORT_UNLESS(SelfHandle);
        } else {
            Y_ABORT_UNLESS(!SelfHandle);
        }
    }

    static constexpr ui64 TAG_WAKEUP_RESUME = 201;

    TMonotonic Now() {
        return TMonotonic::Now();
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        CA_LOG_D("wakeup with tag " << tag);
        if (tag == TAG_WAKEUP_RESUME) {
            TBase::DoExecute();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }

    STFUNC(BaseStateFuncBody) {
        AccountActorSystemStats(TlsActivationContext->Monotonic());
        // we assume that exception handling is done in parents/descendants
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
            default:
                TBase::BaseStateFuncBody(ev);
        }
    }

protected:
    void DoBoostrap() {
        if (!SelfHandle) {
            return;
        }

        OldActivationStats = TlsActivationContext->AsActorContext().Mailbox.GetElapsedCycles();
        if (!OldActivationStats.has_value()) {
            TlsActivationContext->AsActorContext().Mailbox.EnableStats();
            OldActivationStats = TlsActivationContext->AsActorContext().Mailbox.GetElapsedCycles();
        }

        Y_ABORT_UNLESS(OldActivationStats.has_value());
    }

private:
    void ReportThrottledTime(TMonotonic now) {
        if (!Throttled) {
            return;
        }

        if (Counters) {
            Counters->SchedulerThrottled->Add((now - *Throttled).MicroSeconds());
        }

        SelfHandle->MarkResumed(now);
        Throttled.Clear();
    }

protected:
    void DoExecuteImpl() override {
        if (!SelfHandle) {
            if (NoThrottle) {
                return TBase::DoExecuteImpl();
            } else {
                return;
            }
        }

        TMonotonic now = Now();
        AccountActorSystemStats(now);
        TMaybe<TDuration> delay = CalcDelay(now);
        bool executed = false;
        if (NoThrottle || !delay) {
            ReportThrottledTime(now);
            executed = true;

            ExecutionTimer.ConstructInPlace();
            TBase::DoExecuteImpl();

            TDuration passed = TDuration::MicroSeconds(ExecutionTimer->Passed() * SecToUsec);

            if (Finished) {
                return;
            }
            TrackedWork += passed;
            SelfHandle->UpdateLastExecutionTime(passed);
            SelfHandle->TrackTime(passed, now);
            Counters->ComputeActorExecutions->Collect(passed.MicroSeconds());
        }
        if (delay) {
            Counters->SchedulerDelays->Collect(delay->MicroSeconds());
            CA_LOG_D("schedule wakeup after " << delay->MicroSeconds() << " msec ");
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup(TAG_WAKEUP_RESUME));
        }

        if (!executed) {
            if (!Throttled) {
                SelfHandle->MarkThrottled();
                Throttled = now;
            } else {
                Counters->ThrottledActorsSpuriousActivations->Inc();
            }
        }
        ExecutionTimer.Clear();
    }

    void AccountActorSystemStats(NMonotonic::TMonotonic now) {
        if (!SelfHandle) {
            return;
        }

        auto newStats = TlsActivationContext->AsActorContext().Mailbox.GetElapsedCycles();
        Y_ABORT_UNLESS(OldActivationStats.has_value());
        Y_ABORT_UNLESS(newStats.has_value());
        Y_ABORT_UNLESS(*newStats >= *OldActivationStats);
        auto toAccount = TDuration::MicroSeconds(NHPTimer::GetSeconds(*newStats - *OldActivationStats) * 1e6);
        {
            auto minTime = Min(toAccount, TrackedWork);
            TrackedWork -= minTime;
            toAccount -= minTime;
        }

        SelfHandle->TrackTime(toAccount, now);
        OldActivationStats = newStats;
    }

    TMaybe<TDuration> CalcDelay(NMonotonic::TMonotonic now) {
        auto result = SelfHandle->Delay(now);
        Counters->ComputeActorDelays->Collect(result.GetOrElse(TDuration::Zero()).MicroSeconds());
        if (NoThrottle || !result.Defined()) {
            return {};
        } else {
            return result;
        }
    }

    void PassAway() override {
        Finished = true;
        if (SelfHandle) {
            auto now = Now();
            if (Throttled) {
                SelfHandle->MarkResumed(now);
            }
            if (ExecutionTimer) {
                TDuration passed = TDuration::MicroSeconds(ExecutionTimer->Passed() * SecToUsec);
                SelfHandle->TrackTime(passed, now);
            }
        }
        if (SelfHandle) {
            auto finishEv = MakeHolder<TEvSchedulerUnregister>(std::move(SelfHandle));
            this->Send(SchedulerActorId, finishEv.Release());
        }
        TBase::PassAway();
    }

private:
    TMaybe<THPTimer> ExecutionTimer;
    TDuration TrackedWork = TDuration::Zero();
    TMaybe<TMonotonic> Throttled;
    THolder<TSchedulerEntity> SelfHandle;
    NActors::TActorId SchedulerActorId;
    bool NoThrottle;
    bool Finished = false;

    std::optional<ui64> OldActivationStats;

    TIntrusivePtr<TKqpCounters> Counters;

    TString Pool;
    double Weight;
};

} // namespace NKikimr::NKqp::NScheduler
