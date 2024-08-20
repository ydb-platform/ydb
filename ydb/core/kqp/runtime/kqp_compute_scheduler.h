#pragma once

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>

namespace NKikimr {
namespace NKqp {

class TSchedulerEntity;
class TSchedulerEntityHandle {
private:
    std::unique_ptr<TSchedulerEntity> Ptr;

public:
    TSchedulerEntityHandle(TSchedulerEntity*);

    TSchedulerEntityHandle();
    TSchedulerEntityHandle(TSchedulerEntityHandle&&); 

    TSchedulerEntityHandle& operator = (TSchedulerEntityHandle&&);

    bool Defined() const {
        return Ptr.get() != nullptr;
    }

    operator bool () const {
        return Defined();
    }

    TSchedulerEntity& operator*() {
        return *Ptr;
    }

    void TrackTime(TDuration time, TMonotonic now);
    void ReportBatchTime(TDuration time);

    TMaybe<TDuration> Delay(TMonotonic now);

    void MarkThrottled();
    void MarkResumed();

    double EstimateWeight(TMonotonic now, TDuration minTime);

    void Clear();

    ~TSchedulerEntityHandle();
};

class TComputeScheduler {
public:
    struct TDistributionRule {
        double Share;
        TString Name;
        TVector<TDistributionRule> SubRules;

        bool empty() {
            return SubRules.empty() && Name.empty();
        }
    };

public:
    TComputeScheduler();
    ~TComputeScheduler();

    void ReportCounters(TIntrusivePtr<TKqpCounters>);

    void SetPriorities(TDistributionRule rootRule, double cores, TMonotonic now);
    void SetMaxDeviation(TDuration);
    void SetForgetInterval(TDuration);
    ::NMonitoring::TDynamicCounters::TCounterPtr GetGroupUsageCounter(TString group) const;

    TSchedulerEntityHandle Enroll(TString group, double weight, TMonotonic now);

    void AdvanceTime(TMonotonic now);

    void Deregister(TSchedulerEntity& self, TMonotonic now);

    bool Disabled(TString group);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

struct TComputeActorSchedulingOptions {
    TMonotonic Now;
    NActors::TActorId NodeService;
    TSchedulerEntityHandle Handle;
    TComputeScheduler* Scheduler;
    TString Group = "";
    double Weight = 1;
    bool NoThrottle = true;
    TIntrusivePtr<TKqpCounters> Counters = nullptr;
};

struct TKqpComputeSchedulerEvents {
    enum EKqpComputeSchedulerEvents {
        EvDeregister = EventSpaceBegin(TKikimrEvents::ES_KQP) + 400,
        EvAccountTime,
    };
};

struct TEvSchedulerDeregister : public TEventLocal<TEvSchedulerDeregister, TKqpComputeSchedulerEvents::EvDeregister> {
    TSchedulerEntityHandle SchedulerEntity;

    TEvSchedulerDeregister(TSchedulerEntityHandle entity)
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
    TSchedulableComputeActorBase(TComputeActorSchedulingOptions options, TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
        , SelfHandle(std::move(options.Handle))
        , NodeService(options.NodeService)
        , NoThrottle(options.NoThrottle)
        , Counters(options.Counters)
        , Group(options.Group)
        , Weight(options.Weight)
    {
        if (!NoThrottle) {
            Y_ABORT_UNLESS(Counters);
            Y_ABORT_UNLESS(SelfHandle);
            GroupUsage = options.Scheduler->GetGroupUsageCounter(options.Group);
        } else {
            Y_ABORT_UNLESS(!SelfHandle);
        }
    }

    static constexpr ui64 ResumeWakeupTag = 201;

    TMonotonic Now() {
        return TMonotonic::Now();
        //return TlsActivationContext->Monotonic();
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        CA_LOG_D("wakeup with tag " << tag);
        if (tag == ResumeWakeupTag) {
            TBase::DoExecute();
        } else {
            TBase::HandleExecuteBase(ev);
        }
    }

    STFUNC(BaseStateFuncBody) {
        AccountActorSystemStats(TlsActivationContext->Monotonic());
        // we assume that exception handling is done in parents/descendents
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
            default:
                TBase::BaseStateFuncBody(ev);
        }
    }

    void DoBoostrap() {
        if (!SelfHandle.Defined()) {
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
        if (Counters && Throttled) {
            Counters->SchedulerThrottled->Add((now - *Throttled).MicroSeconds());
        }
        if (Throttled) {
            SelfHandle.MarkResumed();
            Throttled.Clear();
        }
    }

protected:
    void DoExecuteImpl() override {
        if (!SelfHandle.Defined()) {
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
            SelfHandle.ReportBatchTime(passed);
            SelfHandle.TrackTime(passed, now);
            GroupUsage->Add(passed.MicroSeconds());
            Counters->ComputeActorExecutions->Collect(passed.MicroSeconds());
        }
        if (delay) {
            Counters->SchedulerDelays->Collect(delay->MicroSeconds());
            CA_LOG_D("schedule wakeup after " << delay->MicroSeconds() << " msec ");
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup(ResumeWakeupTag));
        }

        if (!executed) {
            if (!Throttled) {
                SelfHandle.MarkThrottled();
                Throttled = now;
            } else {
                Counters->ThrottledActorsSpuriousActivations->Inc();
            }
        }
        ExecutionTimer.Clear();
    }

    void AccountActorSystemStats(NMonotonic::TMonotonic now) {
        if (!SelfHandle.Defined()) {
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

        GroupUsage->Add(toAccount.MicroSeconds());
        SelfHandle.TrackTime(toAccount, now);
        OldActivationStats = newStats;
    }

    TMaybe<TDuration> CalcDelay(NMonotonic::TMonotonic now) {
        auto result = SelfHandle.Delay(now);
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
                SelfHandle.MarkResumed();
            }
            if (ExecutionTimer) {
                TDuration passed = TDuration::MicroSeconds(ExecutionTimer->Passed() * SecToUsec);
                SelfHandle.TrackTime(passed, now);
                GroupUsage->Add(passed.MicroSeconds());
            }
        }
        if (SelfHandle) {
            auto finishEv = MakeHolder<TEvSchedulerDeregister>(std::move(SelfHandle));
            this->Send(NodeService, finishEv.Release());
        }
        TBase::PassAway();
    }

private:
    TMaybe<THPTimer> ExecutionTimer;
    TDuration TrackedWork = TDuration::Zero();
    TMaybe<TMonotonic> Throttled;
    TSchedulerEntityHandle SelfHandle;
    NActors::TActorId NodeService;
    bool NoThrottle;
    bool Finished = false;

    std::optional<ui64> OldActivationStats;

    TIntrusivePtr<TKqpCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr GroupUsage;

    TString Group;
    double Weight;
};

} // namespace NKqp
} // namespace NKikimR
