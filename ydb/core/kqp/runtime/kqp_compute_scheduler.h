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

    operator bool () {
        return Ptr.get() != nullptr;
    }

    TSchedulerEntity& operator*() {
        return *Ptr;
    }

    void TrackTime(TDuration time, TMonotonic now);

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

    TSchedulerEntityHandle Enroll(TString group, double weight, TMonotonic now);

    void AdvanceTime(TMonotonic now);

    void Deregister(TSchedulerEntity& self, TMonotonic now);

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
            if (Counters) {
                GroupUsage = Counters->GetKqpCounters()
                    ->GetSubgroup("NodeScheduler/Group", options.Group)
                    ->GetCounter("Usage", true);
            }
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

    void DoBootstrap() {
        //ScheduleReniceWakeup();
    }

    STFUNC(BaseStateFuncBody) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::HandleWakeup);
                default:
                    TBase::BaseStateFuncBody(ev);
            }
        } catch (...) {
            CA_LOG_E("exception in CA handler " << CurrentExceptionMessage());
            PassAway();
        }
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
        if (!SelfHandle) {
            if (NoThrottle) {
                return TBase::DoExecuteImpl();
            } else {
                return;
            }
        }

        ExecuteStart = Now();
        TMonotonic now = *ExecuteStart;
        TMaybe<TDuration> delay = CalcDelay(*ExecuteStart);
        bool executed = false;
        if (NoThrottle || !delay) {
            ReportThrottledTime(now);
            executed = true;

            THPTimer timer;
            TBase::DoExecuteImpl();

            double passed = timer.Passed() * SecToUsec;

            if (Finished) {
                return;
            }
            SelfHandle.TrackTime(TDuration::MicroSeconds(passed), now);
            Counters->ComputeActorExecutions->Collect(passed);
            if (GroupUsage) {
                GroupUsage->Add(passed);
            }
        }
        if (delay) {
            CA_LOG_D("schedule wakeup after " << delay->MicroSeconds() << " msec ");
            this->Schedule(*delay, new NActors::TEvents::TEvWakeup(ResumeWakeupTag));
        }

        if (!executed && !Throttled) {
            SelfHandle.MarkThrottled();
            Throttled = now;
        }
        ExecuteStart.Clear();
    }

    TMaybe<TDuration> CalcDelay(NMonotonic::TMonotonic now) {
        auto result = SelfHandle.Delay(now);
        if (NoThrottle || !result.Defined()) {
            return {};
        } else {
            return result;
        }
    }

    void PassAway() override {
        Finished = true;
        if (ExecuteStart && SelfHandle) {
            auto now = Now();
            SelfHandle.TrackTime(now - *ExecuteStart, now);
        }
        if (SelfHandle) {
            auto finishEv = MakeHolder<TEvSchedulerDeregister>(std::move(SelfHandle));
            this->Send(NodeService, finishEv.Release());
        }
        TBase::PassAway();
    }

private:
    TMaybe<TMonotonic> ExecuteStart;
    TMaybe<TMonotonic> Throttled;
    TSchedulerEntityHandle SelfHandle;
    NActors::TActorId NodeService;
    bool NoThrottle;
    bool Finished = false;

    TIntrusivePtr<TKqpCounters> Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr GroupUsage;

    TString Group;
    double Weight;
};

} // namespace NKqp
} // namespace NKikimR
