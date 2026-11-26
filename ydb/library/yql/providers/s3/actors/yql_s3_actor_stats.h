#pragma once

#include "yql_s3_actor_stats_interface.h"

#include <ydb/library/yql/dq/actors/common/dq_schedulable.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NYql::NDq {

// Observes the caller's scheduler state without changing quota accounting.
// Gauges count live work; unfinished exits are retained in a cumulative
// counter.
class TS3ActorStats final: public IS3ActorStats {
private:
    enum class ECpuState {
        Idle,
        Waiting,
        Active
    };

public:
    TS3ActorStats(const ::NMonitoring::TDynamicCounterPtr &counters, const IDqSchedulableWork *work, const TString &kind = "reader")
        : SchedulerEnabled(work != nullptr)
    {
        if (!counters || !work) {
            return;
        }
        const auto scope = work->GetWorkScope();
        const auto group = counters->GetSubgroup("db", scope.Namespace)->GetSubgroup("pool", scope.Name)->GetSubgroup("kind", kind);
        Active = group->GetCounter("S3CpuActive");
        Waiting = group->GetCounter("S3CpuWaiting");
        AdmissionWaitUs = group->GetCounter("S3CpuAdmissionWaitUs", true);
        UnfinishedOnExit = group->GetCounter("S3CpuUnfinishedOnExit", true);
    }

    TS3ActorStats(const TS3ActorStats &) = delete;
    TS3ActorStats &operator=(const TS3ActorStats &) = delete;

    ~TS3ActorStats() {
        OnExit(TMonotonic::Now());
    }

    TSnapshot GetSnapshot() const override {
        const auto guard = Guard(Lock);
        return { Phase, StateSince, CpuState == ECpuState::Active };
    }

    void SetState(EState state, TMonotonic now = TMonotonic::Now()) {
        const auto guard = Guard(Lock);
        SetStateLocked(state, now);
    }

    void OnAllowed(TMonotonic now) {
        const auto guard = Guard(Lock);
        if (!SchedulerEnabled) {
            return;
        }
        FinishWaiting(now);
        if (CpuState != ECpuState::Active) {
            if (Active) {
                Active->Inc();
            }
            CpuState = ECpuState::Active;
        }
    }

    void OnRejected(TMonotonic now) {
        const auto guard = Guard(Lock);
        if (!SchedulerEnabled) {
            return;
        }
        if (CpuState == ECpuState::Idle) {
            if (Waiting) {
                Waiting->Inc();
            }
            WaitStarted = now;
            CpuState = ECpuState::Waiting;
        }
    }

    void OnStop(TMonotonic now) {
        const auto guard = Guard(Lock);
        StopLocked(now);
    }

    // Observes unfinished work before diagnostic cleanup. Does not release quota.
    // Explicitly called on Run() exit because the source may retain this object.
    void OnExit(TMonotonic now) {
        const auto guard = Guard(Lock);
        if (CpuState != ECpuState::Idle) {
            if (UnfinishedOnExit) {
                UnfinishedOnExit->Inc();
            }
            StopLocked(now);
        }
        SetStateLocked(EState::Finished, now);
    }

private:
    void SetStateLocked(EState state, TMonotonic now) {
        if (Phase != state) {
            Phase = state;
            StateSince = now;
        }
    }

    void StopLocked(TMonotonic now) {
        if (CpuState == ECpuState::Active) {
            if (Active) {
                Active->Dec();
            }
        } else {
            FinishWaiting(now);
        }
        CpuState = ECpuState::Idle;
    }

    void FinishWaiting(TMonotonic now) {
        if (CpuState == ECpuState::Waiting) {
            if (Waiting) {
                Waiting->Dec();
            }
            if (AdmissionWaitUs) {
                AdmissionWaitUs->Add((now - WaitStarted).MicroSeconds());
            }
        }
    }

private:
    mutable TAdaptiveLock Lock;
    const bool SchedulerEnabled;
    EState Phase = EState::Initializing;
    TMonotonic StateSince = TMonotonic::Now();
    ECpuState CpuState = ECpuState::Idle;
    TMonotonic WaitStarted;
    ::NMonitoring::TDynamicCounters::TCounterPtr Active;
    ::NMonitoring::TDynamicCounters::TCounterPtr Waiting;
    ::NMonitoring::TDynamicCounters::TCounterPtr AdmissionWaitUs;
    ::NMonitoring::TDynamicCounters::TCounterPtr UnfinishedOnExit;
};

}   // namespace NYql::NDq
