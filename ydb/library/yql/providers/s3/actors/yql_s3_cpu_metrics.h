#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql::NDq {

// Observes the caller's scheduler state without changing quota accounting.
// Gauges count live work; unfinished exits are retained in a cumulative
// counter.
class TS3CpuMetrics {
public:
    TS3CpuMetrics(const ::NMonitoring::TDynamicCounterPtr &counters, const IDqSchedulableWork *work, const TString &kind) {
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

    TS3CpuMetrics(const TS3CpuMetrics &) = delete;
    TS3CpuMetrics &operator=(const TS3CpuMetrics &) = delete;

    ~TS3CpuMetrics() {
        OnExit(TMonotonic::Now());
    }

    void OnAdmission(bool admitted, TMonotonic now) {
        if (!Active) {
            return;
        }
        if (admitted) {
            FinishWaiting(now);
            if (State != EState::Active) {
                Active->Inc();
                State = EState::Active;
            }
        } else if (State == EState::Idle) {
            Waiting->Inc();
            WaitStarted = now;
            State = EState::Waiting;
        }
    }

    void OnStop(TMonotonic now) {
        if (State == EState::Active) {
            Active->Dec();
        } else {
            FinishWaiting(now);
        }
        State = EState::Idle;
    }

    // Called on every Run() exit, with the destructor as a fallback. Deliberately
    // does not call StopExecution(): these metrics must expose missing cleanup.
    void OnExit(TMonotonic now) {
        if (State != EState::Idle) {
            UnfinishedOnExit->Inc();
            OnStop(now);
        }
    }

private:
    void FinishWaiting(TMonotonic now) {
        if (State == EState::Waiting) {
            Waiting->Dec();
            // Accumulated when admission succeeds or the wait ends on exit.
            AdmissionWaitUs->Add((now - WaitStarted).MicroSeconds());
        }
    }

    enum class EState {
        Idle,
        Waiting,
        Active
    };
    EState State = EState::Idle;
    TMonotonic WaitStarted;
    ::NMonitoring::TDynamicCounters::TCounterPtr Active;
    ::NMonitoring::TDynamicCounters::TCounterPtr Waiting;
    ::NMonitoring::TDynamicCounters::TCounterPtr AdmissionWaitUs;
    ::NMonitoring::TDynamicCounters::TCounterPtr UnfinishedOnExit;
};

}   // namespace NYql::NDq
