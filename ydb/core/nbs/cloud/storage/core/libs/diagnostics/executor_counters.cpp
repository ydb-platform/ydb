#include "executor_counters.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/intrlist.h>
#include <util/system/spinlock.h>

namespace NYdb::NBS {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

struct TExecutorCounters::TActivityCounters
{
    TDynamicCounters::TCounterPtr InProgress;
    TDynamicCounters::TCounterPtr Count;
    TDynamicCounters::TCounterPtr Time;

    void Register(TDynamicCounters& counters)
    {
        InProgress = counters.GetCounter("InProgress");
        Count = counters.GetCounter("Count", true);
        Time = counters.GetCounter("Time", true);
    }

    void Started()
    {
        InProgress->Inc();
    }

    void Completed()
    {
        InProgress->Dec();
        Count->Inc();
    }

    void ReportProgress(TDuration time)
    {
        Time->Add(time.MicroSeconds());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TExecutorCounters::TExecutor: public TIntrusiveListItem<TExecutor>
{
    std::array<TAtomic, EActivity::MAX> Activities{};

    void Started(int index)
    {
        auto started = AtomicSwap(&Activities[index], GetCycleCount());
        Y_DEBUG_ABORT_UNLESS(!started);
    }

    TDuration Completed(int index)
    {
        auto started = AtomicSwap(&Activities[index], 0);
        Y_DEBUG_ABORT_UNLESS(started);

        return CyclesToDurationSafe(GetCycleCount() - started);
    }

    TDuration ReportProgress(int index)
    {
        for (;;) {
            auto started = AtomicGet(Activities[index]);
            if (!started) {
                return TDuration::Zero();
            }

            auto now = GetCycleCount();
            if (AtomicCas(&Activities[index], now, started)) {
                return CyclesToDurationSafe(now - started);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TExecutorCounters::TImpl
{
    std::array<TActivityCounters, EActivity::MAX> Activities;
    TIntrusiveListWithAutoDelete<TExecutor, TDelete> Executors;
    TAdaptiveLock Lock;
};

////////////////////////////////////////////////////////////////////////////////

TExecutorCounters::TExecutorCounters()
    : Impl(std::make_unique<TExecutorCounters::TImpl>())
{}

TExecutorCounters::~TExecutorCounters() = default;

void TExecutorCounters::Register(TDynamicCounters& counters)
{
    Impl->Activities[WAIT].Register(*counters.GetSubgroup("activity", "Wait"));
    Impl->Activities[EXECUTE].Register(
        *counters.GetSubgroup("activity", "Execute"));
}

void TExecutorCounters::UpdateStats()
{
    with_lock (Impl->Lock) {
        for (auto& executor: Impl->Executors) {
            for (int index = 0; index < EActivity::MAX; ++index) {
                if (auto time = executor.ReportProgress(index)) {
                    Impl->Activities[index].ReportProgress(time);
                }
            }
        }
    }
}

TExecutorCounters::TExecutor* TExecutorCounters::AllocExecutor()
{
    auto executor = std::make_unique<TExecutor>();
    with_lock (Impl->Lock) {
        Impl->Executors.PushBack(executor.release());
        return Impl->Executors.Back();
    }
}

void TExecutorCounters::ReleaseExecutor(TExecutor* executor)
{
    with_lock (Impl->Lock) {
        executor->Unlink();
    }

    delete executor;
}

void TExecutorCounters::ActivityStarted(TExecutor* executor, int index)
{
    executor->Started(index);

    Impl->Activities[index].Started();
}

void TExecutorCounters::ActivityCompleted(TExecutor* executor, int index)
{
    if (auto time = executor->Completed(index)) {
        Impl->Activities[index].ReportProgress(time);
    }

    Impl->Activities[index].Completed();
}

}   // namespace NYdb::NBS
