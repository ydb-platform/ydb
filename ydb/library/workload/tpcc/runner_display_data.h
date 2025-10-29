#pragma once

#include "task_queue.h"
#include "terminal.h"

#include <memory>

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

// We have two sources of stats: terminal stats and per taskqueue thread stats.
// Note, that terminal stats are also per thread: multiple terminals are running on the same thread
// shared same stats object. Thus, terminal stats are aggregated values.
//
// Here we collect all the stats in a single place to easily display progress:
//  * terminal stats: TPC-C related statistics like per transaction type latencies, tpmC.
//  * thread stats: load, Queue

constexpr double ThreadSaturatedLoadThreshold = 0.8;

struct TAllStatistics {

    struct TThreadStatistics {
        TThreadStatistics() {
            TaskThreadStats = std::make_unique<ITaskQueue::TThreadStats>();
            TerminalStats = std::make_unique<TTerminalStats>();
        }

        void CalculateDerivative(const TThreadStatistics& prev, size_t seconds) {
            TerminalsPerSecond =
                (TaskThreadStats->InternalTasksResumed.load(std::memory_order_relaxed) -
                    prev.TaskThreadStats->InternalTasksResumed.load(std::memory_order_relaxed)) / seconds;

            QueriesPerSecond =
                (TaskThreadStats->ExternalTasksResumed.load(std::memory_order_relaxed) -
                    prev.TaskThreadStats->ExternalTasksResumed.load(std::memory_order_relaxed)) / seconds;

            ExecutingTime = TaskThreadStats->ExecutingTime.load(std::memory_order_relaxed) -
                prev.TaskThreadStats->ExecutingTime.load(std::memory_order_relaxed);

            TotalTime = TaskThreadStats->TotalTime.load(std::memory_order_relaxed) -
                prev.TaskThreadStats->TotalTime.load(std::memory_order_relaxed);

            InternalInflightWaitTimeMs = TaskThreadStats->InternalInflightWaitTimeMs;
            InternalInflightWaitTimeMs.Sub(prev.TaskThreadStats->InternalInflightWaitTimeMs);

            ExternalQueueTimeMs = TaskThreadStats->ExternalQueueTimeMs;
            ExternalQueueTimeMs.Sub(prev.TaskThreadStats->ExternalQueueTimeMs);

            Load = TotalTime != 0 ? (ExecutingTime / TotalTime) : 0.0;
        }

        std::unique_ptr<ITaskQueue::TThreadStats> TaskThreadStats;
        std::unique_ptr<TTerminalStats> TerminalStats;

        size_t TerminalsPerSecond = 0;
        size_t QueriesPerSecond = 0;

        double ExecutingTime = 0;
        double TotalTime = 0;
        double Load = 0.0;
        THistogram InternalInflightWaitTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
        THistogram ExternalQueueTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
    };

    TAllStatistics(size_t threadCount, Clock::time_point ts)
        : StatVec(threadCount)
        , Ts(ts)
    {
    }

    void CalculateDerivativeAndTotal(const TAllStatistics& prev) {
        size_t seconds = duration_cast<std::chrono::duration<size_t>>(Ts - prev.Ts).count();

        // Calculate per-thread derivatives
        if (seconds != 0) {
            for (size_t i = 0; i < StatVec.size(); ++i) {
                StatVec[i].CalculateDerivative(prev.StatVec[i], seconds);
            }
        }

        // Aggregate total statistics
        SaturatedThreads = 0;
        for (const auto& stats: StatVec) {
            stats.TerminalStats->Collect(TotalTerminalStats);
            if (stats.Load >= ThreadSaturatedLoadThreshold) {
                ++SaturatedThreads;
            }
        }
    }

    TVector<TThreadStatistics> StatVec;
    const Clock::time_point Ts;

    TTerminalStats TotalTerminalStats;
    size_t SaturatedThreads = 0;
};

struct TRunStatusData {
    // Phase and timing (computed values only)
    std::string Phase;
    int ElapsedMinutesTotal = 0;
    int ElapsedSecondsTotal = 0;
    int RemainingMinutesTotal = 0;
    int RemainingSecondsTotal = 0;

    double ProgressPercentTotal = 0.0;
    double WarmupPercent = 0.0;

    double Tpmc = 0.0;
    double Efficiency = 0.0;

    size_t RunningTerminals = 0;
    size_t RunningTransactions = 0;
};

struct TRunDisplayData {
    TRunDisplayData(size_t threadCount, Clock::time_point ts)
        : Statistics(threadCount, ts)
    {}

    TAllStatistics Statistics;
    TRunStatusData StatusData;
};

} // namespace NYdb::NTPCC
