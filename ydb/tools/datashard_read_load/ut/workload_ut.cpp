#include <ydb/tools/datashard_read_load/workload.h>
#include <ydb/tools/datashard_read_load/stats.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <algorithm>
#include <stdexcept>

namespace NDataShardReadLoad {

TEST(ReportClock, DelayedCollectionDoesNotOverlapIntervalsOrTriggerCatchUpReports) {
    TReportClock clock;
    EXPECT_EQ(clock.NextUs(), 1000000);
    EXPECT_EQ(clock.Record(9005569), 9005569);
    EXPECT_EQ(clock.NextUs(), 10005569);

    // Collection starts when due, but blocks until 11.351327s. That completion
    // time anchors both this interval and the next deadline, not its start time.
    EXPECT_EQ(clock.Record(11351327), 2345758);
    EXPECT_EQ(clock.NextUs(), 12351327);
    EXPECT_EQ(clock.Record(12379029), 1027702);
    EXPECT_EQ(clock.NextUs(), 13379029);
    EXPECT_GT(clock.NextUs(), 12392351); // no spurious report 13ms later
}

TEST(ReportClock, IntervalsPartitionElapsedTimeIncludingTimeSpentWritingReports) {
    TReportClock clock;
    int64_t sum = 0;
    for (int64_t collectedUs : {1100000, 2450000, 3500000, 6000000}) {
        // Time spent writing earlier reports is part of the next interval;
        // phase records do not reset the interval clock.
        sum += clock.Record(collectedUs);
        EXPECT_EQ(sum, collectedUs);
        EXPECT_EQ(clock.NextUs(), collectedUs + 1000000);
    }
}

TEST(ArrivalClock, CountsLateArrivalsWithoutCatchUp) {
    TArrivalClock clock(100, 0, 1000000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_EQ(clock.NextUs(), 10000);
    EXPECT_EQ(clock.Poll(95000), 9u); // caller skips eight and launches one
    EXPECT_EQ(clock.LastDueUs(), 90000);
    EXPECT_EQ(clock.NextUs(), 100000);
    EXPECT_EQ(clock.Poll(95000), 0u);
    EXPECT_EQ(clock.Poll(1000000), 90u);
    EXPECT_EQ(clock.Poll(2000000), 0u);
}

TEST(ArrivalClock, ZeroRateAndPhaseBoundaries) {
    TArrivalClock idle(0, 1000000, 2000000);
    EXPECT_EQ(idle.Poll(1500000), 0u);
    EXPECT_EQ(idle.NextUs(), 2000000);
    TArrivalClock before(1, 0, 1000000), after(1, 1000000, 2000000);
    EXPECT_EQ(before.Poll(1000000), 1u);
    EXPECT_EQ(after.Poll(999999), 0u);
    EXPECT_EQ(after.Poll(1000000), 1u);
    EXPECT_EQ(after.Poll(2000000), 0u);
}

TEST(ArrivalClock, FractionalMicrosecondIntervalsRoundUp) {
    TArrivalClock clock(3, 0, 1000000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_EQ(clock.NextUs(), 333334);
    EXPECT_EQ(clock.Poll(333333), 0u);
    EXPECT_EQ(clock.Poll(333334), 1u);
    EXPECT_EQ(clock.LastDueUs(), 333334);
    EXPECT_EQ(clock.Poll(1000000), 1u);
}

TEST(ArrivalClock, BatchingPreservesRateDespiteWakeupJitter) {
    TArrivalClock clock(50000, 0, 1000000);
    uint64_t scheduled = 0, dispatched = 0;
    // An 80us wakeup interval cannot deliver 50k/s with one dispatch per wakeup.
    // Batching elapsed arrivals should deliver the configured rate exactly.
    for (int64_t now = 0; now < 1000000; now += 80) {
        const auto batch = clock.PollBatch(now, 64);
        scheduled += batch.Scheduled;
        dispatched += batch.Dispatch;
        for (uint64_t i = 0; i < batch.Dispatch; ++i) {
            EXPECT_LE(clock.DueUs(batch.FirstIndex + i), now);
        }
    }
    const auto last = clock.PollBatch(999999, 64);
    EXPECT_EQ(scheduled + last.Scheduled, 50000u);
    EXPECT_EQ(dispatched + last.Dispatch, 50000u);
    EXPECT_EQ(clock.PollBatch(1000000, 64).Scheduled, 0u);
}

TEST(ArrivalClock, BatchesAreBoundedAndDoNotRetainMissedArrivals) {
    TArrivalClock clock(50000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 64).Dispatch, 1u);
    const auto stalled = clock.PollBatch(100000, 64);
    EXPECT_EQ(stalled.Scheduled, 5000u);
    EXPECT_EQ(stalled.Dispatch, 64u);
    EXPECT_EQ(clock.DueUs(stalled.FirstIndex), 98740);
    EXPECT_EQ(clock.PollBatch(100000, 64).Dispatch, 0u);
    const auto next = clock.PollBatch(100080, 64);
    EXPECT_EQ(next.Scheduled, 4u);
    EXPECT_EQ(next.Dispatch, 4u);
    EXPECT_EQ(clock.DueUs(next.FirstIndex), 100020);
}

TEST(ArrivalClock, NoBatchLaunchesAtOrAfterPhaseEnd) {
    TArrivalClock clock(10000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 64).Dispatch, 1u);
    const auto end = clock.PollBatch(1000000, 64);
    EXPECT_EQ(end.Scheduled, 9999u);
    EXPECT_EQ(end.Dispatch, 0u);
    EXPECT_EQ(clock.PollBatch(1001000, 64).Dispatch, 0u);
}

TEST(ArrivalClock, BatchLimitOnePreservesOriginalPacing) {
    TArrivalClock clock(50000, 0, 1000000);
    EXPECT_EQ(clock.PollBatch(0, 1).Dispatch, 1u);
    const auto late = clock.PollBatch(80, 1);
    EXPECT_EQ(late.Scheduled, 4u);
    EXPECT_EQ(late.Dispatch, 1u);
    EXPECT_EQ(clock.DueUs(late.FirstIndex), 80);
}

TEST(ArrivalClock, ThreadPartitionsPreserveTheOriginalSchedule) {
    for (uint64_t rate : {1u, 3u, 17u, 50000u}) {
        for (uint64_t partitions : {2u, 3u, 8u}) {
            TArrivalClock original(rate, 10000, 135000);
            std::vector<TArrivalClock> workers;
            for (uint64_t i = 0; i < partitions; ++i) {
                workers.emplace_back(rate, 10000, 135000, i, partitions);
            }
            for (int64_t now = 9900; now <= 135100; now += 137) {
                const auto batch = original.PollBatch(now, 64);
                std::vector<int64_t> expected, actual;
                for (uint64_t i = 0; i < batch.Dispatch; ++i) {
                    expected.push_back(original.DueUs(batch.FirstIndex + i));
                }
                uint64_t scheduled = 0;
                int64_t next = 135000;
                for (auto& clock : workers) {
                    const auto part = clock.PollBatch(now, 64);
                    scheduled += part.Scheduled;
                    next = std::min(next, clock.NextUs());
                    for (uint64_t i = 0; i < part.Dispatch; ++i) {
                        actual.push_back(clock.DueUs(part.FirstIndex + i));
                    }
                }
                std::sort(actual.begin(), actual.end());
                EXPECT_EQ(actual, expected);
                EXPECT_EQ(scheduled, batch.Scheduled);
                EXPECT_EQ(next, original.NextUs());
            }
        }
    }
}

TEST(ArrivalClock, PartitionedBatchesSkipOnlyTheirOwnArrivals) {
    TArrivalClock even(1000, 0, 1000000, 0, 2);
    TArrivalClock odd(1000, 0, 1000000, 1, 2);
    EXPECT_EQ(even.PollBatch(0, 2).Dispatch, 1u);
    EXPECT_EQ(odd.PollBatch(0, 2).Dispatch, 0u);
    const auto a = even.PollBatch(10000, 2);
    const auto b = odd.PollBatch(10000, 2);
    EXPECT_EQ(a.Scheduled, 5u);
    EXPECT_EQ(b.Scheduled, 5u);
    EXPECT_EQ(a.Dispatch, 2u);
    EXPECT_EQ(b.Dispatch, 2u);
    EXPECT_EQ(even.DueUs(a.FirstIndex), 8000);
    EXPECT_EQ(odd.DueUs(b.FirstIndex), 7000);
    EXPECT_EQ(even.LastDueUs(), 10000);
    EXPECT_EQ(odd.LastDueUs(), 9000);
    EXPECT_EQ(even.NextUs(), 12000);
    EXPECT_EQ(odd.NextUs(), 11000);
    EXPECT_EQ(even.PollBatch(10000, 2).Scheduled, 0u);
    EXPECT_EQ(odd.PollBatch(10000, 2).Scheduled, 0u);
    EXPECT_THROW(TArrivalClock(1, 0, 1000000, 0, 0), std::invalid_argument);
    EXPECT_THROW(TArrivalClock(1, 0, 1000000, 2, 2), std::invalid_argument);
}

TEST(Capacity, ThreadPartitionsKeepTotalCapsIncludingSmallAndDisabledStreams) {
    TOptions options;
    options.Threads = 4;
    options.Caps = {7, 2, 0};
    EXPECT_EQ(WorkerCaps(options, 0), (std::array<uint64_t, 3>{2, 1, 0}));
    EXPECT_EQ(WorkerCaps(options, 1), (std::array<uint64_t, 3>{2, 1, 0}));
    EXPECT_EQ(WorkerCaps(options, 2), (std::array<uint64_t, 3>{2, 0, 0}));
    EXPECT_EQ(WorkerCaps(options, 3), (std::array<uint64_t, 3>{1, 0, 0}));
    std::array<uint64_t, 3> sum{};
    for (uint64_t i = 0; i < options.Threads; ++i) {
        const auto caps = WorkerCaps(options, i);
        for (size_t p = 0; p < PopulationCount; ++p) {
            sum[p] += caps[p];
        }
    }
    EXPECT_EQ(sum, options.Caps);
    EXPECT_THROW(WorkerCaps(options, options.Threads), std::invalid_argument);
}

TEST(Capacity, PopulationsAndPhaseTransitionsAreIndependent) {
    TCapacity capacity({2, 1, 1});
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_FALSE(capacity.Acquire(Long));
    EXPECT_TRUE(capacity.Acquire(Short));
    EXPECT_TRUE(capacity.Acquire(Probe));
    TArrivalClock nextPhase(10, 1000000, 2000000);
    EXPECT_EQ(nextPhase.Poll(1000000), 1u);
    EXPECT_FALSE(capacity.Acquire(Long));
    capacity.Release(Long);
    EXPECT_TRUE(capacity.Acquire(Long));
    capacity.Release(Probe);
    EXPECT_EQ(capacity.Outstanding(Probe), 0u);
    EXPECT_THROW(capacity.Release(Probe), std::logic_error);
}

TEST(Capacity, StopsLaunchingAndDrainsOutstandingWork) {
    TCapacity capacity({1, 1, 1});
    TArrivalClock clock(100, 0, 10000);
    EXPECT_EQ(clock.Poll(0), 1u);
    EXPECT_TRUE(capacity.Acquire(Long));
    EXPECT_EQ(clock.Poll(10000), 0u);
    EXPECT_EQ(capacity.Outstanding(Long), 1u);
    capacity.Release(Long);
    EXPECT_EQ(capacity.Outstanding(Long), 0u);
}

TEST(Statistics, AggregationMergesSamplesAndPreservesAccounting) {
    TStats slow, fast, total;
    slow.Scheduled = 101;
    slow.Launched = 100;
    slow.SkippedScheduler = 1;
    slow.Completed = 99;
    slow.Statuses["CANCELLED"] = 99;
    slow.Latency.RecordValues(1000000, 99);
    fast.Scheduled = 10003;
    fast.Launched = 10000;
    fast.SkippedCapacity = 3;
    fast.Completed = 9998;
    fast.Statuses["SUCCESS"] = 9998;
    fast.SuccessfulRows = 9998;
    fast.Latency.RecordValues(1000, 9998);
    fast.SuccessLatency.RecordValues(1000, 9998);
    total.Add(slow);
    total.Add(fast);
    EXPECT_EQ(total.Scheduled, total.Launched + total.SkippedScheduler + total.SkippedCapacity);
    EXPECT_EQ(total.Launched - total.Completed, 3u);
    EXPECT_EQ(total.Statuses["CANCELLED"], 99u);
    EXPECT_EQ(total.Statuses["SUCCESS"], 9998u);
    EXPECT_EQ(total.SuccessfulRows, 9998u);
    // Percentiles come from merged samples, not averaged worker percentiles.
    EXPECT_LT(total.Latency.GetValueAtPercentile(99), 2000);
    EXPECT_GE(total.Latency.GetMax(), 1000000);
    EXPECT_LT(total.SuccessLatency.GetMax(), 2000);
}

TOptions Parse(std::initializer_list<const char*> arguments) {
    std::vector<char*> argv;
    for (const auto* argument : arguments) {
        argv.push_back(const_cast<char*>(argument));
    }
    return ParseOptions(argv.size(), argv.data());
}

TEST(Options, RequiresExplicitLoadConfiguration) {
    EXPECT_THROW(Parse({"load", "run", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load"}), std::runtime_error);
    auto options = Parse({"load", "run", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load",
        "--long-inflight", "1", "--short-inflight", "0", "--probe-inflight", "1", "--phase", "baseline:2s:10:0"});
    EXPECT_EQ(options.Phases.size(), 1u);
    EXPECT_EQ(options.Phases[0].DurationUs, 2000000);
    EXPECT_EQ(options.Phases[0].Rates, (std::array<uint64_t, 3>{10, 0, 1}));
    EXPECT_EQ(options.DispatchBatch, 64u);
    EXPECT_EQ(options.Threads, 1u);
}

TEST(Options, ThreadsDoNotMultiplyRatesOrCaps) {
    auto options = Parse({"load", "run", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load",
        "--threads", "4", "--long-inflight", "7", "--short-inflight", "0", "--probe-inflight", "1",
        "--phase", "burst:2s:25000:0"});
    EXPECT_EQ(options.Threads, 4u);
    EXPECT_EQ(options.Caps, (std::array<uint64_t, 3>{7, 0, 1}));
    EXPECT_EQ(options.Phases[0].Rates, (std::array<uint64_t, 3>{25000, 0, 1}));
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--threads", "0"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--threads", "65"}), std::runtime_error);
}

TEST(Options, RejectsInvalidPhaseAndDeadlines) {
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--phase", "x:1s:-1:0"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--cancel-after", "60s"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/`bad"}), std::runtime_error);
    EXPECT_THROW(Parse({"load", "init", "-e", "localhost:2136", "-d", "/Root", "--table", "/Root/load", "--dispatch-batch", "0"}), std::runtime_error);
}

} // namespace NDataShardReadLoad
