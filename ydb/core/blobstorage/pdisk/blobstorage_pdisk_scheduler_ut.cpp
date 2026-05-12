#include "defs.h"

#include <ydb/library/schlab/schine/scheduler.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <utility>

namespace NKikimr {
namespace NPDisk {

namespace {

NSchLab::TCbs MakeCbs(const TString& name, ui64 weight, ui64 maxBudget) {
    NSchLab::TCbs cbs;
    cbs.CbsName = name;
    cbs.Weight = weight;
    cbs.MaxBudget = maxBudget;
    return cbs;
}

void AddJobs(NSchLab::TScheduler& scheduler, ui8 ownerId, ui8 gateId, ui32 count, ui64 cost,
        ui64& timeNs, THashSet<ui64>& jobIds) {
    NSchLab::TCbs* cbs = scheduler.GetCbs(ownerId, gateId);
    UNIT_ASSERT(cbs);
    for (ui32 i = 0; i < count; ++i) {
        auto job = scheduler.CreateJob();
        job->Cost = cost;
        scheduler.AddJob(cbs, job, ownerId, gateId, ++timeNs);
        jobIds.insert(job->Id);
    }
}

ui64 AddSingleJob(NSchLab::TScheduler& scheduler, NSchLab::TCbs* cbs, ui8 ownerId, ui8 gateId,
        ui64 cost, ui64& timeNs) {
    UNIT_ASSERT(cbs);
    auto job = scheduler.CreateJob();
    job->Cost = cost;
    scheduler.AddJob(cbs, job, ownerId, gateId, ++timeNs);
    return job->Id;
}

void DrainScheduler(NSchLab::TScheduler& scheduler, ui64& timeNs, THashSet<ui64>& jobIds) {
    while (!scheduler.IsEmpty()) {
        ++timeNs;
        auto job = scheduler.SelectJob(timeNs);
        UNIT_ASSERT(job);
        jobIds.insert(job->Id);
        timeNs += job->Cost;
        scheduler.CompleteJob(++timeNs, job);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TPDiskScheduler) {

    Y_UNIT_TEST(AllJobsAreRetrieved) {
        NSchLab::TScheduler scheduler;
        ui64 timeNs = 0;
        const ui8 ownerId = 1;
        const ui8 gateA = 1;
        const ui8 gateB = 2;

        scheduler.AddCbs(ownerId, gateA, MakeCbs("A", 1, 10), ++timeNs);
        scheduler.AddCbs(ownerId, gateB, MakeCbs("B", 1, 10), ++timeNs);
        scheduler.UpdateTotalWeight();

        THashSet<ui64> expected;
        AddJobs(scheduler, ownerId, gateA, 64, 10, timeNs, expected);
        AddJobs(scheduler, ownerId, gateB, 64, 10, timeNs, expected);

        THashSet<ui64> seen;
        DrainScheduler(scheduler, timeNs, seen);

        UNIT_ASSERT_VALUES_EQUAL(expected.size(), seen.size());
        for (ui64 jobId : expected) {
            UNIT_ASSERT(seen.contains(jobId));
        }
    }

    Y_UNIT_TEST(HigherWeightGetsMoreThroughput) {
        NSchLab::TScheduler scheduler;
        ui64 timeNs = 0;
        const ui8 ownerId = 1;
        const ui8 highGate = 1;
        const ui8 lowGate = 2;

        scheduler.AddCbs(ownerId, highGate, MakeCbs("High", 2, 100), ++timeNs);
        scheduler.AddCbs(ownerId, lowGate, MakeCbs("Low", 1, 100), ++timeNs);
        scheduler.UpdateTotalWeight();

        NSchLab::TCbs* highCbs = scheduler.GetCbs(ownerId, highGate);
        NSchLab::TCbs* lowCbs = scheduler.GetCbs(ownerId, lowGate);
        UNIT_ASSERT(highCbs);
        UNIT_ASSERT(lowCbs);

        const ui32 backlog = 4096;
        const ui64 jobCost = 10;
        for (ui32 i = 0; i < backlog; ++i) {
            auto highJob = scheduler.CreateJob();
            highJob->Cost = jobCost;
            scheduler.AddJob(highCbs, highJob, ownerId, highGate, ++timeNs);

            auto lowJob = scheduler.CreateJob();
            lowJob->Cost = jobCost;
            scheduler.AddJob(lowCbs, lowJob, ownerId, lowGate, ++timeNs);
        }

        ui32 highCount = 0;
        ui32 lowCount = 0;
        const ui32 iterations = 1024;
        const ui64 highCbsIdx = highCbs->CbsIdx;

        for (ui32 i = 0; i < iterations; ++i) {
            ++timeNs;
            auto job = scheduler.SelectJob(timeNs);
            UNIT_ASSERT(job);
            if (job->CbsIdx == highCbsIdx) {
                ++highCount;
            } else {
                ++lowCount;
            }
            timeNs += job->Cost;
            scheduler.CompleteJob(++timeNs, job);
        }

        Cout << highCount << " vs " << lowCount << Endl;
        UNIT_ASSERT_GT(highCount, lowCount);
        UNIT_ASSERT_GT(lowCount, 0u);
    }

    Y_UNIT_TEST(MaxBudgetDoesNotChangeSparseLatencyUnderHeavyContention) {
        const auto runPeriodicLatency = [](ui64 secondMaxBudget, ui32 secondPeriod) -> std::pair<ui64, ui64> {
            NSchLab::TScheduler scheduler;
            ui64 timeNs = 0;
            const ui8 ownerId = 1;
            const ui8 busyGate = 1;
            const ui8 secondGate = 2;
            const ui64 jobCost = 10;
            const ui32 secondRequests = 256;

            scheduler.AddCbs(ownerId, busyGate, MakeCbs("Busy", 1, 10), ++timeNs);
            scheduler.AddCbs(ownerId, secondGate, MakeCbs("Second", 1, secondMaxBudget), ++timeNs);
            scheduler.UpdateTotalWeight();

            NSchLab::TCbs* busyCbs = scheduler.GetCbs(ownerId, busyGate);
            NSchLab::TCbs* secondCbs = scheduler.GetCbs(ownerId, secondGate);
            UNIT_ASSERT(busyCbs);
            UNIT_ASSERT(secondCbs);

            for (ui32 i = 0; i < 512; ++i) {
                AddSingleJob(scheduler, busyCbs, ownerId, busyGate, jobCost, timeNs);
            }

            TDeque<ui64> enqueuedAt;
            ui32 secondSent = 0;
            ui32 secondDone = 0;
            ui32 completed = 0;
            ui64 latencySum = 0;
            ui64 maxLatency = 0;

            while (secondDone < secondRequests) {
                if (secondSent < secondRequests && completed % secondPeriod == 0) {
                    const ui64 enqueueTimeNs = timeNs + 1;
                    AddSingleJob(scheduler, secondCbs, ownerId, secondGate, jobCost, timeNs);
                    enqueuedAt.push_back(enqueueTimeNs);
                    ++secondSent;
                }

                AddSingleJob(scheduler, busyCbs, ownerId, busyGate, jobCost, timeNs);
                ++timeNs;
                auto job = scheduler.SelectJob(timeNs);
                UNIT_ASSERT(job);
                const bool isSecondJob = job->CbsIdx == secondCbs->CbsIdx;
                timeNs += job->Cost;
                scheduler.CompleteJob(++timeNs, job);

                if (isSecondJob) {
                    UNIT_ASSERT(!enqueuedAt.empty());
                    const ui64 latency = timeNs - enqueuedAt.front();
                    enqueuedAt.pop_front();
                    latencySum += latency;
                    maxLatency = Max(maxLatency, latency);
                    ++secondDone;
                }

                ++completed;
            }

            return {latencySum / secondRequests, maxLatency};
        };

        const ui32 secondPeriod = 10;
        auto [lowAvg, lowMax] = runPeriodicLatency(10, secondPeriod);
        auto [highAvg, highMax] = runPeriodicLatency(100, secondPeriod);

        Cout << "period# " << secondPeriod
            << " lowAvg# " << lowAvg
            << " highAvg# " << highAvg
            << " lowMax# " << lowMax
            << " highMax# " << highMax << Endl;

        UNIT_ASSERT_VALUES_EQUAL(lowAvg, highAvg);
        UNIT_ASSERT_VALUES_EQUAL(lowMax, highMax);
    }

    Y_UNIT_TEST(SparseCbsIsServedImmediatelyUnderHeavyBusyLoad) {
        NSchLab::TScheduler scheduler;
        ui64 timeNs = 0;
        const ui8 busyOwnerId = 1;
        const ui8 sparseOwnerId = 1;
        const ui8 busyGate = 1;
        const ui8 sparseGate = 2;

        scheduler.AddCbs(busyOwnerId, busyGate, MakeCbs("Busy", 1, 100), ++timeNs);
        scheduler.AddCbs(sparseOwnerId, sparseGate, MakeCbs("Sparse", 1, 100), ++timeNs);
        scheduler.UpdateTotalWeight();

        NSchLab::TCbs* busyCbs = scheduler.GetCbs(busyOwnerId, busyGate);
        NSchLab::TCbs* sparseCbs = scheduler.GetCbs(sparseOwnerId, sparseGate);
        UNIT_ASSERT(busyCbs);
        UNIT_ASSERT(sparseCbs);

        const ui64 jobCost = 10;
        const ui32 initialBusyBacklog = 512;
        const ui32 warmupBusyCompletions = 128;
        const ui32 busyPerSparse = 10;
        const ui32 sparseCycles = 200;

        for (ui32 i = 0; i < initialBusyBacklog; ++i) {
            AddSingleJob(scheduler, busyCbs, busyOwnerId, busyGate, jobCost, timeNs);
        }

        for (ui32 i = 0; i < warmupBusyCompletions; ++i) {
            ++timeNs;
            auto busyJob = scheduler.SelectJob(timeNs);
            UNIT_ASSERT(busyJob);
            UNIT_ASSERT_VALUES_EQUAL(busyJob->CbsIdx, busyCbs->CbsIdx);
            timeNs += busyJob->Cost;
            scheduler.CompleteJob(++timeNs, busyJob);
        }

        for (ui32 cycle = 0; cycle < sparseCycles; ++cycle) {
            for (ui32 i = 0; i < busyPerSparse; ++i) {
                AddSingleJob(scheduler, busyCbs, busyOwnerId, busyGate, jobCost, timeNs);
            }

            const ui64 sparseJobId = AddSingleJob(scheduler, sparseCbs, sparseOwnerId, sparseGate, jobCost, timeNs);

            ++timeNs;
            auto selected = scheduler.SelectJob(timeNs);
            UNIT_ASSERT(selected);
            UNIT_ASSERT_VALUES_EQUAL(selected->Id, sparseJobId);
            UNIT_ASSERT_VALUES_EQUAL(selected->CbsIdx, sparseCbs->CbsIdx);
            timeNs += selected->Cost;
            scheduler.CompleteJob(++timeNs, selected);

            for (ui32 i = 0; i < busyPerSparse; ++i) {
                ++timeNs;
                auto busyJob = scheduler.SelectJob(timeNs);
                UNIT_ASSERT(busyJob);
                UNIT_ASSERT_VALUES_EQUAL(busyJob->CbsIdx, busyCbs->CbsIdx);
                timeNs += busyJob->Cost;
                scheduler.CompleteJob(++timeNs, busyJob);
            }
        }
    }
}

} // NPDisk
} // NKikimr
