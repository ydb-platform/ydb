#include <ydb/library/schlab/schine/scheduler.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NSchLab {

struct TSchedulerTest {
    static void Check(const TScheduler& scheduler) {
        TVector<ui32> nonEmpty;
        ui32 active = 0;
        for (ui32 i = 0; i < scheduler.Cbs.size(); ++i) {
            const auto& cbs = scheduler.Cbs[i];
            if (!cbs.Jobs.Empty()) {
                nonEmpty.push_back(i);
            }
            active += cbs.State == CbsStateActive || cbs.State == CbsStateRunning;
            ui64 count = 0, cost = 0;
            for (const auto& job : cbs.Jobs) {
                ++count;
                cost += job.Cost;
                UNIT_ASSERT_VALUES_EQUAL(job.CbsIdx, i);
            }
            UNIT_ASSERT_VALUES_EQUAL(count, cbs.JobsSize);
            UNIT_ASSERT_VALUES_EQUAL(cost, cbs.JobsCost);
        }
        UNIT_ASSERT(nonEmpty == scheduler.NonEmptyCbs);
        UNIT_ASSERT_VALUES_EQUAL(active, scheduler.ActiveRunningCount);
    }
};

namespace {

void CheckOwnership() {
    TIntrusivePtr<TJob> a = new TJob;
    TIntrusivePtr<TJob> b = new TJob;
    a->Cost = 10;
    b->Cost = 20;
    {
        TCbs cbs;
        cbs.PushJob(a);
        cbs.PushJob(b);
        UNIT_ASSERT(a.RefCount() == 2 && b.RefCount() == 2);
        TCbs moved(std::move(cbs));
        UNIT_ASSERT(cbs.Jobs.Empty());
        UNIT_ASSERT_VALUES_EQUAL(cbs.JobsSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(cbs.JobsCost, 0);
        UNIT_ASSERT(moved.PeekJob() == a && moved.PeekTailJob() == b);
        auto popped = moved.PopJob();
        UNIT_ASSERT(popped == a && a->Empty() && a.RefCount() == 2);
        popped.Reset();
        UNIT_ASSERT(a.RefCount() == 1);
        moved.Reset(TCbs::InvalidIdx, 0, 0);
        UNIT_ASSERT(b->Empty() && b.RefCount() == 1);
        // Destructor must release and unlink both.
        moved.PushJob(a);
        moved.PushJob(b);
    }
    UNIT_ASSERT(a->Empty() && b->Empty() && a.RefCount() == 1 && b.RefCount() == 1);
    {
        TCbs cbs;
        TIntrusivePtr<TJob> sole = new TJob;
        sole->Cost = 1;
        cbs.PushJob(sole);
        sole.Reset(); // Queue is sole owner.
        auto popped = cbs.PopJob();
        UNIT_ASSERT(popped.RefCount() == 1 && popped->Empty());
    }
}

void CheckPool() {
    TIntrusivePtr<TJobPool> pool = new TJobPool;
    auto job = pool->Acquire();
    auto* address = job.Get();
    job->Payload = address;
    job->CbsIdx = 13;
    job->Id = 14;
    job->JobKind = 15;
    job->AccountCbsIdx = 16;
    job->Cost = 17;
    job->State = JobStateDone;
    job->SeqNo = 18;
    job->Deadline = 19;
    job->WeightedInverseCost = 20;
    job->IsNew = false;
    auto held = job;
    job.Reset();
    auto other = pool->Acquire();
    UNIT_ASSERT(other.Get() != address); // Still externally referenced.
    held.Reset();
    job = pool->Acquire();
    UNIT_ASSERT(job.Get() == address && job.RefCount() == 1 && job->Empty());
    UNIT_ASSERT(!job->Payload && job->CbsIdx == 0 && job->Id == 0 && job->JobKind == 0);
    UNIT_ASSERT(job->AccountCbsIdx == 65535 && job->Cost == 0 && job->State == JobStatePending);
    UNIT_ASSERT(job->SeqNo == 0 && job->Deadline == 0 && job->WeightedInverseCost == 0 && job->IsNew);
    job.Reset();
    other.Reset();
    for (ui32 i = 0; i < 10000; ++i) {
        auto reused = pool->Acquire();
        UNIT_ASSERT(reused.RefCount() == 1 && reused->Empty());
    }
    UNIT_ASSERT(pool->GetCreated() == 2); // No allocation after warmup.
    auto survivor = pool->Acquire();
    pool.Reset(); // Checked-out job keeps pool alive.
    survivor.Reset(); // Last release destroys both pool and cached jobs.
    TIntrusivePtr<TJob> afterScheduler;
    {
        TScheduler scheduler;
        scheduler.SetIsBinLogEnabled(false);
        scheduler.AddCbs(0, 0, TCbs{}, 1);
        afterScheduler = scheduler.CreateJob();
        afterScheduler->Cost = 10;
        UNIT_ASSERT(scheduler.AddJob(afterScheduler, 0, 0, 2));
        auto queueOnly = scheduler.CreateJob();
        queueOnly->Cost = 20;
        UNIT_ASSERT(scheduler.AddJob(queueOnly, 0, 0, 3));
        queueOnly.Reset();
        UNIT_ASSERT_VALUES_EQUAL(afterScheduler.RefCount(), 2);
    }
    UNIT_ASSERT_VALUES_EQUAL(afterScheduler.RefCount(), 1);
    UNIT_ASSERT(afterScheduler->Empty());
    afterScheduler.Reset();
}

void CheckBorrowed() {
    TCbs cbs;
    TIntrusivePtr<TJob> job = new TJob;
    job->Cost = 1;
    cbs.PushJob(job);
    const auto refs = job.RefCount();
    UNIT_ASSERT(cbs.PeekJobBorrowed() == job.Get() && job.RefCount() == refs);
    {
        auto owned = cbs.PeekJob();
        UNIT_ASSERT(owned == job && job.RefCount() == refs + 1);
    }
    UNIT_ASSERT(job.RefCount() == refs);
    {
        TScheduler scheduler;
        scheduler.SetIsBinLogEnabled(false);
        TCbs config;
        scheduler.AddCbs(0, 0, std::move(config), 1);
        auto queued = scheduler.CreateJob();
        queued->Cost = 1;
        scheduler.AddJob(queued, 0, 0, 2);
        queued.Reset(); // Only queue owns the task before selection.
        auto selected = scheduler.SelectJob(3);
        UNIT_ASSERT(selected && selected->RefCount() == 1);
        scheduler.CompleteJob(5, selected);
        auto reused = scheduler.CreateJob();
        UNIT_ASSERT(reused.Get() == selected && reused.RefCount() == 1);
        UNIT_ASSERT(reused->State == JobStatePending);
        reused->Cost = 1;
        scheduler.AddJob(reused, 0, 0, 6);
        auto raw = scheduler.SelectJob(7);
        UNIT_ASSERT(raw == reused.Get() && reused.RefCount() == 2);
        scheduler.CompleteJob(9, raw);
        UNIT_ASSERT(reused.RefCount() == 1 && reused->State == JobStateDone);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TForsetiOwnership) {
    Y_UNIT_TEST(QueueReferences) {
        CheckOwnership();
    }

    Y_UNIT_TEST(JobPoolReuseAndLifetime) {
        CheckPool();
    }

    Y_UNIT_TEST(BorrowedSelection) {
        CheckBorrowed();
    }

    Y_UNIT_TEST(MoveAssignNonEmptyQueues) {
        TCbs source, destination;
        TIntrusivePtr<TJob> a = new TJob, b = new TJob;
        a->Cost = 13;
        b->Cost = 27;
        source.PushJob(a);
        destination.PushJob(b);
        destination = std::move(source);
        UNIT_ASSERT(source.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(source.JobsSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(source.JobsCost, 0);
        UNIT_ASSERT_VALUES_EQUAL(destination.JobsSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(destination.JobsCost, 13);
        UNIT_ASSERT(destination.PeekJob() == a);
        UNIT_ASSERT(b->Empty());
        UNIT_ASSERT_VALUES_EQUAL(b.RefCount(), 1);
        source.PushJob(b);
        UNIT_ASSERT_VALUES_EQUAL(source.JobsSize, 1);
        UNIT_ASSERT_VALUES_EQUAL(source.JobsCost, 27);
    }

    Y_UNIT_TEST(EqualDeadlinesUseRegistrationOrder) {
        TScheduler scheduler;
        scheduler.SetIsBinLogEnabled(false);
        for (ui8 gate = 0; gate < 3; ++gate) {
            TCbs cbs;
            cbs.MaxBudget = cbs.CurBudget = 10;
            cbs.CbsDeadline = 100;
            scheduler.AddCbs(0, gate, std::move(cbs), 0);
        }
        // Insert in reverse order. Equal EDF deadlines still use registration order.
        for (ui8 gate : {2, 1, 0}) {
            auto job = scheduler.CreateJob();
            job->Cost = 1;
            UNIT_ASSERT(scheduler.AddJob(job, 0, gate, 1));
        }
        for (ui8 gate = 0; gate < 3; ++gate) {
            auto* job = scheduler.SelectJob(2 + gate * 2);
            UNIT_ASSERT(job);
            UNIT_ASSERT_VALUES_EQUAL(job->Deadline, 100);
            UNIT_ASSERT_VALUES_EQUAL(job->CbsIdx, scheduler.GetCbs(0, gate)->CbsIdx);
            scheduler.CompleteJob(3 + gate * 2, job);
        }
        UNIT_ASSERT(scheduler.IsEmpty());
    }

    Y_UNIT_TEST(GrowthDrainRefillAndAccounting) {
        for (bool logging : {false, true}) {
            TScheduler scheduler;
            scheduler.SetIsBinLogEnabled(logging);
            TReallyFastRng32 rng(100);
            ui64 now = 1'000'000'000;
            ui32 registered = 0;
            auto grow = [&] {
                for (ui32 i = 0; i < 40; ++i, ++registered) {
                    TCbs cbs;
                    cbs.Weight = 1 + registered % 10;
                    cbs.MaxBudget = registered % 10 == 0 ? 2'000'000 : 0;
                    scheduler.AddCbs(registered / 10, registered % 10, std::move(cbs), ++now);
                }
            };
            grow();
            THashSet<ui64> pending;
            auto record = [&] { TSchedulerTest::Check(scheduler); };
            auto complete = [&] {
                TSchedulerTest::Check(scheduler);
                auto job = scheduler.SelectJob(now);
                TSchedulerTest::Check(scheduler);
                if (!job) {
                    UNIT_ASSERT(scheduler.IsEmpty());
                    return;
                }
                UNIT_ASSERT_VALUES_EQUAL(pending.erase(job->Id), 1);
                // Include budget depletion before the virtual deadline and long idle intervals.
                now += (rng() % 4) ? job->Cost + 1 : 1;
                scheduler.CompleteJob(now, job);
                TSchedulerTest::Check(scheduler);
            };
            for (ui32 step = 0; step < 5000; ++step) {
                if (step == 200 || step == 500 || step == 800) {
                    grow(); // Reallocation while jobs are pending.
                }
                if (step % 100 == 0) {
                    while (!scheduler.IsEmpty()) {
                        complete();
                    }
                    now += 100'000'000;
                    complete();
                }
                const ui32 focus = (step / 100) % registered;
                for (ui32 n = 0; n < 1 + rng() % 3; ++n) {
                    const ui32 i = (step % 100 < 80) ? focus : rng() % registered;
                    auto job = scheduler.CreateJob();
                    job->Cost = 1000 * (1 + rng() % 100);
                    UNIT_ASSERT(scheduler.AddJob(job, i / 10, i % 10, ++now));
                    UNIT_ASSERT(pending.insert(job->Id).second);
                }
                if (step % 73 == 0) {
                    auto* cbs = scheduler.GetCbs(focus / 10, focus % 10);
                    cbs->Weight = 1 + rng() % 10;
                    scheduler.UpdateTotalWeight();
                    auto job = cbs->PeekTailJob();
                    if (job) {
                        const ui64 oldCost = job->Cost;
                        job->Cost += 1234;
                        scheduler.OnJobCostChange(cbs, job, ++now, oldCost);
                    }
                }
                complete();
                record();
            }
            while (!scheduler.IsEmpty()) {
                complete();
            }
            record();
            UNIT_ASSERT(!scheduler.SelectJob(now));
            UNIT_ASSERT(pending.empty());
        }
    }
}
} // namespace NKikimr::NSchLab
