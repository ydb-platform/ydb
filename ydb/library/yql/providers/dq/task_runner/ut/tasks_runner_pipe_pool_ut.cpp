#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <util/system/event.h>
#include <util/system/thread.h>

#include <deque>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>

namespace NYql::NTaskRunnerProxy {
namespace {

struct TFakeProcess
{
    int Id = 0;
    TString Key;
    bool Alive = true;
};

class TManualScheduler
{
public:
    bool AcceptCallbacks = true;

    bool Schedule(std::function<void()> callback)
    {
        TGuard<TMutex> guard(Mutex_);
        if (!AcceptCallbacks) {
            return false;
        }
        Callbacks_.push_back(std::move(callback));
        return true;
    }

    void RunNext()
    {
        std::function<void()> callback;
        {
            TGuard<TMutex> guard(Mutex_);
            UNIT_ASSERT(!Callbacks_.empty());
            callback = std::move(Callbacks_.front());
            Callbacks_.pop_front();
        }
        callback();
    }

    void RunAll()
    {
        int callbackCount = 0;
        while (Size() > 0) {
            UNIT_ASSERT_C(++callbackCount < 100, "Scheduler callback backlog does not converge");
            RunNext();
        }
    }

    void Clear()
    {
        TGuard<TMutex> guard(Mutex_);
        Callbacks_.clear();
    }

    int Size() const
    {
        TGuard<TMutex> guard(Mutex_);
        return static_cast<int>(Callbacks_.size());
    }

private:
    mutable TMutex Mutex_;
    std::deque<std::function<void()>> Callbacks_;
};

class TFixture
{
public:
    using TPool = TPipeProcessPool<TFakeProcess>;

    TManualScheduler Scheduler;
    TPool Pool;

    int NextProcessId = 1;
    bool CleanupSucceeds = true;
    bool FailNextSpawn = false;
    int FailSpawnCount = 0;
    bool ThrowNextSpawn = false;
    bool ProcessOnSpawnError = false;
    std::function<void(const TString&)> OnSpawn;
    std::function<void()> OnCleanup;
    std::vector<std::shared_ptr<TFakeProcess>> Processes;
    std::vector<std::shared_ptr<TFakeProcess>> ActiveProcesses;
    std::vector<int> CleanedProcessIds;

    explicit TFixture(int maxProcesses)
        : Pool(
            maxProcesses,
            [&] (auto callback) { return Scheduler.Schedule(std::move(callback)); },
            [&] (const auto& process) {
                if (OnCleanup) {
                    OnCleanup();
                }
                TGuard<TMutex> guard(Mutex_);
                CleanedProcessIds.push_back(process->Id);
                if (!CleanupSucceeds) {
                    return false;
                }
                process->Alive = false;
                return true;
            },
            [] (const auto& process) { return process->Alive; })
    { }

    TPool::TRequest MakeRequest(const TString& key)
    {
        return {
            .Key = key,
            .Spawn = [&, key] {
                bool throwSpawn = false;
                bool failSpawn = false;
                bool processOnSpawnError = false;
                std::shared_ptr<TFakeProcess> process;
                std::function<void(const TString&)> onSpawn;
                {
                    TGuard<TMutex> guard(Mutex_);
                    throwSpawn = std::exchange(ThrowNextSpawn, false);
                    failSpawn = std::exchange(FailNextSpawn, false);
                    if (FailSpawnCount > 0) {
                        --FailSpawnCount;
                        failSpawn = true;
                    }
                    processOnSpawnError = ProcessOnSpawnError;
                    if (!throwSpawn) {
                        process = std::make_shared<TFakeProcess>(TFakeProcess{
                            .Id = NextProcessId++,
                            .Key = key,
                            .Alive = !failSpawn || processOnSpawnError,
                        });
                        Processes.push_back(process);
                        onSpawn = std::exchange(OnSpawn, {});
                    }
                }

                if (throwSpawn) {
                    throw std::runtime_error("Spawn threw");
                }

                if (onSpawn) {
                    onSpawn(key);
                }

                if (failSpawn) {
                    return TPool::TSpawnResult{
                        .Process = processOnSpawnError ? process : nullptr,
                        .Error = std::make_exception_ptr(std::runtime_error("Spawn failed")),
                    };
                }

                return TPool::TSpawnResult{.Process = std::move(process)};
            },
        };
    }

    void MarkActive(const std::shared_ptr<TFakeProcess>& process)
    {
        TGuard<TMutex> guard(Mutex_);
        ActiveProcesses.push_back(process);
    }

    int CountAliveUnreturned() const
    {
        TGuard<TMutex> guard(Mutex_);
        int result = 0;
        for (const auto& process : Processes) {
            if (process->Alive && Find(ActiveProcesses, process) == ActiveProcesses.end()) {
                ++result;
            }
        }
        return result;
    }

private:
    mutable TMutex Mutex_;
};

void AssertWithinBudget(const TFixture::TPool::TSnapshot& snapshot, int maxProcesses)
{
    UNIT_ASSERT_C(
        snapshot.Idle + snapshot.Retiring + snapshot.Warming <= maxProcesses,
        TStringBuilder()
            << "Pool-owned process limit exceeded: "
            << snapshot.Idle << " idle + "
            << snapshot.Retiring << " retiring + "
            << snapshot.Warming << " warming");
}

Y_UNIT_TEST_SUITE(TPipeProcessPoolTest) {

    Y_UNIT_TEST(DoesNotRefillOverRetiringProcesses)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        fixture.CleanupSucceeds = false;

        auto activeProcess = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        fixture.MarkActive(activeProcess);
        UNIT_ASSERT_VALUES_EQUAL(activeProcess->Key, "B");

        fixture.Scheduler.RunNext();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_LE(fixture.CountAliveUnreturned(), 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 2);
        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 2);
    }

    Y_UNIT_TEST(QueuedRefillCannotBypassBudgetAfterKeyChange)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));

        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        fixture.MarkActive(activeA);
        fixture.MarkActive(activeB);
        fixture.OnSpawn = [&] (const TString&) {
            UNIT_ASSERT_LE(fixture.CountAliveUnreturned(), 2);
        };
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 2);

        fixture.Scheduler.RunNext();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 1);
        UNIT_ASSERT_LE(fixture.CountAliveUnreturned(), 2);
        UNIT_ASSERT_VALUES_EQUAL(activeA->Key, "A");
        UNIT_ASSERT_VALUES_EQUAL(activeB->Key, "B");
    }

    Y_UNIT_TEST(InFlightSpawnUsesReservationAcrossKeyChange)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));

        std::shared_ptr<TFakeProcess> activeB;
        fixture.OnSpawn = [&] (const TString& key) {
            UNIT_ASSERT_VALUES_EQUAL(key, "A");
            activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));
            AssertWithinBudget(fixture.Pool.GetSnapshot(), 2);
        };
        fixture.Scheduler.RunNext();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 2);
        UNIT_ASSERT_VALUES_EQUAL(activeA->Key, "A");
        UNIT_ASSERT_VALUES_EQUAL(activeB->Key, "B");
    }

    Y_UNIT_TEST(CleanupRestartsRefillForLatestKey)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("exeA,mem0"));
        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("exeB,mem1"));

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 0);
        UNIT_ASSERT_VALUES_EQUAL(activeB->Key, "exeB,mem1");
        for (const auto& process : fixture.Processes) {
            if (process->Alive && process != activeB) {
                UNIT_ASSERT_VALUES_EQUAL(process->Key, "exeB,mem1");
            }
        }
    }

    Y_UNIT_TEST(LatestKeyWinsAcrossAtoBtoA)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("exeA,mem0"));
        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("exeB,mem1"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("exeA,mem2"));

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 2);
        UNIT_ASSERT_VALUES_EQUAL(activeB->Key, "exeB,mem1");
        UNIT_ASSERT_VALUES_EQUAL(activeA->Key, "exeA,mem2");
        for (const auto& process : fixture.Processes) {
            if (process->Alive && process != activeA && process != activeB) {
                UNIT_ASSERT_VALUES_EQUAL(process->Key, "exeA,mem2");
            }
        }
    }

    Y_UNIT_TEST(ActiveProcessIsOutsidePoolBudgetAndIsNotCleaned)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        const int activeId = activeA->Id;

        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        fixture.Scheduler.RunAll();

        UNIT_ASSERT(activeA->Alive);
        UNIT_ASSERT(activeB->Alive);
        UNIT_ASSERT(Find(fixture.CleanedProcessIds, activeId) == fixture.CleanedProcessIds.end());
        AssertWithinBudget(fixture.Pool.GetSnapshot(), 1);
    }

    Y_UNIT_TEST(SpawnErrorReleasesReservationWithoutRetryLoop)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.FailNextSpawn = true;

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Warming, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
        UNIT_ASSERT(active->Alive);
    }

    Y_UNIT_TEST(SpawnExceptionReleasesReservationWithoutRetryLoop)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.ThrowNextSpawn = true;

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Warming, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
        UNIT_ASSERT(active->Alive);
    }

    Y_UNIT_TEST(StaleRefillFailureDoesNotBlockLatestKey)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.FailNextSpawn = true;
        fixture.OnSpawn = [&] (const TString& key) {
            UNIT_ASSERT_VALUES_EQUAL(key, "A");
            fixture.MarkActive(fixture.Pool.Acquire(fixture.MakeRequest("B")));
        };

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 0);
        UNIT_ASSERT(activeA->Alive);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.back()->Key, "B");
    }

    Y_UNIT_TEST(FailedDemandCleanupQuarantinesAndBlocksFurtherSpawns)
    {
        for (int maxProcesses : {0, 1}) {
            TFixture fixture(maxProcesses);
            fixture.CleanupSucceeds = false;
            fixture.FailNextSpawn = true;
            fixture.ProcessOnSpawnError = true;

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                fixture.Pool.Acquire(fixture.MakeRequest("A")),
                std::runtime_error,
                "Spawn failed");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);

            UNIT_ASSERT_EXCEPTION_CONTAINS(
                fixture.Pool.Acquire(fixture.MakeRequest("B")),
                std::runtime_error,
                "Spawn failed");
            fixture.Scheduler.RunAll();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);

            fixture.Scheduler.RunAll();
            UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 2);
        }
    }

    Y_UNIT_TEST(MatchingIdleCanBeAcquiredWhileDemandProcessIsQuarantined)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.MarkActive(active);

        TManualEvent spawnEntered;
        TManualEvent allowSpawn;
        fixture.OnSpawn = [&] (const TString& key) {
            UNIT_ASSERT_VALUES_EQUAL(key, "A");
            spawnEntered.Signal();
            allowSpawn.WaitI();
        };
        TThread refillThread([&] {
            fixture.Scheduler.RunNext();
        });
        refillThread.Start();
        bool joined = false;
        Y_DEFER {
            if (!joined) {
                allowSpawn.Signal();
                refillThread.Join();
            }
        };

        UNIT_ASSERT(spawnEntered.WaitT(TDuration::Seconds(5)));
        fixture.CleanupSucceeds = false;
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("A")),
            std::runtime_error,
            "Spawn failed");

        allowSpawn.Signal();
        refillThread.Join();
        joined = true;

        auto idle = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.MarkActive(idle);
        UNIT_ASSERT(idle->Alive);
        UNIT_ASSERT_VALUES_EQUAL(idle->Key, "A");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 3);

        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);
    }

    Y_UNIT_TEST(SuccessfulAcquireTriggeredCleanupResumesRefill)
    {
        TFixture fixture(1);
        fixture.CleanupSucceeds = false;
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("A")),
            std::runtime_error,
            "Spawn failed");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);

        fixture.CleanupSucceeds = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("B")),
            std::runtime_error,
            "Spawn failed");
        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Quarantined, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 2);

        auto active = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        UNIT_ASSERT_VALUES_EQUAL(active->Key, "B");
    }

    Y_UNIT_TEST(SuccessfulInitialDemandCleanupDoesNotQuarantine)
    {
        TFixture fixture(0);
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("A")),
            std::runtime_error,
            "Spawn failed");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);

        auto active = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        UNIT_ASSERT_VALUES_EQUAL(active->Key, "B");
    }

    Y_UNIT_TEST(ConcurrentPartialDemandFailuresRemainOwned)
    {
        TFixture fixture(0);
        fixture.CleanupSucceeds = false;
        fixture.FailSpawnCount = 2;
        fixture.ProcessOnSpawnError = true;

        TManualEvent firstSpawnEntered;
        TManualEvent allowFirstSpawn;
        fixture.OnSpawn = [&] (const TString&) {
            firstSpawnEntered.Signal();
            allowFirstSpawn.WaitI();
        };

        std::exception_ptr firstError;
        TThread firstThread([&] {
            try {
                fixture.Pool.Acquire(fixture.MakeRequest("A"));
            } catch (...) {
                firstError = std::current_exception();
            }
        });
        firstThread.Start();
        bool joined = false;
        Y_DEFER {
            if (!joined) {
                allowFirstSpawn.Signal();
                firstThread.Join();
            }
        };

        UNIT_ASSERT(firstSpawnEntered.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("B")),
            std::runtime_error,
            "Spawn failed");
        allowFirstSpawn.Signal();
        firstThread.Join();
        joined = true;

        UNIT_ASSERT(firstError);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 2);

        fixture.CleanupSucceeds = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("C")),
            std::runtime_error,
            "Spawn failed");
        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
    }

    Y_UNIT_TEST(ShutdownRetriesQuarantinedCleanup)
    {
        TFixture fixture(0);
        fixture.CleanupSucceeds = false;
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("A")),
            std::runtime_error,
            "Spawn failed");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);

        fixture.Pool.BeginShutdown();
        UNIT_ASSERT(!fixture.Pool.FinishShutdown());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 1);

        fixture.CleanupSucceeds = true;
        UNIT_ASSERT(fixture.Pool.FinishShutdown());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 3);
    }

    Y_UNIT_TEST(RejectedQuarantineCleanupIsRetriedOnNextAcquire)
    {
        TFixture fixture(0);
        fixture.CleanupSucceeds = false;
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("A")),
            std::runtime_error,
            "Spawn failed");

        fixture.Scheduler.AcceptCallbacks = false;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("B")),
            std::runtime_error,
            "Spawn failed");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 1);

        fixture.Scheduler.AcceptCallbacks = true;
        fixture.CleanupSucceeds = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            fixture.Pool.Acquire(fixture.MakeRequest("C")),
            std::runtime_error,
            "Spawn failed");
        fixture.Scheduler.RunAll();

        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Quarantined, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 2);
    }

    Y_UNIT_TEST(InFlightSpawnAcrossKeyChangeUsesIndependentBound)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.MarkActive(activeA);

        TManualEvent spawnEntered;
        TManualEvent allowSpawn;
        fixture.OnSpawn = [&] (const TString& key) {
            UNIT_ASSERT_VALUES_EQUAL(key, "A");
            spawnEntered.Signal();
            allowSpawn.WaitI();
        };
        TThread refillThread([&] {
            fixture.Scheduler.RunNext();
        });
        refillThread.Start();
        bool joined = false;
        Y_DEFER {
            if (!joined) {
                allowSpawn.Signal();
                refillThread.Join();
            }
        };

        const bool entered = spawnEntered.WaitT(TDuration::Seconds(5));
        if (entered) {
            auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));
            fixture.MarkActive(activeB);
            AssertWithinBudget(fixture.Pool.GetSnapshot(), 2);
            UNIT_ASSERT_LE(fixture.CountAliveUnreturned(), 2);
        }
        allowSpawn.Signal();
        refillThread.Join();
        joined = true;
        UNIT_ASSERT(entered);

        fixture.Scheduler.RunAll();
        AssertWithinBudget(fixture.Pool.GetSnapshot(), 2);
        UNIT_ASSERT_LE(fixture.CountAliveUnreturned(), 2);
    }

    Y_UNIT_TEST(ShutdownWaitsForInFlightSpawnLifecycle)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeA = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.MarkActive(activeA);

        TManualEvent spawnEntered;
        TManualEvent allowSpawn;
        fixture.OnSpawn = [&] (const TString&) {
            spawnEntered.Signal();
            allowSpawn.WaitI();
        };
        TThread refillThread([&] {
            fixture.Scheduler.RunNext();
        });
        refillThread.Start();
        bool joined = false;
        Y_DEFER {
            if (!joined) {
                allowSpawn.Signal();
                refillThread.Join();
            }
        };

        const bool entered = spawnEntered.WaitT(TDuration::Seconds(5));
        if (entered) {
            fixture.Pool.BeginShutdown();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Warming, 1);
        }
        allowSpawn.Signal();
        refillThread.Join();
        joined = true;
        UNIT_ASSERT(entered);

        UNIT_ASSERT(fixture.Pool.FinishShutdown());
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
        UNIT_ASSERT(activeA->Alive);
    }

    Y_UNIT_TEST(BeginShutdownDoesNotRetryRunningCleanup)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        fixture.CleanupSucceeds = false;
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("B"));
        fixture.MarkActive(active);
        fixture.OnCleanup = [&] {
            fixture.Pool.BeginShutdown();
            UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 1);
        };

        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 1);
        fixture.OnCleanup = {};
        fixture.CleanupSucceeds = true;
        UNIT_ASSERT(fixture.Pool.FinishShutdown());
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
        UNIT_ASSERT(active->Alive);
    }

    Y_UNIT_TEST(PartiallySpawnedProcessRetiresBeforeReservationIsReleased)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.FailNextSpawn = true;
        fixture.ProcessOnSpawnError = true;

        fixture.Scheduler.RunNext();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Warming, 0);

        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Idle, 0);
    }

    Y_UNIT_TEST(CleanupErrorKeepsRetiringBudget)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        fixture.CleanupSucceeds = false;
        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
        UNIT_ASSERT(activeB->Alive);
        fixture.MarkActive(activeB);
        auto activeC = fixture.Pool.Acquire(fixture.MakeRequest("C"));
        fixture.MarkActive(activeC);
        fixture.Scheduler.RunAll();
        UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 1);
        UNIT_ASSERT(activeB->Alive);
        UNIT_ASSERT(activeC->Alive);
    }

    Y_UNIT_TEST(PrewarmFailureCleansStartedProcessesAndPreservesError)
    {
        for (bool partialSpawn : {false, true}) {
            for (bool cleanupSucceeds : {false, true}) {
                TFixture fixture(3);
                fixture.ProcessOnSpawnError = partialSpawn;
                fixture.CleanupSucceeds = cleanupSucceeds;
                fixture.OnSpawn = [&] (const TString&) {
                    fixture.FailNextSpawn = true;
                };

                UNIT_ASSERT_EXCEPTION_CONTAINS(
                    fixture.Pool.Prewarm(fixture.MakeRequest("A")),
                    std::runtime_error,
                    "Spawn failed");

                const int startedCount = partialSpawn ? 2 : 1;
                UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(fixture.CleanedProcessIds.size(), startedCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    fixture.CountAliveUnreturned(), cleanupSucceeds ? 0 : startedCount);
                const auto snapshot = fixture.Pool.GetSnapshot();
                UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
                UNIT_ASSERT_VALUES_EQUAL(snapshot.Warming, 0);
                UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, cleanupSucceeds ? 0 : startedCount);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
            }
        }
    }

    Y_UNIT_TEST(WatcherCleansDeadIdleWithoutRestartUntilDemand)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        fixture.Processes.front()->Alive = false;

        fixture.Pool.SweepDead();
        AssertWithinBudget(fixture.Pool.GetSnapshot(), 2);
        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 2);

        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
        fixture.MarkActive(active);
        fixture.Scheduler.RunAll();
        UNIT_ASSERT(active->Alive);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Idle, 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 2);
    }

    Y_UNIT_TEST(DeadIdleDoesNotCauseRepeatedOrQueuedRefill)
    {
        for (bool queueRefill : {false, true}) {
            TFixture fixture(2);
            fixture.Pool.Prewarm(fixture.MakeRequest("A"));
            std::shared_ptr<TFakeProcess> active;
            if (queueRefill) {
                active = fixture.Pool.Acquire(fixture.MakeRequest("A"));
                fixture.MarkActive(active);
            }
            for (const auto& process : fixture.Processes) {
                if (process != active) {
                    process->Alive = false;
                }
            }

            for (int iteration = 0; iteration < 3; ++iteration) {
                fixture.Pool.SweepDead();
                fixture.Scheduler.RunAll();
                UNIT_ASSERT_VALUES_EQUAL(fixture.Processes.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Idle, 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Retiring, 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 0);
            }

            auto nextActive = fixture.Pool.Acquire(fixture.MakeRequest("B"));
            fixture.MarkActive(nextActive);
            fixture.Scheduler.RunAll();
            UNIT_ASSERT(nextActive->Alive);
            UNIT_ASSERT_VALUES_EQUAL(nextActive->Key, "B");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Pool.GetSnapshot().Idle, 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.CountAliveUnreturned(), 2);
            if (active) {
                UNIT_ASSERT(active->Alive);
            }
        }
    }

    Y_UNIT_TEST(ZeroDisablesOnlyBackgroundPool)
    {
        TFixture fixture(0);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));

        auto active = fixture.Pool.Acquire(fixture.MakeRequest("B"));

        UNIT_ASSERT_VALUES_EQUAL(active->Key, "B");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Scheduler.Size(), 0);
        AssertWithinBudget(fixture.Pool.GetSnapshot(), 0);
    }

    Y_UNIT_TEST(SchedulerRejectionDoesNotLeakPendingRefill)
    {
        TFixture fixture(1);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        fixture.Scheduler.AcceptCallbacks = false;

        auto active = fixture.Pool.Acquire(fixture.MakeRequest("A"));

        const auto snapshot = fixture.Pool.GetSnapshot();
        UNIT_ASSERT(!snapshot.RefillQueued);
        UNIT_ASSERT(!snapshot.RefillRunning);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Warming, 0);
        UNIT_ASSERT(active->Alive);
    }

    Y_UNIT_TEST(SmallPoolOfThreeObeysSameBudget)
    {
        TFixture fixture(3);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto active = fixture.Pool.Acquire(fixture.MakeRequest("B"));

        fixture.Scheduler.RunAll();

        const auto snapshot = fixture.Pool.GetSnapshot();
        AssertWithinBudget(snapshot, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 3);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 0);
        UNIT_ASSERT(active->Alive);
    }

    Y_UNIT_TEST(ShutdownCleansOnlyPoolOwnedProcesses)
    {
        TFixture fixture(2);
        fixture.Pool.Prewarm(fixture.MakeRequest("A"));
        auto activeB = fixture.Pool.Acquire(fixture.MakeRequest("B"));

        fixture.Pool.BeginShutdown();
        fixture.Scheduler.Clear();
        UNIT_ASSERT(fixture.Pool.FinishShutdown());

        UNIT_ASSERT(activeB->Alive);
        const auto snapshot = fixture.Pool.GetSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Idle, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Retiring, 0);
    }
}

} // namespace
} // namespace NYql::NTaskRunnerProxy
