#pragma once

#include <util/generic/function.h>
#include <util/generic/list.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <util/system/mutex.h>

#include <exception>
#include <memory>

namespace NYql::NTaskRunnerProxy {

template <class TProcess>
class TPipeProcessPool
{
public:
    using TProcessPtr = std::shared_ptr<TProcess>;

    struct TSpawnResult
    {
        TProcessPtr Process;
        std::exception_ptr Error;
    };

    struct TRequest
    {
        TString Key;
        std::function<TSpawnResult()> Spawn;
    };

    struct TSnapshot
    {
        int Idle = 0;
        int Retiring = 0;
        int Warming = 0;
        int Quarantined = 0;
        bool RefillQueued = false;
        bool RefillRunning = false;
    };

    // Scheduled callbacks must execute serially.
    using TSchedule = std::function<bool(std::function<void()>)>;
    using TCleanup = std::function<bool(const TProcessPtr&)>;
    using TIsAlive = std::function<bool(const TProcessPtr&)>;

    TPipeProcessPool(int maxProcesses, TSchedule schedule, TCleanup cleanup, TIsAlive isAlive);

    // Only during initialization, before concurrent access.
    void Prewarm(const TRequest& request);

    TProcessPtr Acquire(const TRequest& request);
    void SweepDead();

    // Callers must stop acquiring processes before shutdown begins.
    void BeginShutdown();

    // No callbacks may be running or pending.
    bool FinishShutdown();

    TSnapshot GetSnapshot() const;

private:
    struct TEntry
    {
        i64 Id = 0;
        TString Key;
        TProcessPtr Process;
        bool CleanupAttempted = false;
    };

    struct TQuarantinedEntry
    {
        i64 Id = 0;
        TProcessPtr Process;
        std::exception_ptr Error;
        bool CleanupAttempted = false;
    };

    const int MaxProcesses_;
    const TSchedule Schedule_;
    const TCleanup Cleanup_;
    const TIsAlive IsAlive_;

    mutable TMutex Mutex_;
    TList<TEntry> Idle_;
    TList<TEntry> Retiring_;
    TList<TQuarantinedEntry> Quarantined_;
    int Warming_ = 0;
    i64 NextEntryId_ = 1;

    TMaybe<TRequest> DesiredRequest_;
    bool CleanupQueued_ = false;
    bool CleanupRunning_ = false;
    bool RefillQueued_ = false;
    bool RefillRunning_ = false;
    bool RefillBlocked_ = false;
    bool ShuttingDown_ = false;

    void RequestCleanup();
    void ProcessCleanup();

    void RequestRefill();
    void ProcessRefill();

    TSpawnResult Spawn(const TRequest& request) const;
    bool Schedule(std::function<void()> callback);
    int GetPoolOwnedCountUnsafe() const;
    void VerifyBudgetUnsafe() const;
};

} // namespace NYql::NTaskRunnerProxy

#define TASKS_RUNNER_PIPE_POOL_INL_H_
#include "tasks_runner_pipe_pool-inl.h"
#undef TASKS_RUNNER_PIPE_POOL_INL_H_
