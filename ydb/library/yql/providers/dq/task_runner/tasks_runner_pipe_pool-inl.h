#ifndef TASKS_RUNNER_PIPE_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include tasks_runner_pipe_pool.h"
// For the sake of sane code completion.
#include "tasks_runner_pipe_pool.h"
#endif

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

#include <util/system/yassert.h>

namespace NYql::NTaskRunnerProxy {

template <class TProcess>
TPipeProcessPool<TProcess>::TPipeProcessPool(
    int maxProcesses,
    TSchedule schedule,
    TCleanup cleanup,
    TIsAlive isAlive)
    : MaxProcesses_(Max(0, maxProcesses))
    , Schedule_(std::move(schedule))
    , Cleanup_(std::move(cleanup))
    , IsAlive_(std::move(isAlive))
{ }

template <class TProcess>
void TPipeProcessPool<TProcess>::Prewarm(const TRequest& request)
{
    {
        TGuard<TMutex> guard(Mutex_);
        DesiredRequest_ = request;
        RefillBlocked_ = false;
    }

    while (true) {
        {
            TGuard<TMutex> guard(Mutex_);
            if (GetPoolOwnedCountUnsafe() >= MaxProcesses_) {
                break;
            }
            ++Warming_;
            VerifyBudgetUnsafe();
        }

        auto spawnResult = Spawn(request);
        if (spawnResult.Error) {
            {
                TGuard<TMutex> guard(Mutex_);
                --Warming_;
                if (spawnResult.Process) {
                    Retiring_.push_back(TEntry{
                        .Id = NextEntryId_++,
                        .Key = request.Key,
                        .Process = std::move(spawnResult.Process),
                    });
                }
                VerifyBudgetUnsafe();
            }

            auto error = spawnResult.Error;
            BeginShutdown();
            FinishShutdown();
            std::rethrow_exception(error);
        }

        Y_ABORT_UNLESS(spawnResult.Process);
        TGuard<TMutex> guard(Mutex_);
        --Warming_;
        Idle_.push_back(TEntry{
            .Id = NextEntryId_++,
            .Key = request.Key,
            .Process = std::move(spawnResult.Process),
        });
        VerifyBudgetUnsafe();
    }
}

template <class TProcess>
typename TPipeProcessPool<TProcess>::TProcessPtr TPipeProcessPool<TProcess>::Acquire(const TRequest& request)
{
    TProcessPtr result;
    std::exception_ptr quarantineError;
    {
        TGuard<TMutex> guard(Mutex_);
        DesiredRequest_ = request;
        RefillBlocked_ = false;

        for (auto it = Idle_.begin(); it != Idle_.end();) {
            if (it->Key == request.Key) {
                if (!result) {
                    result = std::move(it->Process);
                    it = Idle_.erase(it);
                } else {
                    ++it;
                }
            } else {
                Retiring_.push_back(std::move(*it));
                it = Idle_.erase(it);
            }
        }

        if (!Quarantined_.empty()) {
            for (auto& entry : Quarantined_) {
                entry.CleanupAttempted = false;
            }
            if (!result) {
                quarantineError = Quarantined_.front().Error;
            }
        }
        VerifyBudgetUnsafe();
    }

    RequestCleanup();
    RequestRefill();

    if (!result) {
        if (quarantineError) {
            std::rethrow_exception(quarantineError);
        }

        auto spawnResult = Spawn(request);
        if (spawnResult.Error) {
            if (spawnResult.Process) {
                bool cleaned = false;
                try {
                    cleaned = Cleanup_(spawnResult.Process);
                } catch (...) {
                }

                if (!cleaned) {
                    TGuard<TMutex> guard(Mutex_);
                    Quarantined_.push_back(TQuarantinedEntry{
                        .Id = NextEntryId_++,
                        .Process = std::move(spawnResult.Process),
                        .Error = spawnResult.Error,
                        .CleanupAttempted = true,
                    });
                }
            }
            std::rethrow_exception(spawnResult.Error);
        }
        Y_ABORT_UNLESS(spawnResult.Process);
        result = std::move(spawnResult.Process);
    }

    return result;
}

template <class TProcess>
void TPipeProcessPool<TProcess>::SweepDead()
{
    bool foundDeadProcess = false;
    {
        TGuard<TMutex> guard(Mutex_);
        for (auto it = Idle_.begin(); it != Idle_.end();) {
            bool alive = true;
            try {
                alive = IsAlive_(it->Process);
            } catch (...) {
            }

            if (!alive) {
                Retiring_.push_back(std::move(*it));
                it = Idle_.erase(it);
                foundDeadProcess = true;
                RefillBlocked_ = true;
            } else {
                ++it;
            }
        }
        VerifyBudgetUnsafe();
    }

    if (foundDeadProcess) {
        RequestCleanup();
    }
}

template <class TProcess>
void TPipeProcessPool<TProcess>::BeginShutdown()
{
    TGuard<TMutex> guard(Mutex_);
    ShuttingDown_ = true;
    DesiredRequest_.Clear();
    Retiring_.splice(Retiring_.end(), Idle_);
    VerifyBudgetUnsafe();
}

template <class TProcess>
bool TPipeProcessPool<TProcess>::FinishShutdown()
{
    {
        TGuard<TMutex> guard(Mutex_);
        for (auto& entry : Retiring_) {
            entry.CleanupAttempted = false;
        }
        for (auto& entry : Quarantined_) {
            entry.CleanupAttempted = false;
        }
    }
    ProcessCleanup();

    TGuard<TMutex> guard(Mutex_);
    return Retiring_.empty() && Quarantined_.empty() && Warming_ == 0;
}

template <class TProcess>
typename TPipeProcessPool<TProcess>::TSnapshot TPipeProcessPool<TProcess>::GetSnapshot() const
{
    TGuard<TMutex> guard(Mutex_);
    return {
        .Idle = static_cast<int>(Idle_.size()),
        .Retiring = static_cast<int>(Retiring_.size()),
        .Warming = Warming_,
        .Quarantined = static_cast<int>(Quarantined_.size()),
        .RefillQueued = RefillQueued_,
        .RefillRunning = RefillRunning_,
    };
}

template <class TProcess>
void TPipeProcessPool<TProcess>::RequestCleanup()
{
    bool shouldSchedule = false;
    {
        TGuard<TMutex> guard(Mutex_);
        const auto quarantinedIt = FindIf(Quarantined_, [] (const auto& entry) {
            return !entry.CleanupAttempted;
        });
        const auto retiringIt = FindIf(Retiring_, [] (const auto& entry) {
            return !entry.CleanupAttempted;
        });
        if (!ShuttingDown_ &&
            (quarantinedIt != Quarantined_.end() || retiringIt != Retiring_.end()) &&
            !CleanupQueued_ &&
            !CleanupRunning_)
        {
            CleanupQueued_ = true;
            shouldSchedule = true;
        }
    }

    if (shouldSchedule && !Schedule([this] { ProcessCleanup(); })) {
        TGuard<TMutex> guard(Mutex_);
        CleanupQueued_ = false;
    }
}

template <class TProcess>
void TPipeProcessPool<TProcess>::ProcessCleanup()
{
    {
        TGuard<TMutex> guard(Mutex_);
        CleanupQueued_ = false;
        CleanupRunning_ = true;
    }

    bool stateChanged = false;
    while (true) {
        TProcessPtr process;
        i64 entryId = 0;
        bool quarantined = false;
        {
            TGuard<TMutex> guard(Mutex_);
            auto quarantinedIt = FindIf(Quarantined_, [] (const auto& entry) {
                return !entry.CleanupAttempted;
            });
            if (quarantinedIt != Quarantined_.end()) {
                entryId = quarantinedIt->Id;
                process = quarantinedIt->Process;
                quarantinedIt->CleanupAttempted = true;
                quarantined = true;
            } else {
                auto retiringIt = FindIf(Retiring_, [] (const auto& entry) {
                    return !entry.CleanupAttempted;
                });
                if (retiringIt != Retiring_.end()) {
                    entryId = retiringIt->Id;
                    process = retiringIt->Process;
                    retiringIt->CleanupAttempted = true;
                } else {
                    CleanupRunning_ = false;
                    break;
                }
            }
        }

        bool cleaned = false;
        try {
            cleaned = Cleanup_(process);
        } catch (...) {
        }

        if (cleaned) {
            TGuard<TMutex> guard(Mutex_);
            if (quarantined) {
                auto it = FindIf(Quarantined_, [entryId] (const auto& entry) {
                    return entry.Id == entryId;
                });
                if (it != Quarantined_.end()) {
                    Quarantined_.erase(it);
                    stateChanged = true;
                }
            } else {
                auto it = FindIf(Retiring_, [entryId] (const auto& entry) {
                    return entry.Id == entryId;
                });
                if (it != Retiring_.end()) {
                    Retiring_.erase(it);
                    stateChanged = true;
                }
            }
            if (stateChanged) {
                VerifyBudgetUnsafe();
            }
        }
    }

    if (stateChanged) {
        RequestRefill();
    }
}

template <class TProcess>
void TPipeProcessPool<TProcess>::RequestRefill()
{
    bool shouldSchedule = false;
    {
        TGuard<TMutex> guard(Mutex_);
        if (!ShuttingDown_ &&
            DesiredRequest_ &&
            !RefillBlocked_ &&
            Quarantined_.empty() &&
            GetPoolOwnedCountUnsafe() < MaxProcesses_ &&
            !RefillQueued_ &&
            !RefillRunning_)
        {
            RefillQueued_ = true;
            shouldSchedule = true;
        }
    }

    if (shouldSchedule && !Schedule([this] { ProcessRefill(); })) {
        TGuard<TMutex> guard(Mutex_);
        RefillQueued_ = false;
    }
}

template <class TProcess>
void TPipeProcessPool<TProcess>::ProcessRefill()
{
    {
        TGuard<TMutex> guard(Mutex_);
        RefillQueued_ = false;
        if (ShuttingDown_ || !DesiredRequest_ || RefillBlocked_ || !Quarantined_.empty()) {
            return;
        }
        RefillRunning_ = true;
    }

    bool scheduleCleanup = false;
    bool scheduleRefill = false;
    while (true) {
        TRequest request;
        {
            TGuard<TMutex> guard(Mutex_);
            if (ShuttingDown_ ||
                !DesiredRequest_ ||
                RefillBlocked_ ||
                !Quarantined_.empty() ||
                GetPoolOwnedCountUnsafe() >= MaxProcesses_)
            {
                RefillRunning_ = false;
                break;
            }
            request = *DesiredRequest_;
            ++Warming_;
            VerifyBudgetUnsafe();
        }

        auto spawnResult = Spawn(request);
        if (spawnResult.Error) {
            {
                TGuard<TMutex> guard(Mutex_);
                --Warming_;
                const bool requestStillDesired =
                    !ShuttingDown_ && DesiredRequest_ && DesiredRequest_->Key == request.Key;
                RefillBlocked_ = requestStillDesired;
                RefillRunning_ = false;
                scheduleRefill = !requestStillDesired && !ShuttingDown_ && DesiredRequest_;
                if (spawnResult.Process) {
                    Retiring_.push_back(TEntry{
                        .Id = NextEntryId_++,
                        .Key = request.Key,
                        .Process = std::move(spawnResult.Process),
                    });
                    scheduleCleanup = true;
                }
                VerifyBudgetUnsafe();
            }
            break;
        }

        Y_ABORT_UNLESS(spawnResult.Process);
        {
            TGuard<TMutex> guard(Mutex_);
            --Warming_;
            TEntry entry{
                .Id = NextEntryId_++,
                .Key = request.Key,
                .Process = std::move(spawnResult.Process),
            };
            if (!ShuttingDown_ && DesiredRequest_ && DesiredRequest_->Key == request.Key) {
                Idle_.push_back(std::move(entry));
            } else {
                Retiring_.push_back(std::move(entry));
                scheduleCleanup = true;
            }
            VerifyBudgetUnsafe();
        }
    }

    if (scheduleCleanup) {
        RequestCleanup();
    }
    if (scheduleRefill) {
        RequestRefill();
    }
}

template <class TProcess>
typename TPipeProcessPool<TProcess>::TSpawnResult TPipeProcessPool<TProcess>::Spawn(const TRequest& request) const
{
    try {
        return request.Spawn();
    } catch (...) {
        return {
            .Error = std::current_exception(),
        };
    }
}

template <class TProcess>
bool TPipeProcessPool<TProcess>::Schedule(std::function<void()> callback)
{
    try {
        return Schedule_(std::move(callback));
    } catch (...) {
        return false;
    }
}

template <class TProcess>
int TPipeProcessPool<TProcess>::GetPoolOwnedCountUnsafe() const
{
    return static_cast<int>(Idle_.size() + Retiring_.size()) + Warming_;
}

template <class TProcess>
void TPipeProcessPool<TProcess>::VerifyBudgetUnsafe() const
{
    Y_ABORT_UNLESS(GetPoolOwnedCountUnsafe() <= MaxProcesses_);
}

} // namespace NYql::NTaskRunnerProxy
