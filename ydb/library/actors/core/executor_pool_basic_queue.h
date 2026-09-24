#pragma once

// Queue-independent worker scheduling. This header is deliberately included
// only by executor implementations, not by actors or mailbox code.
#include "executor_pool_basic.h"
#include "activity_guard.h"
#include "debug.h"
#include "mailbox.h"
#include "probes.h"
#include "thread_context.h"

#define EXECUTOR_POOL_QUEUE_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, "Pool_", PoolId, " TExecutorPoolBasic::", __func__, ": ", __VA_ARGS__)

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    template<class TQueue>
    TMailbox* TBasicExecutorPool::GetReadyActivationWithQueue(TQueue& queue, ui64 revolvingCounter) {
        if (TlsThreadContext->IsShared()) {
            return GetReadyActivationSharedImpl(queue, revolvingCounter);
        }
        return EnableWaker ? GetReadyActivationWakerImpl(queue, revolvingCounter)
            : GetReadyActivationRingQueueImpl(queue, revolvingCounter);
    }

    template<class TQueue>
    void TBasicExecutorPool::ScheduleActivationWithQueue(TQueue& queue, TMailbox* mailbox, ui64 revolvingCounter) {
        if (EnableWaker) {
            ScheduleActivationExWakerImpl(queue, mailbox, revolvingCounter);
        } else {
            ScheduleActivationExRingQueueImpl(queue, mailbox, revolvingCounter, std::nullopt);
        }
    }

    template<class TQueue>
    TMailbox* TBasicExecutorPool::GetReadyActivationRingQueueImpl(TQueue& queue, ui64 revolvingCounter) {
        if (StopFlag.load(std::memory_order_acquire)) {
            return nullptr;
        }

        TWorkerId workerId = TlsThreadContext->WorkerId();
        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "");
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        if (Harmonizer) {
            EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "try to harmonize");
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
            EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "harmonize done");
        }
        Threads[workerId].UnsetWork();

        while (!StopFlag.load(std::memory_order_acquire)) {
            {
                ui64 checkToSleepWorkers = CheckToSleepWorkers.load(std::memory_order_acquire);
                bool needToCheckSleep = checkToSleepWorkers != 0;
                if (needToCheckSleep) {
                    CheckToSleepWorkers.compare_exchange_weak(checkToSleepWorkers, checkToSleepWorkers - 1, std::memory_order_release, std::memory_order_relaxed);
                } else { // otherwise we ready to get activation
                    TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                    if (const ui32 activation = queue.Pop(++revolvingCounter)) {
                        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "activation found");
                        Threads[workerId].SetWork();
                        AtomicDecrement(Semaphore);
                        return MailboxTable->Get(activation);
                    }
                }
            }

            TAtomic semaphoreRaw = AtomicGet(Semaphore);
            TSemaphore semaphore = TSemaphore::GetSemaphore(semaphoreRaw);
            if (!semaphore.OldSemaphore || workerId >= 0 && semaphore.CurrentSleepThreadCount < 0) {
                EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "semaphore.OldSemaphore == 0 or workerId >= 0 && semaphore.CurrentSleepThreadCount < 0");
                if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                    EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "wctx.ExecutionContext.IsNeededToWaitNextActivation == false");
                    return nullptr;
                }

                bool needToWait = false;
                bool needToBlock = false;
                AskToGoToSleep(&needToWait, &needToBlock);
                if (needToWait) {
                    EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "go to sleep");
                    if (Threads[workerId].Wait(SpinThresholdCycles, &StopFlag)) {
                        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "sleep interrupted");
                        return nullptr;
                    }
                }
            }
            SpinLockPause();
        }

        return nullptr;
    }

    template<class TQueue>
    TMailbox* TBasicExecutorPool::GetReadyActivationWakerImpl(TQueue& queue, ui64 revolvingCounter) {
        if (StopFlag.load(std::memory_order_acquire)) {
            return nullptr;
        }

        const TWorkerId workerId = TlsThreadContext->WorkerId();
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);
        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
        }
        Threads[workerId].UnsetWork();

        const auto settleWakerState = [&] {
            while (!StopFlag.load(std::memory_order_acquire)) {
                const EThreadState state = Threads[workerId].GetState<EThreadState>();
                Y_DEBUG_ABORT_UNLESS(state != EThreadState::Waker);
                if (IsNeedToBeWaker(state)) {
                    RunWaker(workerId);
                    continue;
                }
                if (state == EThreadState::Spin || state == EThreadState::Sleep || state == EThreadState::Blocking) {
                    const bool stopped = Threads[workerId].WaitForWaker(
                        StopFlag, ActivationCredits, CheckToSleepWorkers, WakerRequestBit);
                    if (stopped) {
                        return true;
                    }
                    continue;
                }
                return false;
            }
            return true;
        };

        while (!StopFlag.load(std::memory_order_acquire)) {
            if (TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                ui64 reductions = CheckToSleepWorkers.load(std::memory_order_acquire);
                bool restartWorkerIteration = false;
                while (true) {
                    if (reductions & WakerRequestBit) {
                        if (CheckToSleepWorkers.compare_exchange_weak(reductions, reductions & WakerReductionMask,
                                std::memory_order_acq_rel, std::memory_order_acquire)) {
                            EThreadState expected = EThreadState::None;
                            bool changed = Threads[workerId].TrySetNeedToBeWaker(&expected);
                            Y_DEBUG_ABORT_UNLESS(changed);
                            settleWakerState();
                            restartWorkerIteration = true;
                            break;
                        }
                        continue;
                    }
                    if (reductions == 0) {
                        break;
                    }
                    if (!CheckToSleepWorkers.compare_exchange_weak(reductions, reductions - 1,
                            std::memory_order_acq_rel, std::memory_order_acquire)) {
                        continue;
                    }

                    EThreadState expected = EThreadState::None;
                    Y_ABORT_UNLESS(Threads[workerId].ReplaceState(expected, EThreadState::Blocking));
                    if (!WakerPending.exchange(true, std::memory_order_acq_rel)) {
                        Threads[workerId].TrySetNeedToBeWaker();
                    }
                    if (settleWakerState()) {
                        return nullptr;
                    }
                    restartWorkerIteration = true;
                    break;
                }

                if (restartWorkerIteration) {
                    continue;
                }
            }

            {
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> queueActivityGuard;
                if (const ui32 activation = queue.Pop(++revolvingCounter)) {
                    const i64 previousCredits = ActivationCredits.fetch_sub(1, std::memory_order_acq_rel);
                    Y_DEBUG_ABORT_UNLESS(previousCredits > 0);
                    Threads[workerId].SetWork();
                    return MailboxTable->Get(activation);
                }
            }

            if (ActivationCredits.load(std::memory_order_acquire) > 0) {
                SpinLockPause();
                continue;
            }

            if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                return nullptr;
            }

            EThreadState expected = EThreadState::None;
            if (!Threads[workerId].ReplaceState(expected, EThreadState::Spin)) {
                continue;
            }
            if (!WakerPending.exchange(true, std::memory_order_acq_rel)) {
                Threads[workerId].TrySetNeedToBeWaker();
            }
            if (settleWakerState()) {
                return nullptr;
            }
        }
        return nullptr;
    }

    template<class TQueue>
    TMailbox* TBasicExecutorPool::GetReadyActivationSharedImpl(TQueue& queue, ui64 revolvingCounter) {
        TWorkerId workerId = TlsThreadContext->WorkerId();
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        if (!SharedPool->HasWakerPools) {
            SharedPool->Threads[workerId].UnsetWork();
        }
        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
        }
        constexpr ui32 maxAttempts = 8;
        if (EnableWaker) {
            TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
            for (ui32 attempt = 0; attempt < maxAttempts && !StopFlag.load(std::memory_order_acquire); ++attempt) {
                if (const ui32 activation = queue.Pop(revolvingCounter++)) {
                    const i64 credits = ActivationCredits.fetch_sub(1, std::memory_order_acq_rel);
                    Y_DEBUG_ABORT_UNLESS(credits > 0);
                    SharedPool->Threads[workerId].SetWorkForWaker();
                    return MailboxTable->Get(activation);
                }
                if (ActivationCredits.load(std::memory_order_acquire) == 0) {
                    return nullptr;
                }
                SpinLockPause();
            }
            return nullptr;
        }
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "revolvingCounter == ", revolvingCounter, " semaphore == ", semaphore.OldSemaphore);
        for (ui32 attempt = 0; attempt < maxAttempts && !StopFlag.load(std::memory_order_acquire); ++attempt) {
            if (!semaphore.OldSemaphore) {
                EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Executor, "semaphore == 0");
                return nullptr;
            } else {
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                if (const ui32 activation = queue.Pop(revolvingCounter++)) {
                    if (SharedPool->HasWakerPools) {
                        SharedPool->Threads[workerId].SetWorkForWaker();
                    } else {
                        SharedPool->Threads[workerId].SetWork();
                    }
                    AtomicDecrement(Semaphore);
                    EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "activation == ", activation, " semaphore == ", semaphore.OldSemaphore);
                    return MailboxTable->Get(activation);
                }
            }

            SpinLockPause();
            x = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(x);
        }
        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Executor, "stop");
        return nullptr;
    }

    template<class TQueue>
    void TBasicExecutorPool::ScheduleActivationExRingQueueImpl(TQueue& queue, TMailbox* mailbox, ui64 revolvingCounter, std::optional<TAtomic> initSemaphore) {
        queue.Push(mailbox->Hint, revolvingCounter);
        bool needToWakeUp = false;
        bool needToChangeOldSemaphore = true;

        TAtomic x;
        TSemaphore semaphore;
        if (!initSemaphore || SharedPool) {
            x = AtomicIncrement(Semaphore);
            needToChangeOldSemaphore = false;
            semaphore = TSemaphore::GetSemaphore(x);
            EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "Semaphore incremented to ", semaphore.OldSemaphore, " CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
        } else {
            x = *initSemaphore;
            semaphore = TSemaphore::GetSemaphore(x);
        }
        EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "semaphore.OldSemaphore == ", semaphore.OldSemaphore, " semaphore.CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
        if (SharedPool) {
            if (SharedPool->WakeUpLocalThreads(PoolId)) {
                EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "shared pool wake up local threads");
                return;
            }
        }

        i16 sleepThreads = 0;
        Y_UNUSED(sleepThreads);
        do {
            needToWakeUp = semaphore.CurrentSleepThreadCount > 0;
            i64 oldX = semaphore.ConvertToI64();
            bool changed = false;
            if (needToChangeOldSemaphore) {
                semaphore.OldSemaphore++;
                changed = true;
            }
            if (needToWakeUp) {
                sleepThreads = semaphore.CurrentSleepThreadCount--;
                changed = true;
            }
            if (changed) {
                x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), oldX);
            }
            if (x == oldX) {
                break;
            }
            semaphore = TSemaphore::GetSemaphore(x);
        } while (true);

        if (needToWakeUp) { // we must find someone to wake-up
            EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "need to wake up");
            WakeUpLoop(semaphore.CurrentThreadCount);
        } else if (SharedPool) {
            if (SharedPool->WakeUpAdjacentOwner(PoolId)) {
                EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "shared pool wake up adjacent owner");
            } else {
                EXECUTOR_POOL_QUEUE_DEBUG(EDebugLevel::Activation, "shared pool wake up global threads");
                SharedPool->WakeUpGlobalThreads(PoolId);
            }
        }
    }

    template<class TQueue>
    void TBasicExecutorPool::ScheduleActivationExWakerImpl(TQueue& queue, TMailbox* mailbox, ui64 revolvingCounter) {
        ActivationCredits.fetch_add(1, std::memory_order_acq_rel);
        queue.Push(mailbox->Hint, revolvingCounter);
        if (SleepingCount.load(std::memory_order_acquire) > 0 ||
                (SharedPool && SharedPool->SharedSleepingCount.load(std::memory_order_acquire) > 0)) {
            RequestWaker(false);
        }
    }

} // namespace NActors

#undef EXECUTOR_POOL_QUEUE_DEBUG
