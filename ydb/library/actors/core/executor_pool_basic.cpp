#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_basic_sanitizer.h"
#include "executor_pool_shared.h"
#include "executor_pool_jail.h"
#include "actor.h"
#include "config.h"
#include "executor_thread_ctx.h"
#include "probes.h"
#include "mailbox.h"
#include "debug.h"
#include "thread_context.h"
#include <atomic>
#include <memory>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>

#ifdef _linux_
#include <pthread.h>
#endif

#define POOL_ID() \
    (!TlsThreadContext ? "OUTSIDE" : \
    (TlsThreadContext->IsShared() ? "Shared[" + ToString(TlsThreadContext->OwnerPoolId()) + "]_" + ToString(TlsThreadContext->PoolId()) : \
    ("Pool_" + ToString(TlsThreadContext->PoolId()))))

#define WORKER_ID() ("Worker_" + ToString(TlsThreadContext ? TlsThreadContext->WorkerId() : Max<TWorkerId>()))

#define EXECUTOR_POOL_BASIC_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, POOL_ID(), " ", WORKER_ID(), " TExecutorPoolBasic::", __func__, ": ", __VA_ARGS__)


namespace NActors {

    class TBasicExecutorPool::TWaker {
    public:
        explicit TWaker(TBasicExecutorPool* pool)
        {
            SleepingStack.reserve(pool->MaxFullThreadCount);
        }

    private:
        friend class TBasicExecutorPool;
        TVector<i16> SleepingStack;
        ui64 PreviousReductions = 0;
        ui64 TakenTokensToSleep = 0;
        ui64 TakenTokensToWakeup = 0;
    };

    namespace {
#ifdef ACTOR_SANITIZER
        constexpr bool DebugMode = true;
#else
        constexpr bool DebugMode = false;
#endif
    }


    LWTRACE_USING(ACTORLIB_PROVIDER);


    const double TWaitingStatsConstants::HistogramResolutionUs = MaxSpinThersholdUs / BucketCount;
    const ui64 TWaitingStatsConstants::HistogramResolution = NHPTimer::GetCyclesPerSecond() * 0.000001 * HistogramResolutionUs;

    constexpr TDuration TBasicExecutorPool::DEFAULT_TIME_PER_MAILBOX;

    TString GetCurrentThreadKind() {
        if (TlsThreadContext) {
            return TlsThreadContext->WorkerId() >= 0 ? "[common]" : "[shared]";
        }
        return "[outsider]";
    }

    TBasicExecutorPool::TBasicExecutorPool(
        ui32 poolId,
        ui32 threads,
        ui64 spinThreshold,
        const TString& poolName,
        IHarmonizer *harmonizer,
        TAffinity* affinity,
        TDuration timePerMailbox,
        ui32 eventsPerMailbox,
        int realtimePriority,
        ui32 /*maxActivityType*/,
        i16 minThreadCount,
        i16 maxThreadCount,
        i16 defaultThreadCount,
        i16 priority,
        bool hasOwnSharedThread,
        TExecutorPoolJail *jail
    )
        : TBasicExecutorPool(TBasicExecutorPoolConfig{
            .PoolId = poolId,
            .PoolName = poolName,
            .Threads = threads,
            .SpinThreshold = spinThreshold,
            .Affinity = (affinity ? static_cast<TCpuMask>(*affinity) : TCpuMask{}),
            .TimePerMailbox = timePerMailbox,
            .EventsPerMailbox = eventsPerMailbox,
            .RealtimePriority = realtimePriority,
            .MinThreadCount = minThreadCount,
            .MaxThreadCount = maxThreadCount,
            .DefaultThreadCount = defaultThreadCount,
            .Priority = priority,
            .HasSharedThread = hasOwnSharedThread,
        }, harmonizer, jail)
    {
        if (affinity != nullptr) {
            delete affinity;
        }
    }

    TBasicExecutorPool::TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer, TExecutorPoolJail *jail)
        : TExecutorPoolBase(cfg.PoolId, cfg.Threads, new TAffinity(cfg.Affinity))
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , SpinThresholdCyclesPerThread(new NThreading::TPadded<std::atomic<ui64>>[cfg.Threads])
        , WaitingStats(new TWaitingStats<ui64>[cfg.Threads])
        , PoolName(cfg.PoolName)
        , TimePerMailbox(cfg.TimePerMailbox)
        , TimePerMailboxTsValue(NHPTimer::GetClockRate() * cfg.TimePerMailbox.SecondsFloat())
        , EventsPerMailboxValue(cfg.EventsPerMailbox)
        , RealtimePriority(cfg.RealtimePriority)
        , ThreadCount(cfg.Threads)
        , SuggestedThreadCount(cfg.Threads)
        , MinFullThreadCount(cfg.MinThreadCount)
        , MaxFullThreadCount(cfg.MaxThreadCount)
        , DefaultFullThreadCount(cfg.DefaultThreadCount)
        , Harmonizer(harmonizer)
        , SoftProcessingDurationTs(cfg.SoftProcessingDurationTs)
        , HasOwnSharedThread(cfg.HasSharedThread)
        , SharedOnly(cfg.ForcedForeignSlotCount || cfg.AdjacentPools.size() || (!cfg.Threads && !cfg.MaxThreadCount))
        , Priority(cfg.Priority)
        , Jail(jail)
        , EnableWaker(cfg.EnableWaker)
        , ActorSystemProfile(cfg.ActorSystemProfile)
    {
        Y_UNUSED(Jail, SoftProcessingDurationTs);

        if (cfg.AllThreadsAreShared) {
            ui32 sharedThreads = DefaultThreadCount;
            ui32 threads = ThreadCount;
            if (sharedThreads && threads) {
                threads = threads - sharedThreads;
            }
            if (cfg.AllThreadsAreShared) {
                threads = 0;
            }

            if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
                for (ui32 idx = 0; idx < threads; ++idx) {
                    SpinThresholdCyclesPerThread[idx].store(0);
                }
            }
            if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                MovingWaitingStats.Reset(new TWaitingStats<double>[threads]);
            }

            i16 limit = Min(threads, (ui32)Max<i16>());
            DefaultFullThreadCount = Min<i16>(DefaultFullThreadCount - sharedThreads, limit);

            MaxFullThreadCount = Min(Max<i16>(MaxFullThreadCount - sharedThreads, DefaultFullThreadCount), limit);

            if (MinFullThreadCount) {
                MinFullThreadCount = Min<i16>(MinFullThreadCount - sharedThreads, DefaultFullThreadCount);
            } else {
                MinFullThreadCount = DefaultFullThreadCount;
            }
        } else {
            ui32 threads = ThreadCount;
            if (HasOwnSharedThread && threads) {
                threads = threads - 1;
            }

            if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
                for (ui32 idx = 0; idx < threads; ++idx) {
                    SpinThresholdCyclesPerThread[idx].store(0);
                }
            }
            if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                MovingWaitingStats.Reset(new TWaitingStats<double>[threads]);
            }

            i16 limit = Min(threads, (ui32)Max<i16>());
            if (DefaultFullThreadCount) {
                DefaultFullThreadCount = Min<i16>(DefaultFullThreadCount - HasOwnSharedThread, limit);
            } else {
                DefaultFullThreadCount = limit;
            }

            MaxFullThreadCount = Min(Max<i16>(MaxFullThreadCount - HasOwnSharedThread, DefaultFullThreadCount), limit);

            if (MinFullThreadCount) {
                MinFullThreadCount = Min<i16>(MinFullThreadCount - HasOwnSharedThread, DefaultFullThreadCount);
            } else {
                MinFullThreadCount = DefaultFullThreadCount;
            }
        }

        ThreadCount = static_cast<i16>(MaxFullThreadCount);
        SuggestedThreadCount = ThreadCount;
        auto semaphore = TSemaphore();
        semaphore.CurrentThreadCount = ThreadCount;
        Semaphore = semaphore.ConvertToI64();

        DefaultThreadCount = DefaultFullThreadCount + HasOwnSharedThread;
        MinThreadCount = MinFullThreadCount + HasOwnSharedThread;
        MaxThreadCount = MaxFullThreadCount + HasOwnSharedThread;

        if (SharedOnly) {
            MaxThreadCount = cfg.ForcedForeignSlotCount + 1;
        }

        Threads.Reset(new NThreading::TPadded<TExecutorThreadCtx>[MaxFullThreadCount]);
        if (EnableWaker) {
            Y_ABORT_UNLESS(!HasOwnSharedThread && !SharedOnly,
                "EnableWaker is supported only for non-shared Basic executor pools");
            Waker = std::make_unique<TWaker>(this);
        }
        if constexpr (DebugMode) {
            Sanitizer.reset(new TBasicExecutorPoolSanitizer(this));
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "ThreadCount == ", ThreadCount, " DefaultThreadCount == ", DefaultThreadCount, " MinThreadCount == ", MinThreadCount, " MaxThreadCount == ", MaxThreadCount, " DefaultFullThreadCount == ", DefaultFullThreadCount, " MinFullThreadCount == ", MinFullThreadCount, " MaxFullThreadCount == ", MaxFullThreadCount);
    }

    TBasicExecutorPool::~TBasicExecutorPool() {
        Threads.Destroy();
    }

    bool TBasicExecutorPool::IsSharedOnly() const {
        return SharedOnly;
    }

    void TBasicExecutorPool::AskToGoToSleep(bool *needToWait, bool *needToBlock) {
        TAtomic x = AtomicGet(Semaphore);
        do {
            i64 oldX = x;
            TSemaphore semaphore = TSemaphore::GetSemaphore(x);;
            if (semaphore.CurrentSleepThreadCount < 0) {
                semaphore.CurrentSleepThreadCount++;
                x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), x);
                if (x == oldX) {
                    *needToWait = true;
                    *needToBlock = true;
                    return;
                }
                continue;
            }

            if (semaphore.OldSemaphore == 0) {
                semaphore.CurrentSleepThreadCount++;
                if (semaphore.CurrentSleepThreadCount == AtomicLoad(&ThreadCount)) {
                    AllThreadsSleep.store(true);
                }
                x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), x);
                if (x == oldX) {
                    *needToWait = true;
                    *needToBlock = false;
                    return;
                }
                continue;
            }

            *needToWait = false;
            *needToBlock = false;
            return;
        } while (true);
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationRingQueue(ui64 revolvingCounter) {
        if (StopFlag.load(std::memory_order_acquire)) {
            return nullptr;
        }

        TWorkerId workerId = TlsThreadContext->WorkerId();
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "");
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        if (Harmonizer) {
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "try to harmonize");
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "harmonize done");
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
                    if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
                        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "activation found");
                        Threads[workerId].SetWork();
                        AtomicDecrement(Semaphore);
                        return MailboxTable->Get(activation);
                    }
                }
            }

            TAtomic semaphoreRaw = AtomicGet(Semaphore);
            TSemaphore semaphore = TSemaphore::GetSemaphore(semaphoreRaw);
            if (!semaphore.OldSemaphore || workerId >= 0 && semaphore.CurrentSleepThreadCount < 0) {
                EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "semaphore.OldSemaphore == 0 or workerId >= 0 && semaphore.CurrentSleepThreadCount < 0");
                if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                    EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "wctx.ExecutionContext.IsNeededToWaitNextActivation == false");
                    return nullptr;
                }

                bool needToWait = false;
                bool needToBlock = false;
                AskToGoToSleep(&needToWait, &needToBlock);
                if (needToWait) {
                    EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "go to sleep");
                    if (Threads[workerId].Wait(SpinThresholdCycles, &StopFlag)) {
                        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "sleep interrupted");
                        return nullptr;
                    }
                }
            }
            SpinLockPause();
        }

        return nullptr;
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationWaker(ui64 revolvingCounter) {
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
                if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
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

    TMailbox* TBasicExecutorPool::GetReadyActivation(ui64 revolvingCounter) {
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "ring queue");
        if (EnableWaker) {
            return GetReadyActivationWaker(revolvingCounter);
        }
        return GetReadyActivationRingQueue(revolvingCounter);
    }

    bool TBasicExecutorPool::TryRequestWaker(bool requireSleepingWorkers) {
        const i16 owner = WakerWorkerId.load(std::memory_order_acquire);
        if (owner != InvalidWakerWorkerId) {
            return true;
        }

        if (requireSleepingWorkers && SleepingCount.load(std::memory_order_acquire) == 0) {
            return false;
        }

        for (i16 workerId = 0; workerId < MaxFullThreadCount; ++workerId) {
            EThreadState state = Threads[workerId].GetState<EThreadState>();
            while (true) {
                if (IsNeedToBeWaker(state) || state == EThreadState::Waker) {
                    return true;
                }
                if (state != EThreadState::Spin && state != EThreadState::Blocking && state != EThreadState::Sleep) {
                    break;
                }
                const EThreadState requestedState = state;
                if (Threads[workerId].TrySetNeedToBeWaker(&state)) {
                    if (requestedState == EThreadState::Blocking || requestedState == EThreadState::Sleep) {
                        Threads[workerId].WaitingPad.Unpark();
                    }
                    return true;
                }
            }
        }
        return false;
    }

    void TBasicExecutorPool::RequestWaker(bool persistent) {
        const bool wasPending = WakerPending.exchange(true, std::memory_order_acq_rel);
        if (wasPending) {
            return;
        }

        if (!persistent) {
            if (TryRequestWaker(true)) {
                return;
            }

            while (true) {
                if (SleepingCount.load(std::memory_order_acquire) == 0) {
                    const ui64 reductions = CheckToSleepWorkers.fetch_or(WakerRequestBit, std::memory_order_acq_rel);
                    if (reductions & WakerRequestBit || SleepingCount.load(std::memory_order_acquire) == 0) {
                        return;
                    }
                }
                if (TryRequestWaker(true)) {
                    return;
                }
            }
        }

        bool requestBitSet = false;
        if (AtomicLoad(&SuggestedThreadCount) <= AtomicLoad(&ThreadCount) &&
                SleepingCount.load(std::memory_order_acquire) == 0) {
            const ui64 reductions = CheckToSleepWorkers.fetch_or(WakerRequestBit, std::memory_order_acq_rel);
            if (reductions & WakerRequestBit) {
                return;
            }
            requestBitSet = true;
            if (SleepingCount.load(std::memory_order_acquire) == 0) {
                return;
            }
        }

        if (TryRequestWaker(false)) {
            return;
        }
        if (!requestBitSet) {
            CheckToSleepWorkers.fetch_or(WakerRequestBit, std::memory_order_acq_rel);
        }
    }

    void TBasicExecutorPool::RunWaker(TWorkerId workerId) {
        EThreadState resumeState = EThreadState::None;
        bool hasResumeState = false;

        while (!StopFlag.load(std::memory_order_acquire)) {
            const EThreadState state = Threads[workerId].GetState<EThreadState>();
            if (IsNeedToBeWaker(state)) {
                const i16 owner = WakerWorkerId.load(std::memory_order_acquire);
                if (owner == InvalidWakerWorkerId) {
                    i16 expected = InvalidWakerWorkerId;
                    if (!WakerWorkerId.compare_exchange_weak(expected, workerId,
                            std::memory_order_acq_rel, std::memory_order_acquire)) {
                        continue;
                    }
                    Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                    hasResumeState = true;
                    continue;
                }
                if (owner == workerId) {
                    Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                    hasResumeState = true;
                    continue;
                }
                Threads[workerId].CancelWakerRequest();
                return;
            }

            if (state != EThreadState::Waker) {
                return;
            }

            Y_ABORT_UNLESS(hasResumeState);
            Y_ABORT_UNLESS(WakerWorkerId.load(std::memory_order_acquire) == workerId);
            WakerLoop(workerId, &resumeState);

            if (WakerPending.load(std::memory_order_acquire)) {
                continue;
            }

            EThreadState finalState = resumeState;
            EThreadState expectedState = EThreadState::Waker;
            Y_ABORT_UNLESS(Threads[workerId].ReplaceState(expectedState, finalState));

            if (WakerPending.load(std::memory_order_acquire)) {
                Y_ABORT_UNLESS(Threads[workerId].TrySetNeedToBeWaker(&finalState));
                Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                continue;
            }

            i16 expectedOwner = workerId;
            Y_ABORT_UNLESS(WakerWorkerId.compare_exchange_strong(expectedOwner, InvalidWakerWorkerId,
                std::memory_order_acq_rel, std::memory_order_acquire));

            if (WakerPending.load(std::memory_order_acquire)) {
                EThreadState currentState = Threads[workerId].GetState<EThreadState>();
                if (IsNeedToBeWaker(currentState) || currentState == EThreadState::Waker) {
                    continue;
                }
                if (currentState != EThreadState::Work &&
                        Threads[workerId].TrySetNeedToBeWaker(&currentState)) {
                    continue;
                }
            }
            return;
        }
    }

    void TBasicExecutorPool::WakerLoop(TWorkerId wakerWorkerId, EThreadState* resumeState) {
        Y_ABORT_UNLESS(resumeState);
        EThreadState wakerState = *resumeState;
        const auto getLogicalState = [](EThreadState state) {
            switch (state) {
                case EThreadState::NeedToBeWaker:
                    return EThreadState::None;
                case EThreadState::NeedToBeWakerFromSpin:
                    return EThreadState::Spin;
                case EThreadState::NeedToBeWakerFromSleep:
                    return EThreadState::Sleep;
                case EThreadState::NeedToBeWakerFromBlocking:
                    return EThreadState::Blocking;
                default:
                    return state;
            }
        };

        i16 previousSleepingCount = SleepingCount.exchange(0, std::memory_order_acq_rel);
        WakerPending.store(false, std::memory_order_release);
        ui64 remainingReductions = CheckToSleepWorkers.exchange(0, std::memory_order_acq_rel) & WakerReductionMask;
        Y_ABORT_UNLESS(Waker->PreviousReductions >= remainingReductions);
        const ui64 claimedReductions = Waker->PreviousReductions - remainingReductions;
        Y_ABORT_UNLESS(Waker->TakenTokensToSleep + Waker->TakenTokensToWakeup + claimedReductions
            <= static_cast<ui64>(MaxFullThreadCount));
        Waker->TakenTokensToSleep += claimedReductions;
        Waker->PreviousReductions = 0;

        const i16 desiredThreadCount = AtomicLoad(&SuggestedThreadCount);
        const i16 previousThreadCount = AtomicLoad(&ThreadCount);
        if (desiredThreadCount < previousThreadCount) {
            ui64 delta = previousThreadCount - desiredThreadCount;
            ui64 converted = Min(Waker->TakenTokensToWakeup, delta);
            Waker->TakenTokensToWakeup -= converted;
            Waker->TakenTokensToSleep += converted;
            delta -= converted;

            converted = Min(static_cast<ui64>(previousSleepingCount), delta);
            previousSleepingCount -= static_cast<i16>(converted);
            delta -= converted;

            Y_ABORT_UNLESS(remainingReductions + delta <= static_cast<ui64>(MaxFullThreadCount));
            remainingReductions += delta;
        } else if (desiredThreadCount > previousThreadCount) {
            ui64 delta = desiredThreadCount - previousThreadCount;
            ui64 converted = Min(Waker->TakenTokensToSleep, delta);
            Waker->TakenTokensToSleep -= converted;
            Waker->TakenTokensToWakeup += converted;
            delta -= converted;

            converted = Min(remainingReductions, delta);
            remainingReductions -= converted;
            delta -= converted;

            Y_ABORT_UNLESS(static_cast<ui64>(previousSleepingCount) + delta <= Waker->SleepingStack.size());
            previousSleepingCount += static_cast<i16>(delta);
        }
        AtomicSet(ThreadCount, desiredThreadCount);

        const i64 previousActivationCredits = ActivationCredits.load(std::memory_order_acquire);
        i64 budget = previousActivationCredits;
        for (i16 workerId = 0; workerId < MaxFullThreadCount; ++workerId) {
            const bool isWaker = workerId == wakerWorkerId;
            EThreadState state = wakerState;
            EThreadState logicalState = wakerState;
            if (!isWaker) {
                state = Threads[workerId].GetState<EThreadState>();
                logicalState = getLogicalState(state);
            }

            if (logicalState == EThreadState::None) {
                if (isWaker) {
                    wakerState = EThreadState::None;
                    continue;
                }
                EThreadState expected = state;
                if (Threads[workerId].ReplaceState(expected, EThreadState::None) && budget > 0) {
                    --budget;
                }
                continue;
            }

            if (logicalState == EThreadState::Spin) {
                const bool consumeReduction = remainingReductions > 0;
                const EThreadState targetState = consumeReduction || budget == 0
                    ? EThreadState::Sleep
                    : EThreadState::None;
                if (isWaker) {
                    wakerState = targetState;
                    if (targetState == EThreadState::Sleep) {
                        Waker->SleepingStack.push_back(workerId);
                        if (consumeReduction) {
                            --remainingReductions;
                        } else {
                            ++previousSleepingCount;
                        }
                    }
                    continue;
                }
                EThreadState expected = state;
                if (Threads[workerId].ReplaceState(expected, targetState)) {
                    if (targetState == EThreadState::None) {
                        --budget;
                    } else {
                        Waker->SleepingStack.push_back(workerId);
                        if (consumeReduction) {
                            --remainingReductions;
                        } else {
                            ++previousSleepingCount;
                        }
                    }
                }
                continue;
            }

            if (logicalState != EThreadState::Blocking) {
                continue;
            }

            EThreadState targetState;
            bool useWakeupToken = false;
            bool useSleepToken = false;
            bool countAsSleeping = false;
            if (Waker->TakenTokensToWakeup > 0) {
                useWakeupToken = true;
                if (budget > 0) {
                    targetState = EThreadState::None;
                } else {
                    targetState = EThreadState::Sleep;
                    countAsSleeping = true;
                }
            } else {
                Y_ABORT_UNLESS(Waker->TakenTokensToSleep > 0);
                useSleepToken = true;
                targetState = EThreadState::Sleep;
            }

            bool replaced = true;
            if (isWaker) {
                wakerState = targetState;
            } else {
                EThreadState expected = state;
                replaced = Threads[workerId].ReplaceState(expected, targetState);
            }
            if (!replaced) {
                continue;
            }

            if (useWakeupToken) {
                --Waker->TakenTokensToWakeup;
            }
            if (useSleepToken) {
                --Waker->TakenTokensToSleep;
            }
            if (targetState == EThreadState::None) {
                if (!isWaker) {
                    --budget;
                    Threads[workerId].WaitingPad.Unpark();
                }
            } else {
                Waker->SleepingStack.push_back(workerId);
                if (countAsSleeping) {
                    ++previousSleepingCount;
                }
            }
        }

        ui64 currentReductions = CheckToSleepWorkers.load(std::memory_order_acquire);
        while (true) {
            Y_ABORT_UNLESS((currentReductions & WakerReductionMask) == 0);
            if (CheckToSleepWorkers.compare_exchange_weak(currentReductions, remainingReductions,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        Waker->PreviousReductions = remainingReductions;

        while (budget > 0 && previousSleepingCount > 0) {
            bool wokeWorker = false;
            for (size_t idx = Waker->SleepingStack.size(); idx > 0; --idx) {
                const i16 workerId = Waker->SleepingStack[idx - 1];
                if (workerId == wakerWorkerId) {
                    if (wakerState != EThreadState::Sleep) {
                        continue;
                    }
                    wakerState = EThreadState::None;
                } else {
                    EThreadState state = Threads[workerId].GetState<EThreadState>();
                    const EThreadState logicalState = getLogicalState(state);
                    if (logicalState != EThreadState::Sleep) {
                        continue;
                    }
                    EThreadState expected = state;
                    if (!Threads[workerId].ReplaceState(expected, EThreadState::None)) {
                        continue;
                    }
                    --budget;
                    Threads[workerId].WaitingPad.Unpark();
                }
                Waker->SleepingStack.erase(Waker->SleepingStack.begin() + idx - 1);
                --previousSleepingCount;
                wokeWorker = true;
                break;
            }
            if (!wokeWorker) {
                break;
            }
        }

        SleepingCount.store(previousSleepingCount, std::memory_order_release);

        // A producer publishes its credit before the corresponding queue item.
        // If the credit appeared while SleepingCount was hidden, repeat the pass.
        if (ActivationCredits.load(std::memory_order_acquire) > previousActivationCredits) {
            WakerPending.store(true, std::memory_order_release);
        }
        *resumeState = wakerState;
    }

    inline void TBasicExecutorPool::WakeUpLoop(i16 currentThreadCount) {
        for (i16 i = 0;;) {
            if (Threads[i].WakeUp()) {
                if (i >= currentThreadCount) {
                    AtomicIncrement(WrongWakenedThreadCount);
                }
                return;
            }
            if (++i >= MaxFullThreadCount) {
                i = 0;
            }
        }
    }

    void TBasicExecutorPool::ScheduleActivationExRingQueue(TMailbox* mailbox, ui64 revolvingCounter, std::optional<TAtomic> initSemaphore) {
        Activations.Push(mailbox->Hint, revolvingCounter);
        bool needToWakeUp = false;
        bool needToChangeOldSemaphore = true;

        TAtomic x;
        TSemaphore semaphore;
        if (!initSemaphore || SharedPool) {
            x = AtomicIncrement(Semaphore);
            needToChangeOldSemaphore = false;
            semaphore = TSemaphore::GetSemaphore(x);
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "Semaphore incremented to ", semaphore.OldSemaphore, " CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
        } else {
            x = *initSemaphore;
            semaphore = TSemaphore::GetSemaphore(x);
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "semaphore.OldSemaphore == ", semaphore.OldSemaphore, " semaphore.CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
        if (SharedPool) {
            if (SharedPool->WakeUpLocalThreads(PoolId)) {
                EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "shared pool wake up local threads");
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
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "need to wake up");
            WakeUpLoop(semaphore.CurrentThreadCount);
        } else if (SharedPool) {
            if (SharedPool->WakeUpAdjacentOwner(PoolId)) {
                EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "shared pool wake up adjacent owner");
            } else {
                EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "shared pool wake up global threads");
                SharedPool->WakeUpGlobalThreads(PoolId);
            }
        }
    }

    void TBasicExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) {
        if (EnableWaker) {
            ScheduleActivationExWaker(mailbox, revolvingCounter);
            return;
        }
        ScheduleActivationExRingQueue(mailbox, revolvingCounter, std::nullopt);
    }

    void TBasicExecutorPool::ScheduleActivationExWaker(TMailbox* mailbox, ui64 revolvingCounter) {
        ActivationCredits.fetch_add(1, std::memory_order_acq_rel);
        Activations.Push(mailbox->Hint, revolvingCounter);
        if (SleepingCount.load(std::memory_order_acquire) > 0) {
            RequestWaker(false);
        }
    }

    void TBasicExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.MaxUtilizationTime = RelaxedLoad(&MaxUtilizationAccumulator) / (i64)(NHPTimer::GetCyclesPerSecond() / 1000);
        poolStats.WrongWakenedThreadCount = RelaxedLoad(&WrongWakenedThreadCount);
        poolStats.CurrentThreadCount = GetThreadCount();
        poolStats.DefaultThreadCount = GetDefaultThreadCount();
        poolStats.MaxThreadCount = GetMaxThreadCount();
        poolStats.SpinningTimeUs = Ts2Us(SpinningTimeUs);
        poolStats.SpinThresholdUs = Ts2Us(SpinThresholdCycles);
        if (Harmonizer) {
            TPoolHarmonizerStats stats = Harmonizer->GetPoolStats(PoolId);
            poolStats.IsNeedy = stats.IsNeedy;
            poolStats.IsStarved = stats.IsStarved;
            poolStats.IsHoggish = stats.IsHoggish;
            poolStats.IncreasingThreadsByNeedyState = stats.IncreasingThreadsByNeedyState;
            poolStats.IncreasingThreadsByExchange = stats.IncreasingThreadsByExchange;
            poolStats.DecreasingThreadsByStarvedState = stats.DecreasingThreadsByStarvedState;
            poolStats.DecreasingThreadsByHoggishState = stats.DecreasingThreadsByHoggishState;
            poolStats.DecreasingThreadsByExchange = stats.DecreasingThreadsByExchange;
            poolStats.PotentialMaxThreadCount = stats.PotentialMaxThreadCount;
        }

        statsCopy.resize(MaxFullThreadCount + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        RecalculateStuckActors(statsCopy[0]);
#endif
        // Per-thread stats
        for (i16 i = 0; i < MaxFullThreadCount; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    void TBasicExecutorPool::GetExecutorPoolState(TExecutorPoolState &poolState) const {
        poolState.CurrentLimit = GetThreadCount();
        poolState.MaxLimit = GetMaxThreadCount();
        poolState.MinLimit = GetMinThreadCount();

        if (Harmonizer) {
            TPoolHarmonizerStats stats = Harmonizer->GetPoolStats(PoolId);
            poolState.ElapsedCpu = stats.AvgElapsedCpu;
            poolState.PossibleMaxLimit = stats.PotentialMaxThreadCount;
            poolState.SharedCpuQuota = stats.SharedCpuQuota;
            poolState.IsNeedy = stats.IsNeedy;
            poolState.IsStarved = stats.IsStarved;
            poolState.IsHoggish = stats.IsHoggish;
        } else {
            poolState.PossibleMaxLimit = poolState.MaxLimit;
        }
    }

    void TBasicExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "");

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[MaxFullThreadCount]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[MaxFullThreadCount]);


        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread.reset(
                new TExecutorThread(
                    i,
                    actorSystem,
                    this,
                    PoolName));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }


        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = MaxFullThreadCount;
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "prepared");
    }

    void TBasicExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "max threads: ", MaxFullThreadCount);
        ThreadUtilization = 0;
        AtomicAdd(MaxUtilizationCounter, -(i64)GetCycleCountFast());

        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread->Start();
        }

        if constexpr (DebugMode) {
            Sanitizer->Start();
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "started");
    }

    void TBasicExecutorPool::PrepareStop() {
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "stop flag set");
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
        }
        if constexpr (DebugMode) {
            Sanitizer->Stop();
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "stopped");
    }

    void TBasicExecutorPool::Shutdown() {
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "shutdown");
        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "join ", i);
            Threads[i].Thread->Join();
        }
        if constexpr (DebugMode) {
            EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "join sanitizer");
            Sanitizer->Join();
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::ExecutorPool, "shutdown done");
    }

    void TBasicExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TBasicExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        if (TlsThreadContext && TlsThreadContext->IsShared()) {
            TlsThreadContext->SharedPool()->Schedule(deadline, ev, cookie, workerId);
        } else {
            Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        const auto deadline = ActorSystem->Monotonic() + delta;
        if (TlsThreadContext && TlsThreadContext->IsShared()) {
            TlsThreadContext->SharedPool()->Schedule(deadline, ev, cookie, workerId);
        } else {
            Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::SetRealTimeMode() const {
// TODO: musl-libc version of `sched_param` struct is for some reason different from pthread
// version in Ubuntu 12.04
#if defined(_linux_) && !defined(_musl_)
        if (RealtimePriority != 0) {
            pthread_t threadSelf = pthread_self();
            sched_param param = {RealtimePriority};
            if (pthread_setschedparam(threadSelf, SCHED_FIFO, &param)) {
                Y_ABORT("Cannot set realtime priority");
            }
        }
#else
        Y_UNUSED(RealtimePriority);
#endif
    }

    float TBasicExecutorPool::GetThreadCount() const {
        return GetFullThreadCount() + SharedCpuQuota.load(std::memory_order_relaxed);
    }

    i16 TBasicExecutorPool::GetFullThreadCount() const {
        return AtomicGet(ThreadCount);
    }

    void TBasicExecutorPool::SetFullThreadCount(i16 threads) {
        threads = Max<i16>(MinFullThreadCount, Min(MaxFullThreadCount, threads));
        with_lock (ChangeThreadsLock) {
            if (EnableWaker) {
                if (AtomicLoad(&SuggestedThreadCount) != threads) {
                    AtomicSet(SuggestedThreadCount, threads);
                    RequestWaker(true);
                }
                LWPROBE(ThreadCount, PoolId, PoolName, threads, MinThreadCount, MaxThreadCount, DefaultThreadCount);
                return;
            }
            i16 prevCount = GetFullThreadCount();
            AtomicSet(ThreadCount, threads);
            TSemaphore semaphore = TSemaphore::GetSemaphore(AtomicGet(Semaphore));
            i64 oldX = semaphore.ConvertToI64();
            semaphore.CurrentThreadCount = threads;
            if (threads > prevCount) {
                semaphore.CurrentSleepThreadCount += (i64)threads - prevCount;
            } else {
                semaphore.CurrentSleepThreadCount -= (i64)prevCount - threads;
                CheckToSleepWorkers.fetch_add(prevCount - threads, std::memory_order_release);
            }
            AtomicAdd(Semaphore, semaphore.ConvertToI64() - oldX);
            LWPROBE(ThreadCount, PoolId, PoolName, threads, MinThreadCount, MaxThreadCount, DefaultThreadCount);
        }
    }

    float TBasicExecutorPool::GetDefaultThreadCount() const {
        return DefaultThreadCount;
    }

    i16 TBasicExecutorPool::GetDefaultFullThreadCount() const {
        return DefaultFullThreadCount;
    }

    float TBasicExecutorPool::GetMinThreadCount() const {
        return MinThreadCount;
    }

    i16 TBasicExecutorPool::GetMinFullThreadCount() const {
        return MinFullThreadCount;
    }

    float TBasicExecutorPool::GetMaxThreadCount() const {
        return MaxThreadCount;
    }

    i16 TBasicExecutorPool::GetMaxFullThreadCount() const {
        return MaxFullThreadCount;
    }

    ui32 TBasicExecutorPool::GetThreads() const {
        return MaxFullThreadCount;
    }

    TCpuConsumption TBasicExecutorPool::GetThreadCpuConsumption(i16 threadIdx) {
        if (threadIdx >= MaxFullThreadCount) {
            return {0.0, 0.0};
        }
        TExecutorThreadCtx& threadCtx = Threads[threadIdx];
        TExecutorThreadStats stats;
        threadCtx.Thread->GetCurrentStatsForHarmonizer(stats);
        return {static_cast<double>(stats.CpuUs), Ts2Us(stats.SafeElapsedTicks), stats.NotEnoughCpuExecutions};
    }

    i16 TBasicExecutorPool::GetBlockingThreadCount() const {
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        return -Min<i16>(semaphore.CurrentSleepThreadCount, 0);
    }

    i16 TBasicExecutorPool::GetPriority() const {
        return Priority;
    }

    void TBasicExecutorPool::Initialize() {
        TlsThreadContext->WaitingStats = &WaitingStats[TlsThreadContext->WorkerId()];
    }

    void TBasicExecutorPool::SetSpinThresholdCycles(ui32 cycles) {
        if (ActorSystemProfile == EASProfile::LowLatency) {
            if (DefaultSpinThresholdCycles > cycles) {
                cycles = DefaultSpinThresholdCycles;
            }
        }
        SpinThresholdCycles = cycles;
        double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
        ui32 bucketIdx = cycles / TWaitingStatsConstants::HistogramResolution;
        LWPROBE(ChangeSpinThreshold, PoolId, PoolName, cycles, resolutionUs * bucketIdx, bucketIdx);
    }

    void TBasicExecutorPool::GetWaitingStats(TWaitingStats<ui64> &acc) const {
        acc.Clear();
        double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
        for (ui32 idx = 0; idx < ThreadCount; ++idx) {
            for (ui32 bucketIdx = 0; bucketIdx < TWaitingStatsConstants::BucketCount; ++bucketIdx) {
                LWPROBE(WaitingHistogramPerThread, PoolId, PoolName, idx, resolutionUs * bucketIdx, resolutionUs * (bucketIdx + 1), WaitingStats[idx].WaitingUntilNeedsTimeHist[bucketIdx].load());
            }
            acc.Add(WaitingStats[idx]);
        }
        for (ui32 bucketIdx = 0; bucketIdx < TWaitingStatsConstants::BucketCount; ++bucketIdx) {
            LWPROBE(WaitingHistogram, PoolId, PoolName, resolutionUs * bucketIdx, resolutionUs * (bucketIdx + 1), acc.WaitingUntilNeedsTimeHist[bucketIdx].load());
        }
    }

    void TBasicExecutorPool::ClearWaitingStats() const {
        for (ui32 idx = 0; idx < ThreadCount; ++idx) {
            WaitingStats[idx].Clear();
        }
    }

    void TBasicExecutorPool::CalcSpinPerThread(ui64 wakingUpConsumption) {
        if (ActorSystemProfile == EASProfile::Default) {
            return;
        }
        for (i16 threadIdx = 0; threadIdx < MaxFullThreadCount; ++threadIdx) {
            ui64 newSpinThreshold = 0;
            if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                MovingWaitingStats[threadIdx].Add(WaitingStats[threadIdx], 0.8, 0.2);
                newSpinThreshold = MovingWaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            } else {
                newSpinThreshold = WaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            }

            if (ActorSystemProfile == EASProfile::LowCpuConsumption) {
                SpinThresholdCyclesPerThread[threadIdx].store(newSpinThreshold, std::memory_order_release);
            } else {
                auto oldSpinThreshold = SpinThresholdCyclesPerThread[threadIdx].load(std::memory_order_acquire);
                if (oldSpinThreshold < newSpinThreshold) {
                    SpinThresholdCyclesPerThread[threadIdx].store(newSpinThreshold, std::memory_order_release);
                }
            }

            double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
            ui32 bucketIdx = newSpinThreshold / TWaitingStatsConstants::HistogramResolution;
            LWPROBE(ChangeSpinThresholdPerThread, PoolId, PoolName, threadIdx, newSpinThreshold, resolutionUs * bucketIdx, bucketIdx);
        }
    }

    TBasicExecutorPool::TSemaphore TBasicExecutorPool::GetSemaphore() const {
        return TSemaphore::GetSemaphore(AtomicGet(Semaphore));
    }

    void TBasicExecutorPool::SetSharedPool(TSharedExecutorPool* pool) {
        SharedPool = pool;
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationShared(ui64 revolvingCounter) {
        TWorkerId workerId = TlsThreadContext->WorkerId();
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        SharedPool->Threads[workerId].UnsetWork();
        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
        }
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "revolvingCounter == ", revolvingCounter, " semaphore == ", semaphore.OldSemaphore);
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (!semaphore.OldSemaphore) {
                EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Executor, "semaphore == 0");
                return nullptr;
            } else {
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                if (const ui32 activation = Activations.Pop(revolvingCounter++)) {
                    SharedPool->Threads[workerId].SetWork();
                    AtomicDecrement(Semaphore);
                    EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Activation, "activation == ", activation, " semaphore == ", semaphore.OldSemaphore);
                    return MailboxTable->Get(activation);
                }
            }

            SpinLockPause();
            x = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(x);
        }
        EXECUTOR_POOL_BASIC_DEBUG(EDebugLevel::Executor, "stop");
        return nullptr;
    }

    void TBasicExecutorPool::SetSharedCpuQuota(float quota) {
        SharedCpuQuota.store(quota, std::memory_order_release);
    }

    ui64 TBasicExecutorPool::TimePerMailboxTs() const {
        return TimePerMailboxTsValue;
    }

    ui32 TBasicExecutorPool::EventsPerMailbox() const {
        return EventsPerMailboxValue;
    }

}
