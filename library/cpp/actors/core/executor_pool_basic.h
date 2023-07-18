#pragma once

#include "actorsystem.h"
#include "executor_thread.h"
#include "scheduler_queue.h"
#include "executor_pool_base.h"
#include "harmonizer.h"
#include <library/cpp/actors/actor_type/indexes.h>
#include <library/cpp/actors/util/unordered_cache.h>
#include <library/cpp/actors/util/threadparkpad.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>

namespace NActors {
    class TBasicExecutorPool: public TExecutorPoolBase {
        struct TThreadCtx {
            TAutoPtr<TExecutorThread> Thread;
            TThreadParkPad Pad;
            TThreadParkPad BlockedPad;
            TAtomic WaitingFlag;
            TAtomic BlockedFlag;

            // different threads must spin/block on different cache-lines.
            // we add some padding bytes to enforce this rule
            static const size_t SizeWithoutPadding = sizeof(TAutoPtr<TExecutorThread>) + 2 * sizeof(TThreadParkPad) + 2 * sizeof(TAtomic);
            ui8 Padding[64 - SizeWithoutPadding];
            static_assert(64 >= SizeWithoutPadding);

            enum EWaitState {
                WS_NONE,
                WS_ACTIVE,
                WS_BLOCKED,
                WS_RUNNING
            };

            enum EBlockedState {
                BS_NONE,
                BS_BLOCKING,
                BS_BLOCKED
            };

            TThreadCtx()
                : WaitingFlag(WS_NONE)
                , BlockedFlag(BS_NONE)
            {
            }
        };

        struct TTimers {
            NHPTimer::STime Elapsed = 0;
            NHPTimer::STime Parked = 0;
            NHPTimer::STime Blocked = 0;
            NHPTimer::STime HPStart = GetCycleCountFast();
            NHPTimer::STime HPNow;
        };

        const ui64 SpinThreshold;
        const ui64 SpinThresholdCycles;

        TArrayHolder<TThreadCtx> Threads;

        TArrayHolder<NSchedulerQueue::TReader> ScheduleReaders;
        TArrayHolder<NSchedulerQueue::TWriter> ScheduleWriters;

        const TString PoolName;
        const TDuration TimePerMailbox;
        const ui32 EventsPerMailbox;

        const int RealtimePriority;

        TAtomic ThreadUtilization;
        TAtomic MaxUtilizationCounter;
        TAtomic MaxUtilizationAccumulator;
        TAtomic WrongWakenedThreadCount;

        TAtomic ThreadCount;
        TMutex ChangeThreadsLock;

        i16 MinThreadCount;
        i16 MaxThreadCount;
        i16 DefaultThreadCount;
        IHarmonizer *Harmonizer;

        const i16 Priority = 0;
        const ui32 ActorSystemIndex = NActors::TActorTypeOperator::GetActorSystemIndex();
    public:
        struct TSemaphore {
            i64 OldSemaphore = 0; // 34 bits
            // Sign bit
            i16 CurrentSleepThreadCount = 0; // 14 bits
            // Sign bit
            i16 CurrentThreadCount = 0; // 14 bits

            inline i64 ConverToI64() {
                i64 value = (1ll << 34) + OldSemaphore;
                return value
                    | (((i64)CurrentSleepThreadCount + (1 << 14)) << 35)
                    | ((i64)CurrentThreadCount << 50);
            }

            static inline TSemaphore GetSemaphore(i64 value) {
                TSemaphore semaphore;
                semaphore.OldSemaphore = (value & 0x7ffffffffll) - (1ll << 34);
                semaphore.CurrentSleepThreadCount = ((value >> 35) & 0x7fff) - (1 << 14);
                semaphore.CurrentThreadCount = (value >> 50) & 0x3fff;
                return semaphore;
            }
        };

        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_TIME_PER_MAILBOX;
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX;

        TBasicExecutorPool(ui32 poolId,
                           ui32 threads,
                           ui64 spinThreshold,
                           const TString& poolName = "",
                           IHarmonizer *harmonizer = nullptr,
                           TAffinity* affinity = nullptr,
                           TDuration timePerMailbox = DEFAULT_TIME_PER_MAILBOX,
                           ui32 eventsPerMailbox = DEFAULT_EVENTS_PER_MAILBOX,
                           int realtimePriority = 0,
                           ui32 maxActivityType = 0 /* deprecated */,
                           i16 minThreadCount = 0,
                           i16 maxThreadCount = 0,
                           i16 defaultThreadCount = 0,
                           i16 priority = 0);
        explicit TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer);
        ~TBasicExecutorPool();

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingReadCounter) override;

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void ScheduleActivationEx(ui32 activation, ui64 revolvingWriteCounter) override;

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;
        TString GetName() const override {
            return PoolName;
        }

        void SetRealTimeMode() const override;

        ui32 GetThreadCount() const override;
        void SetThreadCount(ui32 threads) override;
        i16 GetDefaultThreadCount() const override;
        i16 GetMinThreadCount() const override;
        i16 GetMaxThreadCount() const override;
        bool IsThreadBeingStopped(i16 threadIdx) const override;
        TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) override;
        i16 GetBlockingThreadCount() const override;
        i16 GetPriority() const override;

    private:
        void WakeUpLoop(i16 currentThreadCount);
        bool GoToWaiting(TThreadCtx& threadCtx, TTimers &timers, bool needToBlock);
        void GoToSpin(TThreadCtx& threadCtx);
        bool GoToSleep(TThreadCtx& threadCtx, TTimers &timers);
        bool GoToBeBlocked(TThreadCtx& threadCtx, TTimers &timers);
    };
}
