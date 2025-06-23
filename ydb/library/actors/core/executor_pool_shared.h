#pragma once

#include "actorsystem.h"
#include "config.h"
#include "executor_thread.h"
#include "executor_thread_ctx.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_base.h"
#include "scheduler_queue.h"
#include <memory>
#include <ydb/library/actors/actor_type/indexes.h>
#include <ydb/library/actors/util/unordered_cache.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/threading/chunk_queue/queue.h>

#include <util/system/mutex.h>

#include <queue>

namespace NActors {

    class TExecutorPoolJail;
    class TSharedExecutorPoolSanitizer;

    class TBasicExecutorPool;

    struct TPoolShortInfo {
        i16 PoolId = 0;
        i16 SharedThreadCount = 0;
        i16 ForeignSlots = 0;
        bool InPriorityOrder = false;
        TString PoolName;
        bool ForcedForeignSlots = false;
        std::vector<i16> AdjacentPools;
    };

    struct TPoolThreadRange {
        i16 Begin;
        i16 End;
    };

    struct TPoolManager {
        std::vector<TPoolShortInfo> PoolInfos;
        TStackVec<TPoolThreadRange, 8> PoolThreadRanges;
        TStackVec<i16, 8> PriorityOrder;

        TPoolManager(const std::vector<TPoolShortInfo> &poolInfos);
    };

    class ISharedPool {
    protected:
        virtual ~ISharedPool() = default;

    public:
        virtual void GetSharedStatsForHarmonizer(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const = 0;
        virtual void GetSharedStats(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const = 0;
        virtual void FillThreadOwners(std::vector<i16>& threadOwners) const = 0;
        virtual void FillForeignThreadsAllowed(std::vector<i16>& foreignThreadsAllowed) const = 0;
        virtual void FillOwnedThreads(std::vector<i16>& ownedThreads) const = 0;
        virtual i16 GetSharedThreadCount() const = 0;
        virtual void SetForeignThreadSlots(i16 poolId, i16 slots) = 0;
    };

    class TSharedExecutorPool: public TExecutorPoolBaseMailboxed, public ISharedPool {
        friend class TBasicExecutorPool;
        friend class TSharedExecutorPoolSanitizer;

        ui64 PoolId;
        i16 PoolThreads;
        TPoolManager PoolManager;
        TStackVec<TBasicExecutorPool*> Pools;
        std::unique_ptr<TSharedExecutorPoolSanitizer> Sanitizer;


        TArrayHolder<NSchedulerQueue::TReader> ScheduleReaders;
        TArrayHolder<NSchedulerQueue::TWriter> ScheduleWriters;

        const ui64 DefaultSpinThresholdCycles;
        const TString PoolName;
        const ui64 SoftProcessingDurationTs;

        char Barrier[64];

        TArrayHolder<NThreading::TPadded<TSharedExecutorThreadCtx>> Threads;
        static_assert(sizeof(std::decay_t<decltype(Threads[0])>) == PLATFORM_CACHE_LINE);

        alignas(64) TArrayHolder<NThreading::TPadded<std::atomic<ui64>>> ForeignThreadsAllowedByPool;
        TArrayHolder<NThreading::TPadded<std::atomic<ui64>>> ForeignThreadSlots;
        TArrayHolder<NThreading::TPadded<std::atomic<ui64>>> LocalThreads;
        TArrayHolder<NThreading::TPadded<std::atomic<ui64>>> LocalNotifications;
        alignas(64) std::atomic<ui64> SpinThresholdCycles;
        alignas(64) std::atomic<ui64> SpinningTimeUs;
        alignas(64) NThreading::TPadded<std::atomic<ui64>> ThreadsState;
        alignas(64) std::atomic<bool> StopFlag;

        const ui32 ActorSystemIndex = NActors::TActorTypeOperator::GetActorSystemIndex();
    public:
        struct TThreadsState {
            ui64 WorkingThreadCount = 0;
            ui64 Notifications = 0;

            inline ui64 ConvertToUI64() {
                ui64 value = WorkingThreadCount;
                return value
                    | ((ui64)Notifications << 32);
            }

            static inline TThreadsState GetThreadsState(i64 value) {
                TThreadsState state;
                state.WorkingThreadCount = value & 0x7fffffff;
                state.Notifications = (value >> 32) & 0x7fffffff;
                return state;
            }
        };

        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_TIME_PER_MAILBOX;
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX;

        explicit TSharedExecutorPool(const TSharedExecutorPoolConfig& cfg, const std::vector<TPoolShortInfo> &poolInfos);
        ~TSharedExecutorPool();

        i16 SumThreads(const std::vector<TPoolShortInfo> &poolInfos);
        void Initialize() override;
        i16 FindPoolForWorker(TSharedExecutorThreadCtx& thread, ui64 revolvingReadCounter);
        TMailbox* GetReadyActivation(ui64 revolvingReadCounter) override;

        void SwitchToPool(i16 poolId, NHPTimer::STime hpNow);

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingWriteCounter) override;

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;

        void GetSharedStatsForHarmonizer(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override;
        void GetSharedStats(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override;

        void GetExecutorPoolState(TExecutorPoolState &poolState) const override;
        TString GetName() const override {
            return PoolName;
        }

        ui32 GetThreads() const override;
        float GetThreadCount() const override;
        i16 GetMaxFullThreadCount() const override;
        i16 GetMinFullThreadCount() const override;
        float GetDefaultThreadCount() const override;
        float GetMinThreadCount() const override;
        float GetMaxThreadCount() const override;
        i16 GetSharedThreadCount() const override;

        bool WakeUpLocalThreads(i16 poolId);
        bool WakeUpGlobalThreads(i16 poolId);

        void FillForeignThreadsAllowed(std::vector<i16>& foreignThreadsAllowed) const override;
        void FillOwnedThreads(std::vector<i16>& ownedThreads) const override;
        void FillThreadOwners(std::vector<i16>& threadOwners) const override;


        void SetBasicPool(TBasicExecutorPool* pool);

        void SetForeignThreadSlots(i16 poolId, i16 slots) override;

        void ScheduleActivation(TMailbox*) override {
            Y_ABORT("TSharedExecutorPool::ScheduleActivation is not implemented");
        }

        void SpecificScheduleActivation(TMailbox*) override {
            Y_ABORT("TSharedExecutorPool::SpecificScheduleActivation is not implemented");
        }

        ui64 TimePerMailboxTs() const override;
        ui32 EventsPerMailbox() const override;

        // generic
        TAffinity* Affinity() const override {
            return nullptr;
        }

        const TPoolManager& GetPoolManager() const {
            return PoolManager;
        }
    };
}
