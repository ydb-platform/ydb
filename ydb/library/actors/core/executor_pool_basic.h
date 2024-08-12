#pragma once

#include "actorsystem.h"
#include "config.h"
#include "executor_thread.h"
#include "executor_thread_ctx.h"
#include "executor_pool_basic_feature_flags.h"
#include "scheduler_queue.h"
#include "executor_pool_base.h"
#include "harmonizer.h"
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
    class TBasicExecutorPoolSanitizer;

    struct TWaitingStatsConstants {
        static constexpr ui64 BucketCount = 128;
        static constexpr double MaxSpinThersholdUs = 12.8;

        static constexpr ui64 KnownAvgWakingUpTime = 4250;
        static constexpr ui64 KnownAvgAwakeningTime = 7000;

        static const double HistogramResolutionUs;
        static const ui64 HistogramResolution;
    };

    template <typename T>
    struct TWaitingStats : TWaitingStatsConstants {
        std::array<std::atomic<T>, BucketCount> WaitingUntilNeedsTimeHist;

        std::atomic<T> WakingUpTotalTime;
        std::atomic<T> WakingUpCount;
        std::atomic<T> AwakingTotalTime;
        std::atomic<T> AwakingCount;

        TWaitingStats()
        {
            Clear();
        }

        void Clear() {
            std::fill(WaitingUntilNeedsTimeHist.begin(), WaitingUntilNeedsTimeHist.end(), 0);
            WakingUpTotalTime = 0;
            WakingUpCount = 0;
            AwakingTotalTime = 0;
            AwakingCount = 0;
        }

        void Add(ui64 waitingUntilNeedsTime) {
            ui64 waitIdx = std::min(waitingUntilNeedsTime / HistogramResolution, BucketCount - 1);
            WaitingUntilNeedsTimeHist[waitIdx]++;
        }

        void AddAwakening(ui64 waitingUntilNeedsTime, ui64 awakingTime) {
            Add(waitingUntilNeedsTime);
            AwakingTotalTime += awakingTime;
            AwakingCount++;
        }

        void AddFastAwakening(ui64 waitingUntilNeedsTime) {
            Add(waitingUntilNeedsTime - HistogramResolution);
        }

        void AddWakingUp(ui64 wakingUpTime) {
            WakingUpTotalTime += wakingUpTime;
            WakingUpCount++;
        }

        void Add(const TWaitingStats<T> &stats) {
            for (ui32 idx = 0; idx < BucketCount; ++idx) {
                WaitingUntilNeedsTimeHist[idx] += stats.WaitingUntilNeedsTimeHist[idx];
            }
            WakingUpTotalTime += stats.WakingUpTotalTime;
            WakingUpCount += stats.WakingUpCount;
            AwakingTotalTime += stats.AwakingTotalTime;
            AwakingCount += stats.AwakingCount;
        }

        template <typename T2>
        void Add(const TWaitingStats<T2> &stats, double oldK, double newK) {
            for (ui32 idx = 0; idx < BucketCount; ++idx) {
                WaitingUntilNeedsTimeHist[idx] = oldK * WaitingUntilNeedsTimeHist[idx] + newK * stats.WaitingUntilNeedsTimeHist[idx];
            }
            WakingUpTotalTime = oldK * WakingUpTotalTime + newK * stats.WakingUpTotalTime;
            WakingUpCount = oldK * WakingUpCount + newK * stats.WakingUpCount;
            AwakingTotalTime = oldK * AwakingTotalTime + newK * stats.AwakingTotalTime;
            AwakingCount = oldK * AwakingCount + newK * stats.AwakingCount;
        }

        ui32 CalculateGoodSpinThresholdCycles(ui64 avgWakingUpConsumption) {
            auto &bucketCount = TWaitingStatsConstants::BucketCount;
            auto &resolution = TWaitingStatsConstants::HistogramResolution;

            T waitingsCount = std::accumulate(WaitingUntilNeedsTimeHist.begin(), WaitingUntilNeedsTimeHist.end(), 0);

            ui32 bestBucketIdx = 0;
            T bestCpuConsumption = Max<T>();

            T spinTime = 0;
            T spinCount = 0;

            for (ui32 bucketIdx = 0; bucketIdx < bucketCount; ++bucketIdx) {
                auto &bucket = WaitingUntilNeedsTimeHist[bucketIdx];
                ui64 imaginarySpingThreshold = resolution * bucketIdx;
                T cpuConsumption = spinTime + (waitingsCount - spinCount) * (avgWakingUpConsumption + imaginarySpingThreshold);
                if (bestCpuConsumption > cpuConsumption) {
                    bestCpuConsumption = cpuConsumption;
                    bestBucketIdx = bucketIdx;
                }
                spinTime += (2 * imaginarySpingThreshold + resolution) * bucket / 2;
                spinCount += bucket;
                // LWPROBE(WaitingHistogram, Pool->PoolId, Pool->GetName(), resolutionUs * bucketIdx, resolutionUs * (bucketIdx + 1), bucket);
            }
            ui64 result = resolution * bestBucketIdx;
            return result;
        }
    };



    class TBasicExecutorPool: public TExecutorPoolBase {
        friend class TBasicExecutorPoolSanitizer;

        NThreading::TPadded<std::atomic_bool> AllThreadsSleep = true;
        const ui64 DefaultSpinThresholdCycles;
        std::atomic<ui64> SpinThresholdCycles;
        std::unique_ptr<NThreading::TPadded< std::atomic<ui64>>[]> SpinThresholdCyclesPerThread;

        TArrayHolder<NThreading::TPadded<TExecutorThreadCtx>> Threads;
        static_assert(sizeof(std::decay_t<decltype(Threads[0])>) == PLATFORM_CACHE_LINE);
        TArrayHolder<NThreading::TPadded<std::queue<ui32>>> LocalQueues;
        TArrayHolder<TWaitingStats<ui64>> WaitingStats;
        TArrayHolder<TWaitingStats<double>> MovingWaitingStats;
        std::atomic<ui16> LocalQueueSize;

        TArrayHolder<NSchedulerQueue::TReader> ScheduleReaders;
        TArrayHolder<NSchedulerQueue::TWriter> ScheduleWriters;

        const TString PoolName;
        const TDuration TimePerMailbox;
        const ui32 EventsPerMailbox;

        const int RealtimePriority;

        TAtomic ThreadUtilization = 0;
        TAtomic MaxUtilizationCounter = 0;
        TAtomic MaxUtilizationAccumulator = 0;
        TAtomic WrongWakenedThreadCount = 0;
        std::atomic<ui64> SpinningTimeUs;

        TAtomic ThreadCount;
        TMutex ChangeThreadsLock;

        float MinThreadCount;
        i16 MinFullThreadCount;
        float MaxThreadCount;
        i16 MaxFullThreadCount;
        float DefaultThreadCount;
        i16 DefaultFullThreadCount;
        IHarmonizer *Harmonizer;
        ui64 SoftProcessingDurationTs = 0;
        bool HasOwnSharedThread = false;

        const i16 Priority = 0;
        const ui32 ActorSystemIndex = NActors::TActorTypeOperator::GetActorSystemIndex();
        TExecutorPoolJail *Jail = nullptr;

        static constexpr ui64 MaxSharedThreadsForPool = 2;
        NThreading::TPadded<std::atomic_uint64_t> SharedThreadsCount = 0;
        NThreading::TPadded<std::atomic<TSharedExecutorThreadCtx*>> SharedThreads[MaxSharedThreadsForPool] = {nullptr, nullptr};

        std::unique_ptr<TBasicExecutorPoolSanitizer> Sanitizer;

    public:
        struct TSemaphore {
            i64 OldSemaphore = 0; // 34 bits
            // Sign bit
            i16 CurrentSleepThreadCount = 0; // 14 bits
            // Sign bit
            i16 CurrentThreadCount = 0; // 14 bits

            inline i64 ConvertToI64() {
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

        const EASProfile ActorSystemProfile;
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
                           i16 priority = 0,
                           bool hasOwnSharedThread = false,
                           TExecutorPoolJail *jail = nullptr);
        explicit TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer, TExecutorPoolJail *jail=nullptr);
        ~TBasicExecutorPool();

        void Initialize(TWorkerContext& wctx) override;
        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingReadCounter) override;
        ui32 GetReadyActivationCommon(TWorkerContext& wctx, ui64 revolvingReadCounter);
        ui32 GetReadyActivationLocalQueue(TWorkerContext& wctx, ui64 revolvingReadCounter);

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void ScheduleActivationEx(ui32 activation, ui64 revolvingWriteCounter) override;
        void ScheduleActivationExCommon(ui32 activation, ui64 revolvingWriteCounter, TAtomic semaphoreValue);
        void ScheduleActivationExLocalQueue(ui32 activation, ui64 revolvingWriteCounter);

        void SetLocalQueueSize(ui16 size);

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;
        void GetExecutorPoolState(TExecutorPoolState &poolState) const override;
        TString GetName() const override {
            return PoolName;
        }

        void SetRealTimeMode() const override;

        ui32 GetThreads() const override;
        float GetThreadCount() const override;
        i16 GetFullThreadCount() const override;
        void SetFullThreadCount(i16 threads) override;
        float GetDefaultThreadCount() const override;
        i16 GetDefaultFullThreadCount() const override;
        float GetMinThreadCount() const override;
        i16 GetMinFullThreadCount() const override;
        float GetMaxThreadCount() const override;
        i16 GetMaxFullThreadCount() const override;
        TCpuConsumption GetThreadCpuConsumption(i16 threadIdx) override;
        i16 GetBlockingThreadCount() const override;
        i16 GetPriority() const override;

        void SetSpinThresholdCycles(ui32 cycles) override;

        void GetWaitingStats(TWaitingStats<ui64> &acc) const;
        void CalcSpinPerThread(ui64 wakingUpConsumption);
        void ClearWaitingStats() const;

        TSharedExecutorThreadCtx* ReleaseSharedThread();
        void AddSharedThread(TSharedExecutorThreadCtx* thread);

    private:
        void AskToGoToSleep(bool *needToWait, bool *needToBlock);

        void WakeUpLoop(i16 currentThreadCount);
        bool WakeUpLoopShared();
    };
}
