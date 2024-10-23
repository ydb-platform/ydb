#pragma once

#include "executor_pool.h"
#include "executor_thread_ctx.h"


namespace NActors {

    struct TExecutorThreadCtx;
    struct TSharedExecutorPoolConfig;
    class TBasicExecutorPool;

    struct TSharedPoolState {
        std::vector<i16> ThreadByPool;
        std::vector<i16> PoolByThread;
        std::vector<i16> BorrowedThreadByPool;
        std::vector<i16> PoolByBorrowedThread;

        TSharedPoolState(i16 poolCount, i16 threadCount)
            : ThreadByPool(poolCount, -1)
            , PoolByThread(threadCount)
            , BorrowedThreadByPool(poolCount, -1)
            , PoolByBorrowedThread(threadCount, -1)
        {}
    };

    class TSharedExecutorPool: public IActorThreadPool {
    public:
        TSharedExecutorPool(const TSharedExecutorPoolConfig &config, i16 poolCount, std::vector<i16> poolsWithThreads);

        // IThreadPool
        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;
        bool Cleanup() override;

        TSharedExecutorThreadCtx *GetSharedThread(i16 poolId);
        void GetSharedStats(i16 pool, std::vector<TExecutorThreadStats>& statsCopy);
        void GetSharedStatsForHarmonizer(i16 pool, std::vector<TExecutorThreadStats>& statsCopy);
        TCpuConsumption GetThreadCpuConsumption(i16 poolId, i16 threadIdx);
        std::vector<TCpuConsumption> GetThreadsCpuConsumption(i16 poolId);

        void ReturnOwnHalfThread(i16 pool);
        void ReturnBorrowedHalfThread(i16 pool);
        void GiveHalfThread(i16 from, i16 to);

        i16 GetSharedThreadCount() const;

        TSharedPoolState GetState() const;

    private:
        TSharedPoolState State;
   
        std::vector<TBasicExecutorPool*> Pools;

        i16 PoolCount;
        i16 SharedThreadCount;
        std::unique_ptr<TSharedExecutorThreadCtx[]> Threads;

        std::unique_ptr<NSchedulerQueue::TReader[]> ScheduleReaders;
        std::unique_ptr<NSchedulerQueue::TWriter[]> ScheduleWriters;

        TDuration TimePerMailbox;
        ui32 EventsPerMailbox;
        ui64 SoftProcessingDurationTs;
    };

}