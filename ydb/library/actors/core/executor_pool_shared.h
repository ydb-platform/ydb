#pragma once

#include "executor_pool.h"


namespace NActors {

    struct TSharedExecutorPoolConfig;
    struct TSharedExecutorThreadCtx;

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

        TString ToString() const;
    };

    class ISharedExecutorPool : public IActorThreadPool {
    public:
        virtual ~ISharedExecutorPool() = default;

        virtual TSharedExecutorThreadCtx *GetSharedThread(i16 poolId) = 0;
        virtual void GetSharedStats(i16 pool, std::vector<TExecutorThreadStats>& statsCopy) = 0;
        virtual void GetSharedStatsForHarmonizer(i16 pool, std::vector<TExecutorThreadStats>& statsCopy) = 0;
        virtual TCpuConsumption GetThreadCpuConsumption(i16 poolId, i16 threadIdx) = 0;
        virtual std::vector<TCpuConsumption> GetThreadsCpuConsumption(i16 poolId) = 0;
        virtual void Init(const std::vector<IExecutorPool*>& pools, bool withThreads) = 0;

        virtual i16 ReturnOwnHalfThread(i16 pool) = 0;
        virtual i16 ReturnBorrowedHalfThread(i16 pool) = 0;
        virtual void GiveHalfThread(i16 from, i16 to) = 0;

        virtual i16 GetSharedThreadCount() const = 0;

        virtual TSharedPoolState GetState() const = 0;
    };

    ISharedExecutorPool *CreateSharedExecutorPool(const TSharedExecutorPoolConfig &config, i16 poolCount, std::vector<i16> poolsWithThreads);

}