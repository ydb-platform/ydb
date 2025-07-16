#include "shared_info.h"
#include "pool.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/executor_pool_shared.h>
#include <ydb/library/actors/core/executor_pool_basic.h>

using namespace NActors;

namespace {

class TMockSharedPool : public ISharedPool {
public:
    TMockSharedPool(i16 poolCount, i16 threadCount)
        : ThreadCount(threadCount)
        , ForeignThreadsAllowedValues(poolCount, 0)
        , OwnedThreadsValues(poolCount, 0)
        , ThreadOwnersValues(threadCount, 0)
        , ThreadStatsValues(poolCount)
    {
    }

    void GetSharedStatsForHarmonizer(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override {
        statsCopy = ThreadStatsValues[poolId];
    }

    void GetSharedStats(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override {
        statsCopy = ThreadStatsValues[poolId];
    }

    void FillThreadOwners(std::vector<i16>& threadOwners) const override {
        threadOwners = ThreadOwnersValues;
    }

    void FillForeignThreadsAllowed(std::vector<i16>& foreignThreadsAllowed) const override {
        foreignThreadsAllowed = ForeignThreadsAllowedValues;
    }

    void FillOwnedThreads(std::vector<i16>& ownedThreads) const override {
        ownedThreads = OwnedThreadsValues;
    }

    i16 GetSharedThreadCount() const override {
        return ThreadCount;
    }

    void SetForeignThreadsAllowed(i16 poolId, i16 value) {
        ForeignThreadsAllowedValues[poolId] = value;
    }

    void SetOwnedThreads(i16 poolId, i16 value) {
        OwnedThreadsValues[poolId] = value;
    }

    void SetThreadOwner(i16 threadId, i16 poolId) {
        ThreadOwnersValues[threadId] = poolId;
    }

    void SetThreadStats(i16 poolId, const TExecutorThreadStats& stats) {
        ThreadStatsValues[poolId] = TVector<TExecutorThreadStats>(ThreadCount, stats);
    }

    void SetForeignThreadSlots(i16 poolId, i16 slots) override {
        ForeignThreadsAllowedValues[poolId] = slots;
    }

private:
    i16 ThreadCount;
    std::vector<i16> ForeignThreadsAllowedValues;
    std::vector<i16> OwnedThreadsValues;
    std::vector<i16> ThreadOwnersValues;
    TVector<TVector<TExecutorThreadStats>> ThreadStatsValues;
};

} // namespace

Y_UNIT_TEST_SUITE(SharedInfoTests) {
    Y_UNIT_TEST(TestInitialization) {
        TSharedInfo info;
        TMockSharedPool pool(2, 4);

        info.Init(2, &pool);

        UNIT_ASSERT_VALUES_EQUAL(info.PoolCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(info.ForeignThreadsAllowed.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.OwnedThreads.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.CpuConsumption.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.CpuConsumptionPerThread.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(info.ThreadOwners.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(info.ThreadStats.size(), 4);
    }

    Y_UNIT_TEST(TestPull) {
        TSharedInfo info;
        TMockSharedPool pool(2, 4);

        pool.SetForeignThreadsAllowed(0, 1);
        pool.SetForeignThreadsAllowed(1, 2);
        pool.SetOwnedThreads(0, 2);
        pool.SetOwnedThreads(1, 1);
        pool.SetThreadOwner(0, 0);
        pool.SetThreadOwner(1, 0);
        pool.SetThreadOwner(2, 1);
        pool.SetThreadOwner(3, 1);

        info.Init(2, &pool);

        TExecutorThreadStats stats;
        stats.SafeElapsedTicks = 1000;
        stats.CpuUs = 800;
        stats.SafeParkedTicks = 200;
        
        for (i16 i = 0; i < 2; ++i) {
            pool.SetThreadStats(i, stats);
        }


        std::vector<std::unique_ptr<TPoolInfo>> poolInfos;
        poolInfos.emplace_back(std::make_unique<TPoolInfo>());
        poolInfos.emplace_back(std::make_unique<TPoolInfo>());
        for (auto &poolInfo : poolInfos) {
            poolInfo->SharedInfo.resize(4);
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                poolInfo->SharedInfo[threadId].ElapsedCpu.Register(1, 0);
                poolInfo->SharedInfo[threadId].UsedCpu.Register(1, 0);
            }
        }

        ui64 ts = Us2Ts(8.0 * 1'000'000);
        for (auto &poolInfo : poolInfos) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                poolInfo->SharedInfo[threadId].ElapsedCpu.Register(ts, 0.4 * 8);
                poolInfo->SharedInfo[threadId].UsedCpu.Register(ts, 0.3 * 8);
            }
        }

        info.Pull(poolInfos, pool);

        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());
        for (i16 poolId = 0; poolId < 2; ++poolId) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                const auto& consumption = info.CpuConsumptionPerThread[poolId][threadId];
                UNIT_ASSERT_DOUBLES_EQUAL_C(consumption.Elapsed, 0.4, 1e-6, info.ToString());
                UNIT_ASSERT_DOUBLES_EQUAL_C(consumption.Cpu, 0.3, 1e-6, info.ToString());
            }
        }

        ts += Us2Ts(8.0 * 1'000'000);
        for (auto &poolInfo : poolInfos) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                poolInfo->SharedInfo[threadId].ElapsedCpu.Register(ts, (0.4 + 0.5) * 8);
                poolInfo->SharedInfo[threadId].UsedCpu.Register(ts, (0.3 + 0.4) * 8);
            }
        }

        info.Pull(poolInfos, pool);

        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                const auto& consumption = info.CpuConsumptionPerThread[poolId][threadId];
                UNIT_ASSERT_DOUBLES_EQUAL_C(consumption.Elapsed, 0.5, 1e-6, info.ToString());
                UNIT_ASSERT_DOUBLES_EQUAL_C(consumption.Cpu, 0.4, 1e-6, info.ToString());
            }
        }
    }

    Y_UNIT_TEST(TestCpuConsumptionCalculation) {
        TSharedInfo info;
        TMockSharedPool pool(2, 4);

        pool.SetThreadOwner(0, 0);
        pool.SetThreadOwner(1, 0);
        pool.SetThreadOwner(2, 1);
        pool.SetThreadOwner(3, 1);

        info.Init(2, &pool);

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.0f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.0f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 2.0f, info.ToString());
        }

        TExecutorThreadStats highLoadStats;
        highLoadStats.SafeElapsedTicks = 9'000;
        highLoadStats.CpuUs = 9'000;
        highLoadStats.SafeParkedTicks = 1'000;

        pool.SetThreadStats(0, highLoadStats);

        TExecutorThreadStats lowLoadStats;
        lowLoadStats.SafeElapsedTicks = 2'000;
        lowLoadStats.CpuUs = 2'000;
        lowLoadStats.SafeParkedTicks = 8'000;

        std::vector<std::unique_ptr<TPoolInfo>> poolInfos;
        poolInfos.emplace_back(std::make_unique<TPoolInfo>());
        poolInfos.emplace_back(std::make_unique<TPoolInfo>());
        for (auto &poolInfo : poolInfos) {
            poolInfo->SharedInfo.resize(4);
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                poolInfo->SharedInfo[threadId].ElapsedCpu.Register(1, 0);
                poolInfo->SharedInfo[threadId].UsedCpu.Register(1, 0);
            }
        }

        ui64 ts = Us2Ts(8.0 * 1'000'000);
        for (i16 threadId = 0; threadId < 4; ++threadId) {
            poolInfos[0]->SharedInfo[threadId].ElapsedCpu.Register(ts, 0.45 * 8);
            poolInfos[0]->SharedInfo[threadId].UsedCpu.Register(ts, 0.45 * 8);
            poolInfos[1]->SharedInfo[threadId].ElapsedCpu.Register(ts, 0.1 * 8);
            poolInfos[1]->SharedInfo[threadId].UsedCpu.Register(ts, 0.1 * 8);
        }

        info.Pull(poolInfos, pool);

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Elapsed, 1.8f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 1.8f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].CpuQuota, 2.7f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Elapsed, 0.4f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.4f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].CpuQuota, 1.3f, 1e-6, info.ToString());

        ts += Us2Ts(8.0 * 1'000'000);
        for (i16 threadId = 0; threadId < 4; ++threadId) {
            poolInfos[0]->SharedInfo[threadId].ElapsedCpu.Register(ts, (0.45 + 0.5) * 8);
            poolInfos[0]->SharedInfo[threadId].UsedCpu.Register(ts, (0.45 + 0.5) * 8);
            poolInfos[1]->SharedInfo[threadId].ElapsedCpu.Register(ts, (0.1 + 0.05) * 8);
            poolInfos[1]->SharedInfo[threadId].UsedCpu.Register(ts, (0.1 + 0.05) * 8);
        }

        info.Pull(poolInfos, pool);

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Elapsed, 2.0f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 2.0f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].CpuQuota, 2.9f, 1e-6, info.ToString());

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Elapsed, 0.2f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.2f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].CpuQuota, 1.1f, 1e-6, info.ToString());
    }

}
