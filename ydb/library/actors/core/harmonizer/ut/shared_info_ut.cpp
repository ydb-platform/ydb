#include "shared_info.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/executor_pool_shared.h>

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
        UNIT_ASSERT_VALUES_EQUAL(info.CpuConsumptionByPool.size(), 2);
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

        info.Pull(pool);

        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                const auto& consumption = info.CpuConsumptionByPool[poolId][threadId];
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Elapsed, 1000, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Cpu, 800, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Parked, 200, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffElapsed, 1000, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffCpu, 800, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffParked, 200, info.ToString());
            }
        }
    
        stats.SafeElapsedTicks = 2000;
        stats.CpuUs = 1600;
        stats.SafeParkedTicks = 400;
        
        for (i16 i = 0; i < 2; ++i) {
            pool.SetThreadStats(i, stats);
        }

        info.Pull(pool);

        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            for (i16 threadId = 0; threadId < 4; ++threadId) {
                const auto& consumption = info.CpuConsumptionByPool[poolId][threadId];
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Elapsed, 2000, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Cpu, 1600, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.Parked, 400, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffElapsed, 1000, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffCpu, 800, info.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(consumption.DiffParked, 200, info.ToString());
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

        pool.SetThreadStats(1, lowLoadStats);

        info.Pull(pool);

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Elapsed, 1.8f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 1.8f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].CpuQuota, 2.7f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Elapsed, 0.4f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.4f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].CpuQuota, 1.3f, 1e-6, info.ToString());

        highLoadStats.SafeElapsedTicks = 19'000; // +10'000
        highLoadStats.CpuUs = 19'000;            // +10'000
        highLoadStats.SafeParkedTicks = 1'000;   // +0'000

        pool.SetThreadStats(0, highLoadStats);

        lowLoadStats.SafeElapsedTicks = 3'000;  // +1'000
        lowLoadStats.CpuUs = 3'000;              // +1'000
        lowLoadStats.SafeParkedTicks = 17'000;   // +9'000

        pool.SetThreadStats(1, lowLoadStats);

        info.Pull(pool);

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Elapsed, 2.0f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 2.0f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].CpuQuota, 2.9f, 1e-6, info.ToString());

        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Elapsed, 0.2f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.2f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].CpuQuota, 1.1f, 1e-6, info.ToString());
    }

    Y_UNIT_TEST(TestCpuConsumptionEdgeCases) {
        TSharedInfo info;
        TMockSharedPool pool(2, 2);

        pool.SetThreadOwner(0, 0);
        pool.SetThreadOwner(1, 1);

        info.Init(2, &pool);

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.0f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.0f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 1.0f, 1e-6, info.ToString());
        }

        TExecutorThreadStats zeroStats;
        zeroStats.SafeElapsedTicks = 0;
        zeroStats.CpuUs = 0;
        zeroStats.SafeParkedTicks = 0;

        pool.SetThreadStats(0, zeroStats);
        pool.SetThreadStats(1, zeroStats);

        info.Pull(pool);

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.0f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.0f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 1.0f, 1e-6, info.ToString());
        }

        TExecutorThreadStats maxStats;
        maxStats.SafeElapsedTicks = std::numeric_limits<ui64>::max() / 4 - 1;
        maxStats.CpuUs = std::numeric_limits<ui64>::max() / 4 - 1;
        maxStats.SafeParkedTicks = std::numeric_limits<ui64>::max() / 4 - 1;

        pool.SetThreadStats(0, maxStats);
        pool.SetThreadStats(1, maxStats);

        info.Pull(pool);

        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.5f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.5f, 1e-6, info.ToString());
            UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 1.0f, 1e-6, info.ToString());
        }
    }
}
