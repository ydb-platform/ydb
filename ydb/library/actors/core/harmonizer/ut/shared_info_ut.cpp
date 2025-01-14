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
        , ThreadStatsValues(threadCount)
    {
    }

    void GetSharedStatsForHarmonizer(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override {
        Y_UNUSED(poolId);
        statsCopy = ThreadStatsValues;
    }

    void GetSharedStats(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const override {
        Y_UNUSED(poolId);
        statsCopy = ThreadStatsValues;
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

    void SetThreadStats(i16 threadId, const TExecutorThreadStats& stats) {
        ThreadStatsValues[threadId] = stats;
    }

private:
    // i16 PoolCount;
    i16 ThreadCount;
    std::vector<i16> ForeignThreadsAllowedValues;
    std::vector<i16> OwnedThreadsValues;
    std::vector<i16> ThreadOwnersValues;
    TVector<TExecutorThreadStats> ThreadStatsValues;
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

        // Настраиваем начальное состояние
        pool.SetForeignThreadsAllowed(0, 1);
        pool.SetForeignThreadsAllowed(1, 2);
        pool.SetOwnedThreads(0, 2);
        pool.SetOwnedThreads(1, 1);
        pool.SetThreadOwner(0, 0);
        pool.SetThreadOwner(1, 0);
        pool.SetThreadOwner(2, 1);
        pool.SetThreadOwner(3, 1);

        info.Init(2, &pool);

        // Настраиваем статистику потоков
        TExecutorThreadStats stats;
        stats.SafeElapsedTicks = 1000;
        stats.CpuUs = 800;
        stats.SafeParkedTicks = 200;
        
        for (i16 i = 0; i < 4; ++i) {
            pool.SetThreadStats(i, stats);
        }

        // Первый Pull для инициализации базовых значений
        info.Pull(pool);

        // Проверяем значения ForeignThreadsAllowed и OwnedThreads
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());

        // Проверяем расчет потребления CPU
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
    
        // Обновляем статистику
        stats.SafeElapsedTicks = 2000;
        stats.CpuUs = 1600;
        stats.SafeParkedTicks = 400;
        
        for (i16 i = 0; i < 4; ++i) {
            pool.SetThreadStats(i, stats);
        }

        // Второй Pull для проверки расчета разницы
        info.Pull(pool);

        // Проверяем значения ForeignThreadsAllowed и OwnedThreads
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[0], 1, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.ForeignThreadsAllowed[1], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[0], 2, info.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.OwnedThreads[1], 1, info.ToString());

        // Проверяем расчет потребления CPU
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

    Y_UNIT_TEST(TestToString) {
        TSharedInfo info;
        TMockSharedPool pool(2, 4);

        pool.SetForeignThreadsAllowed(0, 1);
        pool.SetForeignThreadsAllowed(1, 2);
        pool.SetOwnedThreads(0, 2);
        pool.SetOwnedThreads(1, 1);

        info.Init(2, &pool);
        info.Pull(pool);

        TString result = info.ToString();
        
        UNIT_ASSERT(result.Contains("ForeignThreadsAllowed"));
        UNIT_ASSERT(result.Contains("OwnedThreads"));
        UNIT_ASSERT(result.Contains("Pool[0]"));
        UNIT_ASSERT(result.Contains("Pool[1]"));
    }

    Y_UNIT_TEST(TestCpuConsumptionCalculation) {
        TSharedInfo info;
        TMockSharedPool pool(2, 4);

        // Настраиваем владельцев потоков
        pool.SetThreadOwner(0, 0); // поток 0 принадлежит пулу 0
        pool.SetThreadOwner(1, 0); // поток 1 принадлежит пулу 0
        pool.SetThreadOwner(2, 1); // поток 2 принадлежит пулу 1
        pool.SetThreadOwner(3, 1); // поток 3 принадлежит пулу 1

        info.Init(2, &pool);

        // Первый пул: высокая загрузка CPU
        TExecutorThreadStats highLoadStats;
        highLoadStats.SafeElapsedTicks = 10'000;
        highLoadStats.CpuUs = 9'000;
        highLoadStats.SafeParkedTicks = 1'000;

        TVector<TExecutorThreadStats> pool0Stats(4, highLoadStats);
        pool.SetThreadStats(0, pool0Stats);

        // Второй пул: низкая загрузка CPU
        TExecutorThreadStats lowLoadStats;
        lowLoadStats.SafeElapsedTicks = 10'000;
        lowLoadStats.CpuUs = 2'000;
        lowLoadStats.SafeParkedTicks = 8'000;

        TVector<TExecutorThreadStats> pool1Stats(4, lowLoadStats);
        pool.SetThreadStats(1, pool1Stats);

        info.Pull(pool);

        // Проверяем начальные значения CpuConsumption
        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.0f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.0f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 0.0f, info.ToString());
        }

        // Обновляем статистику для второго Pull
        highLoadStats.SafeElapsedTicks = 20'000; // +10'000
        highLoadStats.CpuUs = 18'000;            // +9'000
        highLoadStats.SafeParkedTicks = 2'000;   // +1'000

        pool0Stats.assign(4, highLoadStats);
        pool.SetThreadStats(0, pool0Stats);

        lowLoadStats.SafeElapsedTicks = 20'000;  // +10'000
        lowLoadStats.CpuUs = 4'000;              // +2'000
        lowLoadStats.SafeParkedTicks = 16'000;   // +8'000

        pool1Stats.assign(4, lowLoadStats);
        pool.SetThreadStats(1, pool1Stats);

        info.Pull(pool);

        // Проверяем значения CpuConsumption после обновления
        // Для пула 0 (высокая нагрузка)
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Elapsed, 0.5f, 1e-6, info.ToString()); // 10'000 / 20'000
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 0.45f, 1e-6, info.ToString());    // 9'000 / 20'000
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].CpuQuota, 0.05f, 1e-6, info.ToString()); // 1'000 / 20'000

        // Для пула 1 (низкая нагрузка)
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Elapsed, 0.5f, 1e-6, info.ToString());  // 10'000 / 20'000
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.1f, 1e-6, info.ToString());      // 2'000 / 20'000
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].CpuQuota, 0.4f, 1e-6, info.ToString()); // 8'000 / 20'000
    }

    Y_UNIT_TEST(TestCpuConsumptionEdgeCases) {
        TSharedInfo info;
        TMockSharedPool pool(2, 2);

        pool.SetThreadOwner(0, 0);
        pool.SetThreadOwner(1, 1);

        info.Init(2, &pool);

        // Случай 1: Нулевая активность
        TExecutorThreadStats zeroStats;
        zeroStats.SafeElapsedTicks = 0;
        zeroStats.CpuUs = 0;
        zeroStats.SafeParkedTicks = 0;

        TVector<TExecutorThreadStats> zeroPoolStats(2, zeroStats);
        pool.SetThreadStats(0, zeroPoolStats);
        pool.SetThreadStats(1, zeroPoolStats);

        info.Pull(pool);

        // Случай 2: Максимальная активность
        TExecutorThreadStats maxStats;
        maxStats.SafeElapsedTicks = std::numeric_limits<ui64>::max() - 1;
        maxStats.CpuUs = std::numeric_limits<ui64>::max() - 1;
        maxStats.SafeParkedTicks = std::numeric_limits<ui64>::max() - 1;

        TVector<TExecutorThreadStats> maxPoolStats(2, maxStats);
        pool.SetThreadStats(0, maxPoolStats);
        pool.SetThreadStats(1, maxPoolStats);

        info.Pull(pool);

        // Проверяем, что не произошло переполнения
        for (i16 poolId = 0; poolId < 2; ++poolId) {
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Elapsed, 0.5f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].Cpu, 0.5f, info.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.CpuConsumption[poolId].CpuQuota, 0.5f, info.ToString());
        }

        // Случай 3: Асимметричная нагрузка
        TExecutorThreadStats activeStats;
        activeStats.SafeElapsedTicks = 1000;
        activeStats.CpuUs = 900;
        activeStats.SafeParkedTicks = 100;

        TExecutorThreadStats idleStats;
        idleStats.SafeElapsedTicks = 1000;
        idleStats.CpuUs = 100;
        idleStats.SafeParkedTicks = 900;

        TVector<TExecutorThreadStats> activePoolStats(2, activeStats);
        TVector<TExecutorThreadStats> idlePoolStats(2, idleStats);
        
        pool.SetThreadStats(0, activePoolStats);
        pool.SetThreadStats(1, idlePoolStats);

        info.Pull(pool);

        // Проверяем асимметричную нагрузку
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[0].Cpu, 0.9f, 1e-6, info.ToString());
        UNIT_ASSERT_DOUBLES_EQUAL_C(info.CpuConsumption[1].Cpu, 0.1f, 1e-6, info.ToString());
    }
}
