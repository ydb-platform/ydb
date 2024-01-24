#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/stream/null.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include "storage_pool_info.h"

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

using namespace NKikimr;
using namespace NHive;
using namespace NKikimrBlobStorage;

Y_UNIT_TEST_SUITE(StoragePool) {
    double GetAvg(const TMap<ui32, ui32>& values) {
        double sum = 0;
        for (auto [id, value] : values) {
            Y_UNUSED(id);
            sum += value;
        }
        return sum / values.size();
    }

    double GetMin(const TMap<ui32, ui32>& values) {
        double min = values.empty() ? NAN : values.begin()->second;
        for (auto [id, value] : values) {
            Y_UNUSED(id);
            min = std::min<double>(min, value);
        }
        return min;
    }

    double GetMax(const TMap<ui32, ui32>& values) {
        double max = values.empty() ? NAN : values.begin()->second;
        for (auto [id, value] : values) {
            Y_UNUSED(id);
            max = std::max<double>(max, value);
        }
        return max;
    }

    double GetStdDev(const TMap<ui32, ui32>& values) {
        double sum = 0;
        for (auto [id, value] : values) {
            Y_UNUSED(id);
            sum += value;
        }
        auto mean = sum / values.size();
        sum = 0;
        for (auto [id, value] : values) {
            Y_UNUSED(id);
            auto diff = value - mean;
            sum = sum + diff * diff;
        }
        return sqrt(sum / values.size());
    }

    void TestStrategy(NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy strategy) {
        THiveSharedSettings settings;
        settings.CurrentConfig.SetStorageSelectStrategy(strategy);
        TStoragePoolInfo pool("pool1", &settings);
        TEvControllerSelectGroupsResult::TGroupParameters group1;
        group1.MutableAssuredResources()->SetIOPS(1000);
        group1.MutableAssuredResources()->SetReadThroughput(1000);
        group1.MutableAssuredResources()->SetSpace(1000000);
        for (int i = 1; i <= 80; ++i) {
            group1.SetGroupID(i);
            pool.UpdateStorageGroup(i, group1);
        }

        static constexpr int TABLETS1 = 100000;
        static constexpr int TABLETS2 = 60000;
        static constexpr int CHANNELS = 3;

        TMap<ui32, ui32> groupUnits;
        TMap<ui32, ui32> groupChannels[CHANNELS];

        TGroupFilter unit1;
        unit1.GroupParameters.SetRequiredIOPS(1);
        unit1.GroupParameters.SetRequiredThroughput(10);
        unit1.GroupParameters.SetRequiredDataSize(100);

        auto checkUnit1 = [&unit1](const TStorageGroupInfo& group) -> bool {
            return group.IsMatchesParameters(unit1);
        };

        for (int i = 0; i < TABLETS1 * CHANNELS; ++i) {
            int channel = i % CHANNELS;
            const TEvControllerSelectGroupsResult::TGroupParameters* found = pool.FindFreeAllocationUnit(checkUnit1);
            UNIT_ASSERT(found);
            ui32 groupId = found->GetGroupID();
            groupUnits[groupId]++;
            groupChannels[channel][groupId]++;
            pool.GetStorageGroup(groupId).AcquiredResources.IOPS += unit1.GroupParameters.GetRequiredIOPS();
            pool.GetStorageGroup(groupId).AcquiredResources.Throughput += unit1.GroupParameters.GetRequiredThroughput();
            pool.GetStorageGroup(groupId).AcquiredResources.Size += unit1.GroupParameters.GetRequiredDataSize();
        }

        if (strategy != NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN) {
            // simulate storage expansion - it doesn't work well with RoundRobin strategy

            for (int i = 81; i <= 100; ++i) {
                group1.SetGroupID(i);
                pool.UpdateStorageGroup(i, group1);
            }

            for (int i = 0; i < TABLETS2 * CHANNELS; ++i) {
                int channel = i % CHANNELS;
                const TEvControllerSelectGroupsResult::TGroupParameters* found = pool.FindFreeAllocationUnit(checkUnit1);
                UNIT_ASSERT(found);
                ui32 groupId = found->GetGroupID();
                groupUnits[groupId]++;
                groupChannels[channel][groupId]++;
                pool.GetStorageGroup(groupId).AcquiredResources.IOPS += unit1.GroupParameters.GetRequiredIOPS();
                pool.GetStorageGroup(groupId).AcquiredResources.Throughput += unit1.GroupParameters.GetRequiredThroughput();
                pool.GetStorageGroup(groupId).AcquiredResources.Size += unit1.GroupParameters.GetRequiredDataSize();
            }
        }

#ifndef _NDEBUG
        auto avg = GetAvg(groupUnits);
        Ctest << "avg = " << avg << Endl;

        auto min = GetMin(groupUnits);
        Ctest << "min = " << min << Endl;

        auto max = GetMax(groupUnits);
        Ctest << "max = " << max << Endl;

        auto stdDev = GetStdDev(groupUnits);
        Ctest << "std-dev = " << stdDev << Endl;

        for (int i = 0; i < CHANNELS; ++i) {
            auto avg = GetAvg(groupChannels[i]);
            Ctest << "ch." << i << " avg = " << avg << Endl;

            auto min = GetMin(groupChannels[i]);
            Ctest << "ch." << i << " min = " << min << Endl;

            auto max = GetMax(groupChannels[i]);
            Ctest << "ch." << i << " max = " << max << Endl;

            auto stdDev = GetStdDev(groupChannels[i]);
            Ctest << "ch." << i << " std-dev = " << stdDev << Endl;
        }
#endif

        UNIT_ASSERT_VALUES_EQUAL(round(GetAvg(groupUnits)), 4800);
        UNIT_ASSERT(GetStdDev(groupUnits) < 1);
    }

    void TestStrategyWithOverflow(NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy strategy) {
        THiveSharedSettings settings;
        settings.CurrentConfig.SetStorageSelectStrategy(strategy);
        TStoragePoolInfo pool("pool1", &settings);
        TEvControllerSelectGroupsResult::TGroupParameters group1;
        group1.MutableAssuredResources()->SetIOPS(1000);
        group1.MutableAssuredResources()->SetReadThroughput(1000);
        group1.MutableAssuredResources()->SetSpace(1000000);
        group1.SetGroupID(1);
        pool.UpdateStorageGroup(1, group1);
        group1.SetGroupID(2);
        pool.UpdateStorageGroup(2, group1);
        group1.SetGroupID(3);
        pool.UpdateStorageGroup(3, group1);
        group1.SetGroupID(4);
        pool.UpdateStorageGroup(4, group1);

        TMap<ui32, ui32> groupUnits;

        TGroupFilter unit1;
        unit1.GroupParameters.SetRequiredIOPS(1);
        unit1.GroupParameters.SetRequiredThroughput(10);
        unit1.GroupParameters.SetRequiredDataSize(1000);
        auto checkUnit1 = [&unit1](const TStorageGroupInfo& group) -> bool {
            return group.IsMatchesParameters(unit1);
        };
        for (int i = 0; i < 5000; ++i) { // overflow
            const TEvControllerSelectGroupsResult::TGroupParameters* found = pool.FindFreeAllocationUnit(checkUnit1);
            UNIT_ASSERT(found);
            ui32 groupId = found->GetGroupID();
            groupUnits[groupId]++;
            pool.GetStorageGroup(groupId).AcquiredResources.IOPS += unit1.GroupParameters.GetRequiredIOPS();
            pool.GetStorageGroup(groupId).AcquiredResources.Throughput += unit1.GroupParameters.GetRequiredThroughput();
            pool.GetStorageGroup(groupId).AcquiredResources.Size += unit1.GroupParameters.GetRequiredDataSize();
        }

#ifndef _NDEBUG
        auto avg = GetAvg(groupUnits);
        Ctest << "avg = " << avg << Endl;


        auto stdDev = GetStdDev(groupUnits);
        Ctest << "std-dev = " << stdDev << Endl;
#endif

        UNIT_ASSERT_VALUES_EQUAL(round(GetAvg(groupUnits)), 1250);
        UNIT_ASSERT(GetStdDev(groupUnits) < 0.32);
    }

    Y_UNIT_TEST(TestDistributionRandomProbability) {
        TestStrategy(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM);
    }

    Y_UNIT_TEST(TestDistributionRandomProbabilityWithOverflow) {
        TestStrategyWithOverflow(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM);
    }

    Y_UNIT_TEST(TestDistributionExactMin) {
        TestStrategy(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN);
    }

    Y_UNIT_TEST(TestDistributionExactMinWithOverflow) {
        TestStrategyWithOverflow(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN);
    }

    Y_UNIT_TEST(TestDistributionRandomMin7p) {
        TestStrategy(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P);
    }

    Y_UNIT_TEST(TestDistributionRandomMin7pWithOverflow) {
        TestStrategyWithOverflow(NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P);
    }

    /*Y_UNIT_TEST(TestDistributionRoundRobin) {
        TestStrategy(EStorageSelectStrategy::RoundRobin);
    }*/

    /*Y_UNIT_TEST(TestDistributionRoundRobinWithOverflow) {
        TestStrategyWithOverflow(EStorageSelectStrategy::RoundRobin);
    }*/
}
