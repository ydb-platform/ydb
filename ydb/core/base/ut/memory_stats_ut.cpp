#include "memory_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMemory {

using TMemoryStats = NKikimrMemory::TMemoryStats;

Y_UNIT_TEST_SUITE (TMemoryStatsAggregator) {

    TMemoryStats GenerateStats(ui64 index) {
        TMemoryStats stats;
        stats.SetAnonRss(10 + index);
        stats.SetCGroupLimit(20 + index);
        stats.SetMemTotal(30 + index);
        stats.SetMemAvailable(40 + index);
        stats.SetAllocatedMemory(50 + index);
        stats.SetAllocatorCachesMemory(60 + index);
        stats.SetHardLimit(70 + index);
        stats.SetSoftLimit(80 + index);
        stats.SetTargetUtilization(90 + index);
        stats.SetExternalConsumption(100 + index);
        stats.SetSharedCacheConsumption(110 + index);
        stats.SetSharedCacheLimit(120 + index);
        stats.SetMemTableConsumption(130 + index);
        stats.SetMemTableLimit(140 + index);
        stats.SetQueryExecutionConsumption(150 + index);
        stats.SetQueryExecutionLimit(160 + index);
        return stats;
    }

    Y_UNIT_TEST (Aggregate_Empty) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(), "");
    }

    Y_UNIT_TEST (Aggregate_Single) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);

        Cerr << stats1.ShortDebugString() << Endl;

        aggregator.Add(stats1, "host");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(), stats1.ShortDebugString());
    }

    Y_UNIT_TEST (Aggregate_Summarize_ExternalConsumption_DifferentHosts) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);
        TMemoryStats stats2 = GenerateStats(2);
        TMemoryStats stats3 = GenerateStats(3);

        Cerr << stats1.ShortDebugString() << Endl;
        Cerr << stats2.ShortDebugString() << Endl;
        Cerr << stats3.ShortDebugString() << Endl;

        aggregator.Add(stats1, "host1");
        aggregator.Add(stats2, "host2");
        aggregator.Add(stats3, "host3");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(),
            "AnonRss: 36 CGroupLimit: 66 MemTotal: 96 MemAvailable: 126 AllocatedMemory: 156 AllocatorCachesMemory: 186 HardLimit: 216 SoftLimit: 246 TargetUtilization: 276 ExternalConsumption: 306 SharedCacheConsumption: 336 SharedCacheLimit: 366 MemTableConsumption: 396 MemTableLimit: 426 QueryExecutionConsumption: 456 QueryExecutionLimit: 486");
    }

    Y_UNIT_TEST (Aggregate_Summarize_NoExternalConsumption_DifferentHosts) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);
        TMemoryStats stats2 = GenerateStats(2);
        TMemoryStats stats3 = GenerateStats(3);

        stats1.ClearExternalConsumption();
        stats2.ClearExternalConsumption();
        stats3.ClearExternalConsumption();

        Cerr << stats1.ShortDebugString() << Endl;
        Cerr << stats2.ShortDebugString() << Endl;
        Cerr << stats3.ShortDebugString() << Endl;

        aggregator.Add(stats1, "host1");
        aggregator.Add(stats2, "host2");
        aggregator.Add(stats3, "host3");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(),
            "AnonRss: 36 CGroupLimit: 66 MemTotal: 96 MemAvailable: 126 AllocatedMemory: 156 AllocatorCachesMemory: 186 HardLimit: 216 SoftLimit: 246 TargetUtilization: 276 SharedCacheConsumption: 336 SharedCacheLimit: 366 MemTableConsumption: 396 MemTableLimit: 426 QueryExecutionConsumption: 456 QueryExecutionLimit: 486");
    }

    Y_UNIT_TEST (Aggregate_Summarize_ExternalConsumption_OneHost) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);
        TMemoryStats stats2 = GenerateStats(2);
        TMemoryStats stats3 = GenerateStats(3);

        Cerr << stats1.ShortDebugString() << Endl;
        Cerr << stats2.ShortDebugString() << Endl;
        Cerr << stats3.ShortDebugString() << Endl;

        aggregator.Add(stats1, "host");
        aggregator.Add(stats2, "host");
        aggregator.Add(stats3, "host");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(),
            "AnonRss: 36 CGroupLimit: 66 MemTotal: 33 MemAvailable: 43 AllocatedMemory: 156 AllocatorCachesMemory: 186 HardLimit: 73 SoftLimit: 83 TargetUtilization: 93 ExternalConsumption: 80 SharedCacheConsumption: 336 SharedCacheLimit: 366 MemTableConsumption: 396 MemTableLimit: 426 QueryExecutionConsumption: 456 QueryExecutionLimit: 486");
    }

    Y_UNIT_TEST (Aggregate_Summarize_NoExternalConsumption_OneHost) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);
        TMemoryStats stats2 = GenerateStats(2);
        TMemoryStats stats3 = GenerateStats(3);

        Cerr << stats1.ShortDebugString() << Endl;
        Cerr << stats2.ShortDebugString() << Endl;
        Cerr << stats3.ShortDebugString() << Endl;

        stats1.ClearExternalConsumption();
        stats2.ClearExternalConsumption();
        stats3.ClearExternalConsumption();

        aggregator.Add(stats1, "host");
        aggregator.Add(stats2, "host");
        aggregator.Add(stats3, "host");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(),
            "AnonRss: 36 CGroupLimit: 66 MemTotal: 96 MemAvailable: 126 AllocatedMemory: 156 AllocatorCachesMemory: 186 HardLimit: 216 SoftLimit: 246 TargetUtilization: 276 SharedCacheConsumption: 336 SharedCacheLimit: 366 MemTableConsumption: 396 MemTableLimit: 426 QueryExecutionConsumption: 456 QueryExecutionLimit: 486");
    }

    Y_UNIT_TEST (Aggregate_ExternalConsumption_CollidingHosts) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats1 = GenerateStats(1);
        TMemoryStats stats2 = GenerateStats(2);
        TMemoryStats stats3 = GenerateStats(3);

        Cerr << stats1.ShortDebugString() << Endl;
        Cerr << stats2.ShortDebugString() << Endl;
        Cerr << stats3.ShortDebugString() << Endl;

        aggregator.Add(stats1, "host1");
        aggregator.Add(stats2, "host1");
        aggregator.Add(stats3, "host2");

        TMemoryStats aggregated = aggregator.Aggregate();

        Cerr << aggregated.ShortDebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(),
            "AnonRss: 36 CGroupLimit: 66 MemTotal: 65 MemAvailable: 85 AllocatedMemory: 156 AllocatorCachesMemory: 186 HardLimit: 145 SoftLimit: 165 TargetUtilization: 185 ExternalConsumption: 194 SharedCacheConsumption: 336 SharedCacheLimit: 366 MemTableConsumption: 396 MemTableLimit: 426 QueryExecutionConsumption: 456 QueryExecutionLimit: 486");
    }

    Y_UNIT_TEST(Compaction_Single) {
        TMemoryStatsAggregator aggregator;

        TMemoryStats stats;
        stats.SetCompactionConsumption(3);
        stats.SetCompactionLimit(4);

        aggregator.Add(stats, "host");

        TMemoryStats aggregated = aggregator.Aggregate();

        UNIT_ASSERT_VALUES_EQUAL(aggregated.ShortDebugString(), stats.ShortDebugString());
    }
}

}
