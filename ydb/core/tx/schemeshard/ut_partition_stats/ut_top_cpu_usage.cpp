#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

/**
 * Create the TPartitionStats::TTopCpuUsage container will all times set to unique values.
 *
 * @return The CPU usage container with all times set to unique values.
 */
TPartitionStats::TTopCpuUsage MakeTopCpuUsageWithUniqueTimes() {
    TPartitionStats::TTopCpuUsage top_cpu_usage;

    for (ui64 i = 0; i < top_cpu_usage.BucketUpdateTimes.size(); ++i) {
        // Set older time stamps to higher CPU usage values to make it easier
        // to verify the time-based lookup requests
        top_cpu_usage.BucketUpdateTimes[i] = TInstant::MicroSeconds(
            1000 + top_cpu_usage.BucketUpdateTimes.size() - i
        );
    }

    return top_cpu_usage;
}

/**
 * Convert the TPartitionStats::TTopCpuUsage container to a pretty-printed string.
 *
 * @param[in] top_cpu_usage The container to convert to a string
 *
 * @return The corresponding pretty-printed string
 */
TString PrintTopCpuUsage(const TPartitionStats::TTopCpuUsage& top_cpu_usage) {
    auto builder = TStringBuilder() << "\nTopCpuUsage = [\n";

    for (ui64 i = 0; i < top_cpu_usage.Buckets.size(); ++i) {
        builder << Sprintf(
            "  %2u%% -> %u\n",
            top_cpu_usage.Buckets[i].LowBoundary,
            top_cpu_usage.BucketUpdateTimes[i]
        );
    }

    return builder << "]\n";
}

} // namespace <anonymous>

/**
 * Unit tests for the TPartitionStats::TTopCpuUsage class.
 */
Y_UNIT_TEST_SUITE(TSchemeShardPartitionStatsTopCpuUsageTest) {
    /**
     * Verify that TTopCpuUsage::Update() works correctly.
     */
    Y_UNIT_TEST(Update) {
        TPartitionStats::TTopCpuUsage top_cpu_usage1 = MakeTopCpuUsageWithUniqueTimes();

        // Unset (== 0) and smaller values should be ignore, larger values should be kept
        TPartitionStats::TTopCpuUsage top_cpu_usage2;

        top_cpu_usage2.BucketUpdateTimes[1] = TInstant::MicroSeconds(2001);
        top_cpu_usage2.BucketUpdateTimes[2] = TInstant::MicroSeconds(202);
        top_cpu_usage2.BucketUpdateTimes[3] = TInstant::MicroSeconds(2003);

        top_cpu_usage1.Update(top_cpu_usage2);

        std::array<TInstant, 5> expectedTimes = {{
            TInstant::MicroSeconds(1005),
            TInstant::MicroSeconds(2001),
            TInstant::MicroSeconds(1003),
            TInstant::MicroSeconds(2003),
            TInstant::MicroSeconds(1001),
        }};

        UNIT_ASSERT_EQUAL_C(
            top_cpu_usage1.BucketUpdateTimes,
            expectedTimes,
            PrintTopCpuUsage(top_cpu_usage1)
        );
    }

    /**
     * Verify that TTopCpuUsage::UpdateCpuUsage() does not update any buckets,
     * if the given CPU usage percentage does not satisfy any CPU usage threshold.
     */
    Y_UNIT_TEST(UpdateCpuUsage_NoBuckets) {
        TPartitionStats::TTopCpuUsage top_cpu_usage = MakeTopCpuUsageWithUniqueTimes();
        top_cpu_usage.UpdateCpuUsage(10000 /* 1% */, TInstant::MicroSeconds(123456));

        std::array<TInstant, 5> expectedTimes = {{
            TInstant::MicroSeconds(1005),
            TInstant::MicroSeconds(1004),
            TInstant::MicroSeconds(1003),
            TInstant::MicroSeconds(1002),
            TInstant::MicroSeconds(1001),
        }};

        UNIT_ASSERT_EQUAL_C(
            top_cpu_usage.BucketUpdateTimes,
            expectedTimes,
            PrintTopCpuUsage(top_cpu_usage)
        );
    }

    /**
     * Verify that TTopCpuUsage::UpdateCpuUsage() updates the correct buckets,
     * if the given CPU usage percentage satisfies only some of the CPU usage thresholds.
     */
    Y_UNIT_TEST(UpdateCpuUsage_SomeBuckets) {
        TPartitionStats::TTopCpuUsage top_cpu_usage(MakeTopCpuUsageWithUniqueTimes());
        top_cpu_usage.UpdateCpuUsage(150000 /* 15% */, TInstant::MicroSeconds(123456));

        std::array<TInstant, 5> expectedTimes = {{
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(1002),
            TInstant::MicroSeconds(1001),
        }};

        UNIT_ASSERT_EQUAL_C(
            top_cpu_usage.BucketUpdateTimes,
            expectedTimes,
            PrintTopCpuUsage(top_cpu_usage)
        );
    }

    /**
     * Verify that TTopCpuUsage::UpdateCpuUsage() updates all buckets,
     * if the given CPU usage percentage satisfies all CPU usage thresholds.
     */
    Y_UNIT_TEST(UpdateCpuUsage_AllBuckets) {
        TPartitionStats::TTopCpuUsage top_cpu_usage = MakeTopCpuUsageWithUniqueTimes();
        top_cpu_usage.UpdateCpuUsage(310000 /* 31% */, TInstant::MicroSeconds(123456));

        std::array<TInstant, 5> expectedTimes = {{
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
            TInstant::MicroSeconds(123456),
        }};

        UNIT_ASSERT_EQUAL_C(
            top_cpu_usage.BucketUpdateTimes,
            expectedTimes,
            PrintTopCpuUsage(top_cpu_usage)
        );
    }

    /**
     * Verify that TTopCpuUsage::GetLatestMaxCpuUsagePercent() works correctly
     * for all threshold values.
     */
    Y_UNIT_TEST(GetLatestMaxCpuUsagePercent) {
        TPartitionStats::TTopCpuUsage top_cpu_usage = MakeTopCpuUsageWithUniqueTimes();

        for (const auto [since, expectedCpuUsage] : std::array<std::pair<ui64, ui32>, 6>{
            std::make_pair(1005, 2), // The default value - no bucket for this time stamp
            std::make_pair(1000, 40),
            std::make_pair(1001, 30),
            std::make_pair(1002, 20),
            std::make_pair(1003, 10),
            std::make_pair(1004, 5),
        }) {
            auto cpuUsage = top_cpu_usage.GetLatestMaxCpuUsagePercent(TInstant::MicroSeconds(since));

            UNIT_ASSERT_EQUAL_C(
                cpuUsage,
                expectedCpuUsage,
                Sprintf(
                    "Received %u%% for the time stamp %u, expected %u%%",
                    cpuUsage,
                    since,
                    expectedCpuUsage
                )
            );
        }
    }
}
