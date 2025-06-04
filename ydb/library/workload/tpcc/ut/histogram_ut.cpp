#include <ydb/library/workload/tpcc/histogram.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTPCC;

Y_UNIT_TEST_SUITE(THistogramTest) {
    Y_UNIT_TEST(ShouldInitializeCorrectly) {
        THistogram hist(4, 16);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 0); // Empty histogram
    }

    Y_UNIT_TEST(ShouldThrowOnInvalidParameters) {
        UNIT_ASSERT_EXCEPTION(THistogram(0, 10), std::invalid_argument);  // hdrTill = 0
        UNIT_ASSERT_EXCEPTION(THistogram(10, 0), std::invalid_argument);  // maxValue = 0
        UNIT_ASSERT_EXCEPTION(THistogram(20, 10), std::invalid_argument); // hdrTill > maxValue
    }

    Y_UNIT_TEST(ShouldRecordValuesInLinearBuckets) {
        THistogram hist(4, 16);

        // Test values in linear buckets [0,1), [1,2), [2,3), [3,4)
        hist.RecordValue(0);
        hist.RecordValue(1);
        hist.RecordValue(2);
        hist.RecordValue(3);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(25.0), 1);  // 25% of 4 values = 1st value (0)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 2);  // 50% of 4 values = 2nd value (1)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(75.0), 3);  // 75% of 4 values = 3rd value (2)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 4); // 100% of 4 values = 4th value (3)
    }

    Y_UNIT_TEST(ShouldRecordValuesInExponentialBuckets) {
        THistogram hist(4, 16);

        // Test values in exponential buckets [4,8), [8,16)
        hist.RecordValue(4);
        hist.RecordValue(7);
        hist.RecordValue(8);
        hist.RecordValue(15);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 8);  // 50% of 4 values = 2nd value
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(75.0), 16); // 75% of 4 values = 3rd value
    }

    Y_UNIT_TEST(ShouldHandleValuesAboveMaxValue) {
        THistogram hist(4, 16);

        hist.RecordValue(20);
        hist.RecordValue(100);
        hist.RecordValue(1000);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 1000); // Should return max recorded value
    }

    Y_UNIT_TEST(ShouldHandleZeroValues) {
        THistogram hist(4, 16);

        hist.RecordValue(0);
        hist.RecordValue(0);
        hist.RecordValue(1);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(33.3), 1);  // 33.3% of 3 values = 1st value (0)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(66.6), 1);  // 66.6% of 3 values = 2nd value (0)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 2); // 100% of 3 values = 3rd value (1)
    }

    Y_UNIT_TEST(ShouldAddHistograms) {
        THistogram hist1(4, 16);
        THistogram hist2(4, 16);

        hist1.RecordValue(0);
        hist1.RecordValue(1);
        hist2.RecordValue(2);
        hist2.RecordValue(3);

        hist1.Add(hist2);

        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(25.0), 1);  // 25% of 4 values = 1st value (0)
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(50.0), 2);  // 50% of 4 values = 2nd value (1)
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(75.0), 3);  // 75% of 4 values = 3rd value (2)
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(100.0), 4); // 100% of 4 values = 4th value (3)
    }

    Y_UNIT_TEST(ShouldThrowOnAddingDifferentHistograms) {
        THistogram hist1(4, 16);
        THistogram hist2(5, 16);
        THistogram hist3(4, 20);

        UNIT_ASSERT_EXCEPTION(hist1.Add(hist2), std::invalid_argument); // Different hdrTill
        UNIT_ASSERT_EXCEPTION(hist1.Add(hist3), std::invalid_argument); // Different maxValue
    }

    Y_UNIT_TEST(ShouldResetHistogram) {
        THistogram hist(4, 16);

        hist.RecordValue(0);
        hist.RecordValue(1);
        hist.RecordValue(2);

        hist.Reset();

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 0); // Empty histogram
    }

    Y_UNIT_TEST(ShouldHandlePercentileEdgeCases) {
        THistogram hist(4, 16);

        hist.RecordValue(0);
        hist.RecordValue(1);
        hist.RecordValue(2);

        UNIT_ASSERT_EXCEPTION(hist.GetValueAtPercentile(-1.0), std::invalid_argument);  // Negative percentile
        UNIT_ASSERT_EXCEPTION(hist.GetValueAtPercentile(101.0), std::invalid_argument); // Percentile > 100
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(0.0), 1);   // 0th percentile
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 3); // 100th percentile
    }

    Y_UNIT_TEST(ShouldHandleEmptyHistogram) {
        THistogram hist(4, 16);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(0.0), 0);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 0);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 0);
    }

    Y_UNIT_TEST(ShouldHandleSingleValueHistogram) {
        THistogram hist(4, 16);

        hist.RecordValue(0);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(0.0), 1);   // Upper bound of bucket [0,1)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 1);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 1);
    }

    Y_UNIT_TEST(ShouldHandleLargeValues) {
        THistogram hist(4, 16);

        hist.RecordValue(std::numeric_limits<uint64_t>::max());
        hist.RecordValue(std::numeric_limits<uint64_t>::max() - 1);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), std::numeric_limits<uint64_t>::max());
    }

    Y_UNIT_TEST(ShouldHandleAllBucketTypes) {
        THistogram hist(4, 16);

        // Test all bucket types: [0,1), [1,2), [2,3), [3,4), [4,8), [8,16), [16,∞)
        hist.RecordValue(0);  // bucket 0: [0,1)
        hist.RecordValue(1);  // bucket 1: [1,2)
        hist.RecordValue(2);  // bucket 2: [2,3)
        hist.RecordValue(3);  // bucket 3: [3,4)
        hist.RecordValue(4);  // bucket 4: [4,8)
        hist.RecordValue(7);  // bucket 4: [4,8)
        hist.RecordValue(8);  // bucket 5: [8,16)
        hist.RecordValue(15); // bucket 5: [8,16)
        hist.RecordValue(20); // bucket 6: [16,∞)

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(11.1), 1);  // ~11% of 9 values = 1st value (0)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(22.2), 2);  // ~22% of 9 values = 2nd value (1)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(33.3), 3);  // ~33% of 9 values = 3rd value (2)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(44.4), 4);  // ~44% of 9 values = 4th value (3)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(55.5), 8);  // ~55% of 9 values = 5th value (4)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(77.7), 16); // ~77% of 9 values = 7th value (8)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 20); // 100% of 9 values = 9th value (20)
    }

    Y_UNIT_TEST(ShouldHandleLargeHistogram) {
        // Test with hdrTill=128, maxValue=8192
        // Linear buckets: [0,1), [1,2), ..., [127,128)  (128 buckets)
        // Exponential buckets: [128,256), [256,512), [512,1024), [1024,2048), [2048,4096), [4096,8192), [8192,∞)
        THistogram hist(128, 8192);

        // Test values across all bucket types
        for (uint64_t i = 0; i < 128; ++i) {
            hist.RecordValue(i);  // Linear buckets
        }
        hist.RecordValue(200);   // [128,256)
        hist.RecordValue(400);   // [256,512)
        hist.RecordValue(800);   // [512,1024)
        hist.RecordValue(1500);  // [1024,2048)
        hist.RecordValue(3000);  // [2048,4096)
        hist.RecordValue(6000);  // [4096,8192)
        hist.RecordValue(10000); // [8192,∞)

        // Total: 128 + 7 = 135 values
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 68);   // 50% of 135 = 67.5, so 68th value (67), bucket 67, upper bound 68
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(95.0), 256);  // 95% of 135 = 128.25, so 129th value (200), bucket 128, upper bound 256
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(99.0), 8192); // 99% of 135 = 133.65, so 134th value (6000), bucket 133, upper bound 8192
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 10000); // Last value (10000)
    }

    Y_UNIT_TEST(ShouldHandleBucketBoundaries) {
        THistogram hist(4, 16);

        // Test values exactly at bucket boundaries
        hist.RecordValue(0);  // [0,1)
        hist.RecordValue(1);  // [1,2)
        hist.RecordValue(2);  // [2,3)
        hist.RecordValue(3);  // [3,4)
        hist.RecordValue(4);  // [4,8)
        hist.RecordValue(8);  // [8,16)
        hist.RecordValue(16); // [16,∞)

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(14.3), 2);  // ~14% of 7 = 1st value (0), bucket 0, upper bound 1 -> actually 2nd value (1), upper bound 2
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(28.6), 3);  // ~29% of 7 = 2nd value (1), bucket 1, upper bound 2 -> actually 3rd value (2), upper bound 3
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(42.9), 4);  // ~43% of 7 = 3rd value (2), bucket 2, upper bound 3 -> actually 4th value (3), upper bound 4
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(57.1), 4);  // ~57% of 7 = 4th value (3), bucket 3, upper bound 4
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(71.4), 8);  // ~71% of 7 = 5th value (4), bucket 4, upper bound 8
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(85.7), 16); // ~86% of 7 = 6th value (8), bucket 5, upper bound 16
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 16); // 7th value (16)
    }

    Y_UNIT_TEST(ShouldHandleVerySmallPercentiles) {
        THistogram hist(4, 16);

        // Add 1000 values to test very small percentiles
        for (int i = 0; i < 1000; ++i) {
            hist.RecordValue(i % 4); // Values 0, 1, 2, 3 repeated
        }

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(0.1), 1);   // 0.1% of 1000 = 1st value
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(0.5), 1);   // 0.5% of 1000 = 5th value
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(1.0), 1);   // 1% of 1000 = 10th value
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(99.0), 4);  // 99% of 1000 = 990th value
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(99.9), 4);  // 99.9% of 1000 = 999th value
    }

    Y_UNIT_TEST(ShouldHandleExactPercentileBoundaries) {
        THistogram hist(4, 16);

        // Create a histogram with exactly 100 values for precise percentile testing
        for (int i = 0; i < 25; ++i) {
            hist.RecordValue(0);  // 25 values in [0,1)
            hist.RecordValue(1);  // 25 values in [1,2)
            hist.RecordValue(2);  // 25 values in [2,3)
            hist.RecordValue(3);  // 25 values in [3,4)
        }

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(25.0), 1);  // 25% = 25th value (last 0)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 2);  // 50% = 50th value (last 1)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(75.0), 3);  // 75% = 75th value (last 2)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 4); // 100% = 100th value (last 3)
    }

    Y_UNIT_TEST(ShouldHandleMaxValueBoundary) {
        THistogram hist(4, 16);

        hist.RecordValue(15);  // Just below maxValue, should go to [8,16)
        hist.RecordValue(16);  // Equal to maxValue, should go to [16,∞)
        hist.RecordValue(17);  // Above maxValue, should go to [16,∞)

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(33.3), 16); // 33% of 3 = 1st value (15)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 17); // Last value (17)
    }

    Y_UNIT_TEST(ShouldHandleRepeatedValues) {
        THistogram hist(4, 16);

        // Add many identical values
        for (int i = 0; i < 100; ++i) {
            hist.RecordValue(2); // All values in [2,3)
        }

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(1.0), 3);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(50.0), 3);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(99.0), 3);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), 3);
    }

    Y_UNIT_TEST(ShouldHandleDifferentHistogramSizes) {
        // Test with different hdrTill/maxValue combinations
        THistogram hist1(2, 8);    // Small histogram
        THistogram hist2(16, 64);  // Medium histogram
        THistogram hist3(64, 1024); // Large histogram

        hist1.RecordValue(0);
        hist1.RecordValue(1);
        hist1.RecordValue(2);
        hist1.RecordValue(4);
        hist1.RecordValue(8);

        hist2.RecordValue(0);
        hist2.RecordValue(8);
        hist2.RecordValue(16);
        hist2.RecordValue(32);
        hist2.RecordValue(64);

        hist3.RecordValue(0);
        hist3.RecordValue(32);
        hist3.RecordValue(64);
        hist3.RecordValue(128);
        hist3.RecordValue(1024);

        // Test that all histograms work correctly
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(40.0), 2);  // 40% of 5 = 2nd value (1), bucket 1, upper bound 2
        UNIT_ASSERT_VALUES_EQUAL(hist2.GetValueAtPercentile(40.0), 9);  // 2nd value (8), bucket 8, upper bound 9
        UNIT_ASSERT_VALUES_EQUAL(hist3.GetValueAtPercentile(40.0), 33); // 2nd value (32), bucket 32, upper bound 33
    }

    Y_UNIT_TEST(ShouldHandleEdgeCaseValues) {
        THistogram hist(4, 16);

        // Test with maximum possible values
        hist.RecordValue(0);
        hist.RecordValue(std::numeric_limits<uint64_t>::max());
        hist.RecordValue(std::numeric_limits<uint64_t>::max() - 1);

        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(33.3), 1);
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), std::numeric_limits<uint64_t>::max());
    }

    Y_UNIT_TEST(ShouldHandleStressTest) {
        THistogram hist(128, 8192);

        // Add many random values to test performance and correctness
        for (uint64_t i = 0; i < 10000; ++i) {
            uint64_t value = (i * 17 + 13) % 10000; // Pseudo-random values
            hist.RecordValue(value);
        }

        // Test that percentiles are monotonic
        uint64_t prev = 0;
        for (double p = 1.0; p <= 100.0; p += 1.0) {
            uint64_t current = hist.GetValueAtPercentile(p);
            UNIT_ASSERT(current >= prev);
            prev = current;
        }
    }

    Y_UNIT_TEST(ShouldHandleHistogramMerging) {
        THistogram hist1(128, 8192);
        THistogram hist2(128, 8192);
        THistogram hist3(128, 8192);

        // Fill histograms with different value ranges
        for (uint64_t i = 0; i < 100; ++i) {
            hist1.RecordValue(i);          // [0,100)
            hist2.RecordValue(i + 1000);   // [1000,1100)
            hist3.RecordValue(i + 5000);   // [5000,5100)
        }

        // Merge them all into hist1
        hist1.Add(hist2);
        hist1.Add(hist3);

        // Should have 300 values total
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(33.3), 100);  // ~33% = 100th value (99), bucket 99, upper bound 100
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(66.6), 2048); // ~67% = 200th value (1099), bucket [1024,2048), upper bound 2048
        UNIT_ASSERT_VALUES_EQUAL(hist1.GetValueAtPercentile(100.0), 8192); // Last exponential bucket upper bound
    }

    Y_UNIT_TEST(ShouldHandleSpecific128_8192Case) {
        // Comprehensive test for hdrTill=128, maxValue=8192
        THistogram hist(128, 8192);

        // Test bucket structure:
        // Linear: [0,1), [1,2), ..., [127,128) - 128 buckets (indices 0-127)
        // Exponential: [128,256), [256,512), [512,1024), [1024,2048), [2048,4096), [4096,8192) - 6 buckets (indices 128-133)
        // Overflow: [8192,∞) - 1 bucket (index 134)

        // Test linear bucket boundaries
        hist.RecordValue(0);    // bucket 0: [0,1)
        hist.RecordValue(1);    // bucket 1: [1,2)
        hist.RecordValue(63);   // bucket 63: [63,64)
        hist.RecordValue(127);  // bucket 127: [127,128)

        // Test exponential bucket boundaries
        hist.RecordValue(128);  // bucket 128: [128,256)
        hist.RecordValue(255);  // bucket 128: [128,256)
        hist.RecordValue(256);  // bucket 129: [256,512)
        hist.RecordValue(511);  // bucket 129: [256,512)
        hist.RecordValue(512);  // bucket 130: [512,1024)
        hist.RecordValue(1023); // bucket 130: [512,1024)
        hist.RecordValue(1024); // bucket 131: [1024,2048)
        hist.RecordValue(2047); // bucket 131: [1024,2048)
        hist.RecordValue(2048); // bucket 132: [2048,4096)
        hist.RecordValue(4095); // bucket 132: [2048,4096)
        hist.RecordValue(4096); // bucket 133: [4096,8192)
        hist.RecordValue(8191); // bucket 133: [4096,8192)

        // Test overflow bucket
        hist.RecordValue(8192);  // bucket 134: [8192,∞)
        hist.RecordValue(10000); // bucket 134: [8192,∞)
        hist.RecordValue(std::numeric_limits<uint64_t>::max()); // bucket 134: [8192,∞)

        // Total: 19 values
        // Verify percentiles
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(5.3), 2);    // ~5% = 2nd value (1), bucket 1, upper bound 2
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(10.5), 2);   // ~11% = 2nd value (1)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(26.3), 256); // ~26% = 5th value (128), bucket 128, upper bound 256
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(47.4), 1024); // ~47% = 9th value (512), bucket [512,1024), upper bound 1024
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(84.2), 8192); // ~84% = 16th value (4096)
        UNIT_ASSERT_VALUES_EQUAL(hist.GetValueAtPercentile(100.0), std::numeric_limits<uint64_t>::max()); // Last value

        // Test corner case: exactly at maxValue
        THistogram hist2(128, 8192);
        hist2.RecordValue(8192); // Should go to overflow bucket
        UNIT_ASSERT_VALUES_EQUAL(hist2.GetValueAtPercentile(100.0), 8192);

        // Test edge case: just below maxValue
        THistogram hist3(128, 8192);
        hist3.RecordValue(8191); // Should go to [4096,8192)
        UNIT_ASSERT_VALUES_EQUAL(hist3.GetValueAtPercentile(100.0), 8192);
    }
}
