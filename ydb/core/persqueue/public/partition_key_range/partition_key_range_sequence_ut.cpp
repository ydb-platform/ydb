#include "partition_key_range_sequence.h"
#include <library/cpp/testing/unittest/registar.h>
#include <span>

using namespace NKikimr::NPQ;

Y_UNIT_TEST_SUITE(TPartitionKeyRangeSequenceTest) {
    Y_UNIT_TEST(ValidSequence) {
        // Simple valid sequence with 3 partitions
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x45"},
            {1, "\x45", "\x4F"},
            {2, "\x4F", Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT_C(result.has_value(), result.error());
    }

    Y_UNIT_TEST(ValidSinglePartition) {
        // Single partition covering full range
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT_C(result.has_value(), result.error());
    }

    Y_UNIT_TEST(InvalidOverlap) {
        // Overlapping partitions
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x4F"},
            {1, "\x45", "\x55"},
            {2, "\x55", Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "overlap");
    }

    Y_UNIT_TEST(InvalidOverlapLong) {
        // Overlapping partitions
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x41"},
            {1, "\x41", "\x45"},
            {2, "\x45", "\x4F"},
            {3, "\x4F", "\x55"},
            {4, "\x55", Nothing()},
            {5, "\x4A", "\x51"},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "overlap");
    }

    Y_UNIT_TEST(InvalidContains) {
        // Overlapping partitions
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), Nothing()},
            {1, "\x45", "\x55"},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "overlap");
    }

    Y_UNIT_TEST(InvalidGap) {
        // Gap between partitions
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x45"},
            {1, "\x4F", Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "bounds gap");
    }

    Y_UNIT_TEST(InvalidOrder) {
        // Misordered bounds
        const TPartitionKeyRangeView ranges[] = {
            {0, "\x4F", "\x45"},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "invalid bounds range");
    }

    Y_UNIT_TEST(InvalidFullCoverHi) {
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x4F"},
            {1, "\x4F", "\x55"},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "doesn't have the highest bound");
    }

    Y_UNIT_TEST(InvalidFullCoverLo) {
        const TPartitionKeyRangeView ranges[] = {
            {0, "\x45", "\x4F"},
            {1, "\x4F", Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "doesn't have the lowest bound");
    }

    Y_UNIT_TEST(EmptyInput) {
        // Empty input
        const TPartitionKeyRangeView ranges[] = {};
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, 0));
        UNIT_ASSERT(!result.has_value());
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "Empty partitions list");
    }

    Y_UNIT_TEST(ValidFivePartitions) {
        // Valid sequence with 5 partitions using all specified boundaries
        const TPartitionKeyRangeView ranges[] = {
            {0, Nothing(), "\x41"},
            {1, "\x41", "\x45"},
            {2, "\x45", "\x4F"},
            {3, "\x4F", "\x55"},
            {4, "\x55", Nothing()},
        };
        auto result = ValidateKeyRangeSequence(std::span<const TPartitionKeyRangeView>(ranges, sizeof(ranges)/sizeof(ranges[0])));
        UNIT_ASSERT_C(result.has_value(), result.error());
    }
}
