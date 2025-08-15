#include <ydb/core/persqueue/partition_index_generator/partition_index_generator.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

    Y_UNIT_TEST_SUITE(TPartitionIndexGeneratorTest) {
        Y_UNIT_TEST(TestBasicGeneration) {
            TPartitionIndexGenerator generator(100, 1);

            for (ui32 i = 0; i < 100; ++i) {
                auto result = generator.GetNextUnreservedIdAndGroupId();
                UNIT_ASSERT(result.has_value());
                UNIT_ASSERT_VALUES_EQUAL(result->Id, 100 + i);
                UNIT_ASSERT_VALUES_EQUAL(result->GroupId, i + 1);
            }
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReservation) {
            TPartitionIndexGenerator generator(100, 1);

            auto reserveResult = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(reserveResult.has_value());

            auto reserveAgain = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(!reserveAgain.has_value());

            auto getReserved = generator.GetNextReservedIdAndGroupId(105);
            UNIT_ASSERT(getReserved.has_value());
            UNIT_ASSERT_VALUES_EQUAL(getReserved->Id, 105);
            UNIT_ASSERT_VALUES_EQUAL(getReserved->GroupId, 6);
            UNIT_ASSERT(!generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReservationSequenceSkip) {
            TPartitionIndexGenerator generator(100, 1);

            auto reserveResult = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(reserveResult.has_value());

            auto reserveAgain = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(!reserveAgain.has_value());

            for (ui32 i = 0; i < 7; ++i) {
                auto result = generator.GetNextUnreservedIdAndGroupId();
                UNIT_ASSERT(result.has_value());
                UNIT_ASSERT_VALUES_EQUAL(result->Id, 100 + i + (i >= 5 ? 1 : 0)); // Skip 105
                UNIT_ASSERT_VALUES_EQUAL(result->GroupId, 1 + i + (i >= 5 ? 1 : 0));
            }

            // Verify 105 is availiable
            auto reservedResult = generator.GetNextReservedIdAndGroupId(105);
            UNIT_ASSERT(reservedResult.has_value());
            UNIT_ASSERT_VALUES_EQUAL(reservedResult->Id, 105);
            UNIT_ASSERT_VALUES_EQUAL(reservedResult->GroupId, 6);

            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReuseExisting) {
            TPartitionIndexGenerator generator(100, 1);

            auto reserveLow = generator.ReservePartitionIndex(99, 0, false);
            UNIT_ASSERT(!reserveLow.has_value());

            auto reserveLowAllowed = generator.ReservePartitionIndex(99, 0, true);
            UNIT_ASSERT(reserveLowAllowed.has_value());
            UNIT_ASSERT(!generator.ValidateAllocationSequence().has_value()); // no allocated
        }

        Y_UNIT_TEST(TestSplitMergeScenario) {
            TPartitionIndexGenerator generator(100, 1);

            UNIT_ASSERT(generator.ReservePartitionIndex(101, 100, false).has_value());
            UNIT_ASSERT(generator.ReservePartitionIndex(102, 100, false).has_value());
            UNIT_ASSERT(generator.ReservePartitionIndex(103, 101, false).has_value());

            UNIT_ASSERT(generator.GetNextReservedIdAndGroupId(101).has_value());
            UNIT_ASSERT(generator.GetNextReservedIdAndGroupId(102).has_value());
            UNIT_ASSERT(generator.GetNextReservedIdAndGroupId(103).has_value());

            auto nextUnreserved = generator.GetNextUnreservedIdAndGroupId();
            UNIT_ASSERT(nextUnreserved.has_value());
            UNIT_ASSERT_VALUES_EQUAL(nextUnreserved->Id, 100);
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestValidationFailures) {
            TPartitionIndexGenerator generator(100, 1);

            UNIT_ASSERT(generator.ReservePartitionIndex(101, 0, false).has_value());
            auto validation = generator.ValidateAllocationSequence();
            UNIT_ASSERT(!validation.has_value());
            UNIT_ASSERT_STRINGS_EQUAL(validation.error(), "Partition id (101) is reserved but not allocated");
        }

        Y_UNIT_TEST(TestGroupIdGeneration) {
            TPartitionIndexGenerator generator(100, 10);

            auto result1 = generator.GetNextUnreservedIdAndGroupId();
            UNIT_ASSERT(result1.has_value());
            UNIT_ASSERT_VALUES_EQUAL(result1->Id, 100);
            UNIT_ASSERT_VALUES_EQUAL(result1->GroupId, 10);

            auto result2 = generator.GetNextUnreservedIdAndGroupId();
            UNIT_ASSERT(result2.has_value());
            UNIT_ASSERT_VALUES_EQUAL(result2->Id, 101);
            UNIT_ASSERT_VALUES_EQUAL(result2->GroupId, 11);
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }
    } // Y_UNIT_TEST_SUITE(TPartitionIndexGeneratorTest)
} // namespace NKikimr::NPQ
