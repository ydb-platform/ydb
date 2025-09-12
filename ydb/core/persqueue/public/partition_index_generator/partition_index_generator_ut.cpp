#include "partition_index_generator.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

    Y_UNIT_TEST_SUITE(TPartitionIndexGeneratorTest) {
        Y_UNIT_TEST(TestBasicGeneration) {
            TPartitionIndexGenerator generator(100);

            for (ui32 i = 0; i < 100; ++i) {
                auto result = generator.GetNextUnreservedId();
                UNIT_ASSERT(result.has_value());
                UNIT_ASSERT_VALUES_EQUAL(result.value(), 100 + i);
            }
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReservation) {
            TPartitionIndexGenerator generator(100);

            auto reserveResult = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(reserveResult.has_value());

            auto reserveAgain = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(!reserveAgain.has_value());

            auto getReserved = generator.GetNextReservedId(105);
            UNIT_ASSERT(getReserved.has_value());
            UNIT_ASSERT_VALUES_EQUAL(getReserved.value(), 105);
            UNIT_ASSERT(!generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReservationSequenceSkip) {
            TPartitionIndexGenerator generator(100);

            auto reserveResult = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(reserveResult.has_value());

            auto reserveAgain = generator.ReservePartitionIndex(105, 0, false);
            UNIT_ASSERT(!reserveAgain.has_value());

            for (ui32 i = 0; i < 7; ++i) {
                auto result = generator.GetNextUnreservedId();
                UNIT_ASSERT(result.has_value());
                UNIT_ASSERT_VALUES_EQUAL(result.value(), 100 + i + (i >= 5 ? 1 : 0)); // Skip 105
            }

            // Verify 105 is availiable
            auto reservedResult = generator.GetNextReservedId(105);
            UNIT_ASSERT(reservedResult.has_value());
            UNIT_ASSERT_VALUES_EQUAL(reservedResult.value(), 105);

            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReuseExisting) {
            TPartitionIndexGenerator generator(100);

            auto reserveLow = generator.ReservePartitionIndex(99, 0, false);
            UNIT_ASSERT(!reserveLow.has_value());

            auto reserveLowAllowed = generator.ReservePartitionIndex(99, 0, true);
            UNIT_ASSERT(reserveLowAllowed.has_value());
            UNIT_ASSERT(!generator.ValidateAllocationSequence().has_value()); // no allocated
        }

        Y_UNIT_TEST(TestSplitMergeScenario) {
            TPartitionIndexGenerator generator(100);

            UNIT_ASSERT(generator.ReservePartitionIndex(101, 100, false).has_value());
            UNIT_ASSERT(generator.ReservePartitionIndex(102, 100, false).has_value());
            UNIT_ASSERT(generator.ReservePartitionIndex(103, 101, false).has_value());

            UNIT_ASSERT(generator.GetNextReservedId(101).has_value());
            UNIT_ASSERT(generator.GetNextReservedId(102).has_value());
            UNIT_ASSERT(generator.GetNextReservedId(103).has_value());

            auto nextUnreserved = generator.GetNextUnreservedId();
            UNIT_ASSERT(nextUnreserved.has_value());
            UNIT_ASSERT_VALUES_EQUAL(nextUnreserved.value(), 100);
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestValidationFailures) {
            TPartitionIndexGenerator generator(100);

            UNIT_ASSERT(generator.ReservePartitionIndex(101, 0, false).has_value());
            auto validation = generator.ValidateAllocationSequence();
            UNIT_ASSERT(!validation.has_value());
            UNIT_ASSERT_STRINGS_EQUAL(validation.error(), "Partition id (101) is reserved but not allocated");
        }

        Y_UNIT_TEST(TestReserve2) {
            TPartitionIndexGenerator generator(200);
            UNIT_ASSERT(generator.ReservePartitionIndex(200, 0, false).has_value());
            UNIT_ASSERT_VALUES_EQUAL(generator.GetNextUnreservedId().value(), 201);
            UNIT_ASSERT_VALUES_EQUAL(generator.GetNextReservedId(200).value(), 200);
            UNIT_ASSERT_EXCEPTION(generator.GetNextReservedId(200).value(), std::exception);
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }

        Y_UNIT_TEST(TestReserve3) {
            TPartitionIndexGenerator generator(200);
            UNIT_ASSERT_VALUES_EQUAL(generator.GetNextUnreservedId().value(), 200);
            UNIT_ASSERT_EXCEPTION(generator.GetNextReservedId(200).value(), std::exception);
            UNIT_ASSERT(generator.ValidateAllocationSequence().has_value());
        }
    } // Y_UNIT_TEST_SUITE(TPartitionIndexGeneratorTest)
} // namespace NKikimr::NPQ
