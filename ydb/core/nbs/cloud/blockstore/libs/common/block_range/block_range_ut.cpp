#include "block_range.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeTest)
{
    Y_UNIT_TEST(Difference)
    {
        {   // cut left
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(0, 15));
            auto expect = TBlockRange64::MakeClosedInterval(16, 20);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut left
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(10, 15));
            auto expect = TBlockRange64::MakeClosedInterval(16, 20);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut right
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 25));
            auto expect = TBlockRange64::MakeClosedInterval(10, 15);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut right
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 20));
            auto expect = TBlockRange64::MakeClosedInterval(10, 15);
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        {   // cut from middle
            auto result = TBlockRange64::MakeClosedInterval(10, 20).Difference(
                TBlockRange64::MakeClosedInterval(16, 18));
            auto expectFirst = TBlockRange64::MakeClosedInterval(10, 15);
            auto expectSecond = TBlockRange64::MakeClosedInterval(19, 20);
            UNIT_ASSERT_VALUES_EQUAL(expectFirst, *result.First);
            UNIT_ASSERT_VALUES_EQUAL(expectSecond, *result.Second);
        }
    }

    Y_UNIT_TEST(MakeClosedIntervalWithLimit)
    {
        {
            const auto expect = TBlockRange64::MakeClosedInterval(10, 20);
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange64::MakeClosedIntervalWithLimit(10, 20, 30));
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange64::MakeClosedIntervalWithLimit(10, 30, 20));
        }
        {
            // Check ui32 overflow.
            const auto expect =
                TBlockRange32::MakeClosedInterval(10, TBlockRange32::MaxIndex);
            UNIT_ASSERT_VALUES_EQUAL(
                expect,
                TBlockRange32::MakeClosedIntervalWithLimit(
                    10,
                    static_cast<ui64>(TBlockRange32::MaxIndex) + 20,
                    static_cast<ui64>(TBlockRange32::MaxIndex) + 10));
        }
    }

    Y_UNIT_TEST(SplitByStripe)
    {
        const auto src = TBlockRange64::WithLength(10, 10);

        {
            const auto splitBy4 = src.Split(4);
            UNIT_ASSERT_VALUES_EQUAL(3, splitBy4.size());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(10, 2),
                splitBy4[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(12, 4),
                splitBy4[1]);
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(16, 4),
                splitBy4[2]);
        }

        {
            auto splitBy5 = src.Split(5);
            UNIT_ASSERT_VALUES_EQUAL(2, splitBy5.size());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(10, 5),
                splitBy5[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(15, 5),
                splitBy5[1]);
        }

        {
            auto splitBy10 = src.Split(10);
            UNIT_ASSERT_VALUES_EQUAL(1, splitBy10.size());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(10, 10),
                splitBy10[0]);
        }

        {
            auto splitBy100 = src.Split(100);
            UNIT_ASSERT_VALUES_EQUAL(1, splitBy100.size());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(10, 10),
                splitBy100[0]);
        }
    }

    Y_UNIT_TEST(SplitByZeroStripe)
    {
        const auto src = TBlockRange64::WithLength(10, 10);

        const auto nonSplit = src.Split(0);
        UNIT_ASSERT_VALUES_EQUAL(1, nonSplit.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(10, 10),
            nonSplit[0]);
    }

    Y_UNIT_TEST(SplitByOneBlock)
    {
        const auto src = TBlockRange64::WithLength(10, 2);

        const auto splitByOne = src.Split(1);
        UNIT_ASSERT_VALUES_EQUAL(2, splitByOne.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeOneBlock(10),
            splitByOne[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::MakeOneBlock(11),
            splitByOne[1]);
    }
}

}   // namespace NYdb::NBS::NBlockStore
