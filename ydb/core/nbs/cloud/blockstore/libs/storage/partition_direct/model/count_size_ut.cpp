#include "count_size.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCountAndSizeTest)
{
    Y_UNIT_TEST(ShouldAddAndSubtractItems)
    {
        TCountAndSize value;

        value.Add(1024);
        value.Add(2048);
        UNIT_ASSERT_VALUES_EQUAL(2, value.Count);
        UNIT_ASSERT_VALUES_EQUAL(3072, value.Size);

        value.Sub(1024);
        UNIT_ASSERT_VALUES_EQUAL(1, value.Count);
        UNIT_ASSERT_VALUES_EQUAL(2048, value.Size);
    }

    Y_UNIT_TEST(ShouldAccumulateAndPrint)
    {
        TCountAndSize value{.Count = 2, .Size = 1024};
        value += TCountAndSize{.Count = 3, .Size = 2048};

        UNIT_ASSERT_VALUES_EQUAL(5, value.Count);
        UNIT_ASSERT_VALUES_EQUAL(3072, value.Size);
        UNIT_ASSERT_VALUES_EQUAL("5 / 3072", value.Print(false));
        UNIT_ASSERT_VALUES_EQUAL("5 / 3.00 KiB", value.Print(true));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
