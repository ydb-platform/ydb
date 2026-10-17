#include "block_range_field_simple.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldSimpleTest)
{
    Y_UNIT_TEST(ShouldStoreOneContinuousRange)
    {
        TBlockRangeFieldSimple field;
        bool changed = false;

        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(21, 30), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..30]", field.Print());
        UNIT_ASSERT_VALUES_EQUAL(21, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldRejectOperationsRequiringMultipleRanges)
    {
        TBlockRangeFieldSimple field;
        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(10, 30), &changed));

        UNIT_ASSERT(
            !field.TryAdd(TBlockRange16::MakeClosedInterval(40, 50), &changed));
        UNIT_ASSERT(!field.TryRemove(
            TBlockRange16::MakeClosedInterval(15, 20),
            &changed));
    }

    Y_UNIT_TEST(ShouldImplementInterface)
    {
        TBlockRangeFieldSimple simple;
        IBlockRangeFieldImpl& field = simple;
        bool changed = false;

        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(field.TryRemove(
            TBlockRange16::MakeClosedInterval(10, 20),
            &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(field.Empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
