#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_status.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostStatusListTest)
{
    Y_UNIT_TEST(ShouldMakeRotatingForVChunk0)
    {
        const auto list = THostStatusList::MakeRotating(5, 0, 3);
        UNIT_ASSERT_VALUES_EQUAL(5u, list.HostCount());
        UNIT_ASSERT(list.Get(0) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(3) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(4) == EHostStatus::HandOff);
        UNIT_ASSERT_VALUES_EQUAL(3u, list.GetPrimary().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, list.GetHandOff().Count());
    }

    Y_UNIT_TEST(ShouldMakeRotatingForVChunk1)
    {
        const auto list = THostStatusList::MakeRotating(5, 1, 3);
        UNIT_ASSERT(list.Get(0) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(3) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(4) == EHostStatus::HandOff);
    }

    Y_UNIT_TEST(ShouldMakeRotatingForVChunk4)
    {
        const auto list = THostStatusList::MakeRotating(5, 4, 3);
        // Primary slots: (0+4)%5=4, (1+4)%5=0, (2+4)%5=1.
        UNIT_ASSERT(list.Get(0) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(3) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(4) == EHostStatus::Primary);
    }

    Y_UNIT_TEST(ShouldReflectSetInMasks)
    {
        auto list = THostStatusList::MakeRotating(5, 0, 3);
        list.Set(1, EHostStatus::Disabled);

        UNIT_ASSERT_VALUES_EQUAL(2u, list.GetPrimary().Count());
        UNIT_ASSERT(list.GetPrimary().Get(0));
        UNIT_ASSERT(!list.GetPrimary().Get(1));
        UNIT_ASSERT(list.GetPrimary().Get(2));
        UNIT_ASSERT_VALUES_EQUAL(4u, list.GetActive().Count());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
