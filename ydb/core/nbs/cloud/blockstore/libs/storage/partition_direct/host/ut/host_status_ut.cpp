#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_status.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostStatusListTest)
{
    Y_UNIT_TEST(MakeRotating_VChunk0)
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
        UNIT_ASSERT(list.GetDisabled().Empty());
    }

    Y_UNIT_TEST(MakeRotating_VChunk1)
    {
        const auto list = THostStatusList::MakeRotating(5, 1, 3);
        UNIT_ASSERT(list.Get(0) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(3) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(4) == EHostStatus::HandOff);
    }

    Y_UNIT_TEST(MakeRotating_VChunk4)
    {
        const auto list = THostStatusList::MakeRotating(5, 4, 3);
        // Primary slots: (0+4)%5=4, (1+4)%5=0, (2+4)%5=1.
        UNIT_ASSERT(list.Get(0) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(3) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(4) == EHostStatus::Primary);
    }

    Y_UNIT_TEST(SetReflectsInMasks)
    {
        auto list = THostStatusList::MakeRotating(5, 0, 3);
        list.Set(1, EHostStatus::Disabled);

        UNIT_ASSERT_VALUES_EQUAL(2u, list.GetPrimary().Count());
        UNIT_ASSERT(list.GetPrimary().Test(0));
        UNIT_ASSERT(!list.GetPrimary().Test(1));
        UNIT_ASSERT(list.GetPrimary().Test(2));
        UNIT_ASSERT_VALUES_EQUAL(1u, list.GetDisabled().Count());
        UNIT_ASSERT(list.GetDisabled().Test(1));
        UNIT_ASSERT_VALUES_EQUAL(4u, list.GetActive().Count());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
