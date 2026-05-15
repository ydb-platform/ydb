#include "host_roles.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostRolesTest)
{
    Y_UNIT_TEST(ShouldMakeRotatingForVChunk0)
    {
        const auto list = THostRoles::MakeRotating(5, 0, 3);
        UNIT_ASSERT_VALUES_EQUAL(5u, list.HostCount());
        UNIT_ASSERT(list.GetRole(0) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(1) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(2) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(3) == EHostRole::HandOff);
        UNIT_ASSERT(list.GetRole(4) == EHostRole::HandOff);
        UNIT_ASSERT_VALUES_EQUAL(3u, list.GetPrimary().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, list.GetHandOff().Count());
    }

    Y_UNIT_TEST(ShouldMakeRotatingForVChunk1)
    {
        const auto list = THostRoles::MakeRotating(5, 1, 3);
        UNIT_ASSERT(list.GetRole(0) == EHostRole::HandOff);
        UNIT_ASSERT(list.GetRole(1) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(2) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(3) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(4) == EHostRole::HandOff);
    }

    Y_UNIT_TEST(ShouldMakeRotatingForVChunk4)
    {
        const auto list = THostRoles::MakeRotating(5, 4, 3);
        // Primary slots: (0+4)%5=4, (1+4)%5=0, (2+4)%5=1.
        UNIT_ASSERT(list.GetRole(0) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(1) == EHostRole::Primary);
        UNIT_ASSERT(list.GetRole(2) == EHostRole::HandOff);
        UNIT_ASSERT(list.GetRole(3) == EHostRole::HandOff);
        UNIT_ASSERT(list.GetRole(4) == EHostRole::Primary);
    }

    Y_UNIT_TEST(ShouldReflectSetInMasks)
    {
        auto list = THostRoles::MakeRotating(5, 0, 3);
        list.SetRole(1, EHostRole::None);

        UNIT_ASSERT_VALUES_EQUAL(2u, list.GetPrimary().Count());
        UNIT_ASSERT(list.GetPrimary().Get(0));
        UNIT_ASSERT(!list.GetPrimary().Get(1));
        UNIT_ASSERT(list.GetPrimary().Get(2));
        UNIT_ASSERT_VALUES_EQUAL(4u, list.GetActive().Count());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
