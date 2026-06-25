#include "vchunk_config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(TVChunkConfigTest)
{
    Y_UNIT_TEST(ShouldMakeDefaultLayout)
    {
        const auto cfg = TVChunkConfig::MakeDefault(0, 5, 3);
        UNIT_ASSERT_VALUES_EQUAL(0u, cfg.GetVChunkIndex());
        UNIT_ASSERT(cfg.GetDesiredPBuffers() == cfg.GetDDisks());
        UNIT_ASSERT_VALUES_EQUAL(3u, cfg.GetDesiredPBuffers().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, cfg.GetSecondaryPBuffers().Count());
        UNIT_ASSERT(cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldMakeForNonZeroVChunkIndex)
    {
        const auto cfg = TVChunkConfig::MakeDefault(2, 5, 3);
        // Primary slots: (0+2)%5=2, (1+2)%5=3, (2+2)%5=4.
        UNIT_ASSERT_VALUES_EQUAL(
            "[H2,H3,H4]",
            cfg.GetDesiredPBuffers().Print());
    }

    Y_UNIT_TEST(ShouldBeInvalidWhenAllDisabledOnOneSide)
    {
        auto cfg =
            TVChunkConfig::Make(0, THostRoles(), THostRoles(), THostMask(), {});
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldBeInvalidOnHostCountMismatch)
    {
        auto cfg = TVChunkConfig::Make(
            0,
            THostRoles::MakeRotating(5, 0, 3, EHostRole::HandOff),
            THostRoles::MakeRotating(4, 0, 3, EHostRole::None),
            THostMask::MakeAll(5),
            {});
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldBeInvalidOnEmptyHostList)
    {
        auto cfg =
            TVChunkConfig::Make(0, THostRoles(), THostRoles(), THostMask(), {});
        UNIT_ASSERT(!cfg.IsValid());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
