#include "vchunk_config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(TVChunkConfigTest)
{
    Y_UNIT_TEST(ShouldMakeDefaultLayout)
    {
        const auto cfg = TVChunkConfig::Make(0, 5, 3);
        UNIT_ASSERT_VALUES_EQUAL(0u, cfg.VChunkIndex);
        UNIT_ASSERT(cfg.PBufferHosts == cfg.DDiskHosts);
        UNIT_ASSERT_VALUES_EQUAL(3u, cfg.PBufferHosts.GetPrimary().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, cfg.PBufferHosts.GetHandOff().Count());
        UNIT_ASSERT(cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldMakeForNonZeroVChunkIndex)
    {
        const auto cfg = TVChunkConfig::Make(2, 5, 3);
        // Primary slots: (0+2)%5=2, (1+2)%5=3, (2+2)%5=4.
        UNIT_ASSERT(cfg.PBufferHosts.GetRole(2) == EHostRole::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.GetRole(3) == EHostRole::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.GetRole(4) == EHostRole::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.GetRole(0) == EHostRole::HandOff);
        UNIT_ASSERT(cfg.PBufferHosts.GetRole(1) == EHostRole::HandOff);
    }

    Y_UNIT_TEST(ShouldBeInvalidWhenAllDisabledOnOneSide)
    {
        auto cfg = TVChunkConfig::Make(0, 5, 3);
        for (size_t i = 0; i < 5; ++i) {
            cfg.PBufferHosts.SetRole(i, EHostRole::None);
        }
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldBeInvalidOnHostCountMismatch)
    {
        TVChunkConfig cfg{
            .VChunkIndex = 0,
            .PBufferHosts = THostRoles::MakeRotating(5, 0, 3),
            .DDiskHosts = THostRoles::MakeRotating(4, 0, 3),
        };
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(ShouldBeInvalidOnEmptyHostList)
    {
        TVChunkConfig cfg{};
        UNIT_ASSERT(!cfg.IsValid());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
