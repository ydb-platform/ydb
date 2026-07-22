#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

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

    Y_UNIT_TEST(ShouldAddHandOffToDesiredWhenPrimaryDisabled)
    {
        THostMask hostMask = THostMask::MakeAll(5);
        hostMask.Reset(0);

        auto cfg = TVChunkConfig::MakeDefault(0, 5, 3);

        UNIT_ASSERT_VALUES_EQUAL(
            "[H0,H1,H2]",
            cfg.GetDesiredPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H3,H4]", cfg.GetSecondaryPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", cfg.GetHealthyDDisks().Print());

        cfg.DisableHost(0);
        UNIT_ASSERT(cfg.IsValid());

        UNIT_ASSERT_VALUES_EQUAL(
            "[H1,H2,H3]",
            cfg.GetDesiredPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H3,H4]", cfg.GetSecondaryPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H1,H2]", cfg.GetHealthyDDisks().Print());

        cfg.EnableHost(0);
        UNIT_ASSERT_VALUES_EQUAL(
            "[H0,H1,H2]",
            cfg.GetDesiredPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H3,H4]", cfg.GetSecondaryPBuffers().Print());
        UNIT_ASSERT_VALUES_EQUAL("[H0,H1,H2]", cfg.GetHealthyDDisks().Print());
    }

    Y_UNIT_TEST(ShouldAppendHandOffWhenDDisksEnoughForQuorum)
    {
        auto cfg = TVChunkConfig::MakeDefault(
            0,
            DirectBlockGroupHostCount - 1,
            QuorumDirectBlockGroupHostCount);
        const size_t before = cfg.GetHostCount();

        cfg.AppendHost();

        const auto newIdx = static_cast<THostIndex>(before);
        UNIT_ASSERT_VALUES_EQUAL(before + 1, cfg.GetHostCount());
        UNIT_ASSERT(cfg.GetPBufferRole(newIdx) == EHostRole::HandOff);
        UNIT_ASSERT(cfg.GetDDiskRole(newIdx) == EHostRole::None);
        UNIT_ASSERT(!cfg.GetDisabledHosts().Get(newIdx));
        UNIT_ASSERT(!cfg.GetWatermark(newIdx).has_value());
        UNIT_ASSERT(!cfg.GetDDisks().Get(newIdx));
    }

    Y_UNIT_TEST(ShouldAppendPrimaryWhenDDisksNotEnoughForQuorum)
    {
        auto cfg = TVChunkConfig::MakeDefault(
            0,
            DirectBlockGroupHostCount - 1,
            QuorumDirectBlockGroupHostCount - 1);
        const size_t before = cfg.GetHostCount();

        cfg.AppendHost();

        const auto newIdx = static_cast<THostIndex>(before);
        UNIT_ASSERT_VALUES_EQUAL(before + 1, cfg.GetHostCount());
        UNIT_ASSERT(cfg.GetPBufferRole(newIdx) == EHostRole::Primary);
        UNIT_ASSERT(cfg.GetDDiskRole(newIdx) == EHostRole::Primary);
        UNIT_ASSERT(!cfg.GetDisabledHosts().Get(newIdx));
        UNIT_ASSERT_VALUES_EQUAL(0, *cfg.GetWatermark(newIdx));
        UNIT_ASSERT(cfg.GetDDisks().Get(newIdx));
    }

    Y_UNIT_TEST(ShouldRemoveHost)
    {
        // 6 hosts, remove host 2: hosts 3..5 shift down to 2..4 keeping
        // their roles, enabled bits and watermarks.
        auto cfg = TVChunkConfig::MakeDefault(0, 6, 3);
        cfg.DisableHost(2);
        cfg.SetWatermark(4, 42);
        cfg.DisableHost(5);
        const auto role3PBuffer = cfg.GetPBufferRole(3);
        const auto role3DDisk = cfg.GetDDiskRole(3);

        cfg.RemoveHost(2);

        UNIT_ASSERT_VALUES_EQUAL(5u, cfg.GetHostCount());
        UNIT_ASSERT(cfg.GetPBufferRole(2) == role3PBuffer);
        UNIT_ASSERT(cfg.GetDDiskRole(2) == role3DDisk);
        UNIT_ASSERT(!cfg.GetDisabledHosts().Get(2));
        UNIT_ASSERT(cfg.GetWatermark(3).has_value());
        UNIT_ASSERT_VALUES_EQUAL(42u, *cfg.GetWatermark(3));
        UNIT_ASSERT(cfg.GetDisabledHosts().Get(4));
        // Hosts below the removed index keep their layout.
        UNIT_ASSERT(cfg.GetPBufferRole(0) == EHostRole::Primary);
        UNIT_ASSERT(cfg.GetDDiskRole(1) == EHostRole::Primary);
    }

    Y_UNIT_TEST(ShouldRemoveLastHost)
    {
        auto cfg = TVChunkConfig::MakeDefault(0, 6, 3);
        cfg.DisableHost(5);

        cfg.RemoveHost(5);

        UNIT_ASSERT_VALUES_EQUAL(5u, cfg.GetHostCount());
        UNIT_ASSERT(cfg.IsValid());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
