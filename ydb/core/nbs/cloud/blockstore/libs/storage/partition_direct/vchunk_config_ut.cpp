#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(TVChunkConfigTest)
{
    Y_UNIT_TEST(Make_DefaultLayout)
    {
        const auto cfg = TVChunkConfig::Make(0, 5, 3);
        UNIT_ASSERT_VALUES_EQUAL(0u, cfg.VChunkIndex);
        UNIT_ASSERT(cfg.PBufferHosts == cfg.DDiskHosts);
        UNIT_ASSERT_VALUES_EQUAL(3u, cfg.PBufferHosts.Primary().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, cfg.PBufferHosts.HandOff().Count());
        UNIT_ASSERT(cfg.PBufferHosts.Disabled().Empty());
        UNIT_ASSERT(cfg.IsValid());
    }

    Y_UNIT_TEST(Make_NonZeroVChunkIndex)
    {
        const auto cfg = TVChunkConfig::Make(2, 5, 3);
        // Primary slots: (0+2)%5=2, (1+2)%5=3, (2+2)%5=4.
        UNIT_ASSERT(cfg.PBufferHosts.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.Get(3) == EHostStatus::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.Get(4) == EHostStatus::Primary);
        UNIT_ASSERT(cfg.PBufferHosts.Get(0) == EHostStatus::HandOff);
        UNIT_ASSERT(cfg.PBufferHosts.Get(1) == EHostStatus::HandOff);
    }

    Y_UNIT_TEST(IsValid_AllDisabledOnOneSideIsInvalid)
    {
        auto cfg = TVChunkConfig::Make(0, 5, 3);
        for (size_t i = 0; i < 5; ++i) {
            cfg.PBufferHosts.Set(i, EHostStatus::Disabled);
        }
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(IsValid_HostCountMismatchIsInvalid)
    {
        TVChunkConfig cfg{
            .VChunkIndex = 0,
            .PBufferHosts = THostStatusList::MakeRotating(5, 0, 3),
            .DDiskHosts = THostStatusList::MakeRotating(4, 0, 3),
        };
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(IsValid_EmptyHostListIsInvalid)
    {
        TVChunkConfig cfg{};
        UNIT_ASSERT(!cfg.IsValid());
    }

    Y_UNIT_TEST(MakeVChunkConfigSnapshot_ReflectsDirtyMapState)
    {
        TBlocksDirtyMap dirtyMap{4096, 1024};
        auto pb = THostStatusList::MakeRotating(5, 7, 3);
        auto dd = THostStatusList::MakeRotating(5, 7, 3);
        dd.Set(0, EHostStatus::Disabled);
        dirtyMap.UpdateHostStatuses(pb, dd);

        const auto snapshot = dirtyMap.MakeVChunkConfigSnapshot(7);
        UNIT_ASSERT_VALUES_EQUAL(7u, snapshot.VChunkIndex);
        UNIT_ASSERT(snapshot.PBufferHosts == pb);
        UNIT_ASSERT(snapshot.DDiskHosts == dd);
        UNIT_ASSERT(snapshot.IsValid());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
