#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostStatusTest)
{
    Y_UNIT_TEST(HostMask_SetTestResetCount)
    {
        THostMask m;
        UNIT_ASSERT(m.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, m.Count());

        m.Set(0);
        m.Set(3);
        m.Set(31);

        UNIT_ASSERT(m.Test(0));
        UNIT_ASSERT(!m.Test(1));
        UNIT_ASSERT(m.Test(3));
        UNIT_ASSERT(m.Test(31));
        UNIT_ASSERT_VALUES_EQUAL(3u, m.Count());

        m.Reset(3);
        UNIT_ASSERT(!m.Test(3));
        UNIT_ASSERT_VALUES_EQUAL(2u, m.Count());
    }

    Y_UNIT_TEST(HostMask_MakeAll)
    {
        UNIT_ASSERT(THostMask::MakeAll(0).Empty());
        UNIT_ASSERT_VALUES_EQUAL(5u, THostMask::MakeAll(5).Count());
        UNIT_ASSERT_VALUES_EQUAL(32u, THostMask::MakeAll(32).Count());
    }

    Y_UNIT_TEST(HostMask_MakeOne)
    {
        auto m = THostMask::MakeOne(7);
        UNIT_ASSERT_VALUES_EQUAL(1u, m.Count());
        UNIT_ASSERT(m.Test(7));
    }

    Y_UNIT_TEST(HostMask_LogicalOps)
    {
        auto a = THostMask::MakeOne(0).Include(THostMask::MakeOne(1));
        auto b = THostMask::MakeOne(1).Include(THostMask::MakeOne(2));

        UNIT_ASSERT_VALUES_EQUAL(1u, a.LogicalAnd(b).Count());
        UNIT_ASSERT(a.LogicalAnd(b).Test(1));

        UNIT_ASSERT_VALUES_EQUAL(3u, a.Include(b).Count());
        UNIT_ASSERT_VALUES_EQUAL(1u, a.Exclude(b).Count());
        UNIT_ASSERT(a.Exclude(b).Test(0));

        UNIT_ASSERT(a.Contains(THostMask::MakeOne(0)));
        UNIT_ASSERT(!a.Contains(THostMask::MakeOne(2)));
    }

    Y_UNIT_TEST(HostMask_IterationAscending)
    {
        auto m = THostMask::MakeOne(5).Include(
            THostMask::MakeOne(0).Include(THostMask::MakeOne(2)));

        TVector<THostIndex> seen;
        for (auto h: m) {
            seen.push_back(h);
        }
        UNIT_ASSERT_VALUES_EQUAL(3u, seen.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, seen[0]);
        UNIT_ASSERT_VALUES_EQUAL(2u, seen[1]);
        UNIT_ASSERT_VALUES_EQUAL(5u, seen[2]);
    }

    Y_UNIT_TEST(HostMask_First)
    {
        UNIT_ASSERT(!THostMask::MakeEmpty().First().has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            3u,
            *THostMask::MakeOne(7).Include(THostMask::MakeOne(3)).First());
    }

    Y_UNIT_TEST(StatusList_MakeRotating_VChunk0)
    {
        const auto list = THostStatusList::MakeRotating(5, 0, 3);
        UNIT_ASSERT_VALUES_EQUAL(5u, list.HostCount());
        UNIT_ASSERT(list.Get(0) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(3) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(4) == EHostStatus::HandOff);
        UNIT_ASSERT_VALUES_EQUAL(3u, list.Primary().Count());
        UNIT_ASSERT_VALUES_EQUAL(2u, list.HandOff().Count());
        UNIT_ASSERT(list.Disabled().Empty());
    }

    Y_UNIT_TEST(StatusList_MakeRotating_VChunk1)
    {
        const auto list = THostStatusList::MakeRotating(5, 1, 3);
        UNIT_ASSERT(list.Get(0) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(3) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(4) == EHostStatus::HandOff);
    }

    Y_UNIT_TEST(StatusList_MakeRotating_VChunk4)
    {
        const auto list = THostStatusList::MakeRotating(5, 4, 3);
        // Primary slots: (0+4)%5=4, (1+4)%5=0, (2+4)%5=1.
        UNIT_ASSERT(list.Get(0) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(1) == EHostStatus::Primary);
        UNIT_ASSERT(list.Get(2) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(3) == EHostStatus::HandOff);
        UNIT_ASSERT(list.Get(4) == EHostStatus::Primary);
    }

    Y_UNIT_TEST(StatusList_SetReflectsInMasks)
    {
        auto list = THostStatusList::MakeRotating(5, 0, 3);
        list.Set(1, EHostStatus::Disabled);

        UNIT_ASSERT_VALUES_EQUAL(2u, list.Primary().Count());
        UNIT_ASSERT(list.Primary().Test(0));
        UNIT_ASSERT(!list.Primary().Test(1));
        UNIT_ASSERT(list.Primary().Test(2));
        UNIT_ASSERT_VALUES_EQUAL(1u, list.Disabled().Count());
        UNIT_ASSERT(list.Disabled().Test(1));
        UNIT_ASSERT_VALUES_EQUAL(4u, list.Active().Count());
    }

    Y_UNIT_TEST(HostRoute_OperatorLessLexicographic)
    {
        THostRoute a{.SourceHostIndex = 0, .DestinationHostIndex = 1};
        THostRoute b{.SourceHostIndex = 0, .DestinationHostIndex = 2};
        THostRoute c{.SourceHostIndex = 1, .DestinationHostIndex = 0};

        UNIT_ASSERT(a < b);
        UNIT_ASSERT(b < c);
        UNIT_ASSERT(a < c);
        UNIT_ASSERT(!(b < a));
    }

    Y_UNIT_TEST(HostRoute_OrderInTMap)
    {
        TMap<THostRoute, int> m;
        m[THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 0}] = 1;
        m[THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0}] = 2;
        m[THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 5}] = 3;

        TVector<THostRoute> keys;
        for (const auto& [k, v]: m) {
            keys.push_back(k);
            Y_UNUSED(v);
        }
        UNIT_ASSERT_VALUES_EQUAL(3u, keys.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, keys[0].SourceHostIndex);
        UNIT_ASSERT_VALUES_EQUAL(0u, keys[0].DestinationHostIndex);
        UNIT_ASSERT_VALUES_EQUAL(0u, keys[1].SourceHostIndex);
        UNIT_ASSERT_VALUES_EQUAL(5u, keys[1].DestinationHostIndex);
        UNIT_ASSERT_VALUES_EQUAL(1u, keys[2].SourceHostIndex);
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
