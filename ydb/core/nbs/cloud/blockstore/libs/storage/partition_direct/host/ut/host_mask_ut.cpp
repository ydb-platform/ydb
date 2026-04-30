#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_mask.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostMaskTest)
{
    Y_UNIT_TEST(SetTestResetCount)
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

    Y_UNIT_TEST(MakeAll)
    {
        UNIT_ASSERT(THostMask::MakeAll(0).Empty());
        UNIT_ASSERT_VALUES_EQUAL(5u, THostMask::MakeAll(5).Count());
        UNIT_ASSERT_VALUES_EQUAL(32u, THostMask::MakeAll(32).Count());
    }

    Y_UNIT_TEST(MakeOne)
    {
        auto m = THostMask::MakeOne(7);
        UNIT_ASSERT_VALUES_EQUAL(1u, m.Count());
        UNIT_ASSERT(m.Test(7));
    }

    Y_UNIT_TEST(LogicalOps)
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

    Y_UNIT_TEST(IterationAscending)
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

    Y_UNIT_TEST(First)
    {
        UNIT_ASSERT(!THostMask::MakeEmpty().First().has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            3u,
            *THostMask::MakeOne(7).Include(THostMask::MakeOne(3)).First());
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
