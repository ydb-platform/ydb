#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

Y_UNIT_TEST_SUITE(THostMaskTest)
{
    Y_UNIT_TEST(ShouldSetGetResetAndCount)
    {
        THostMask mask;
        UNIT_ASSERT(mask.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, mask.Count());

        mask.Set(0);
        mask.Set(3);
        mask.Set(31);

        UNIT_ASSERT(mask.Get(0));
        UNIT_ASSERT(!mask.Get(1));
        UNIT_ASSERT(mask.Get(3));
        UNIT_ASSERT(mask.Get(31));
        UNIT_ASSERT_VALUES_EQUAL(3u, mask.Count());

        mask.Reset(3);
        UNIT_ASSERT(!mask.Get(3));
        UNIT_ASSERT_VALUES_EQUAL(2u, mask.Count());
    }

    Y_UNIT_TEST(ShouldMakeAll)
    {
        UNIT_ASSERT(THostMask::MakeAll(0).Empty());
        UNIT_ASSERT_VALUES_EQUAL(5u, THostMask::MakeAll(5).Count());
        UNIT_ASSERT_VALUES_EQUAL(32u, THostMask::MakeAll(32).Count());
    }

    Y_UNIT_TEST(ShouldMakeOne)
    {
        auto mask = THostMask::MakeOne(7);
        UNIT_ASSERT_VALUES_EQUAL(1u, mask.Count());
        UNIT_ASSERT(mask.Get(7));
    }

    Y_UNIT_TEST(ShouldDoLogicalOps)
    {
        auto first = THostMask::MakeOne(0).Include(THostMask::MakeOne(1));
        auto second = THostMask::MakeOne(1).Include(THostMask::MakeOne(2));

        UNIT_ASSERT_VALUES_EQUAL(1u, first.LogicalAnd(second).Count());
        UNIT_ASSERT(first.LogicalAnd(second).Get(1));

        UNIT_ASSERT_VALUES_EQUAL(3u, first.Include(second).Count());
        UNIT_ASSERT_VALUES_EQUAL(1u, first.Exclude(second).Count());
        UNIT_ASSERT(first.Exclude(second).Get(0));
    }

    Y_UNIT_TEST(ShouldIterateAscending)
    {
        auto mask = THostMask::MakeOne(5).Include(
            THostMask::MakeOne(0).Include(THostMask::MakeOne(2)));

        TVector<THostIndex> seen;
        for (auto host: mask) {
            seen.push_back(host);
        }
        UNIT_ASSERT_VALUES_EQUAL(3u, seen.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, seen[0]);
        UNIT_ASSERT_VALUES_EQUAL(2u, seen[1]);
        UNIT_ASSERT_VALUES_EQUAL(5u, seen[2]);
    }

    Y_UNIT_TEST(ShouldReturnFirst)
    {
        UNIT_ASSERT(!THostMask::MakeEmpty().First().has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            3u,
            *THostMask::MakeOne(7).Include(THostMask::MakeOne(3)).First());
    }

    Y_UNIT_TEST(ShouldCompareHostRoutesLexicographically)
    {
        THostRoute first{.SourceHostIndex = 0, .DestinationHostIndex = 1};
        THostRoute second{.SourceHostIndex = 0, .DestinationHostIndex = 2};
        THostRoute third{.SourceHostIndex = 1, .DestinationHostIndex = 0};

        UNIT_ASSERT(first < second);
        UNIT_ASSERT(second < third);
        UNIT_ASSERT(first < third);
        UNIT_ASSERT(!(second < first));
    }

    Y_UNIT_TEST(ShouldOrderHostRoutesInTMap)
    {
        TMap<THostRoute, int> routes;
        routes[THostRoute{.SourceHostIndex = 1, .DestinationHostIndex = 0}] = 1;
        routes[THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 0}] = 2;
        routes[THostRoute{.SourceHostIndex = 0, .DestinationHostIndex = 5}] = 3;

        TVector<THostRoute> keys;
        for (const auto& [route, value]: routes) {
            keys.push_back(route);
            Y_UNUSED(value);
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
