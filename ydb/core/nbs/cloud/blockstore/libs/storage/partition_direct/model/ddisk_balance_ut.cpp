#include "ddisk_balance.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDDiskBalanceTest)
{
    Y_UNIT_TEST(ShouldUseDifferentTargetsAndRequestEachVChunkOnce)
    {
        const auto first = TVChunkConfig::MakeDefault(0, 5, 3);
        const auto second = TVChunkConfig::MakeDefault(5, 5, 3);
        const TVector<const TVChunkConfig*> vChunks = {&first, &second};
        const std::array<size_t, MaxHostCount> ddiskCountByHost =
            {2, 2, 2, 0, 0};

        const auto requests =
            PlanDDiskBalance(vChunks, THostMask::MakeAll(5), ddiskCountByHost);

        UNIT_ASSERT_VALUES_EQUAL(2u, requests.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, requests[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(1u, requests[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(3u, requests[0].TargetHost);
        UNIT_ASSERT_VALUES_EQUAL(5u, requests[1].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(2u, requests[1].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, requests[1].TargetHost);
    }

    Y_UNIT_TEST(ShouldAccountForDDisksOfExcludedVChunk)
    {
        const auto first = TVChunkConfig::MakeDefault(0, 5, 3);
        const auto second = TVChunkConfig::MakeDefault(5, 5, 3);
        // The first VChunk has a pending allocation and is excluded from
        // candidates, but its DDisks are still counted.
        const TVector<const TVChunkConfig*> vChunks = {&second};
        std::array<size_t, MaxHostCount> ddiskCountByHost{};
        for (THostIndex host: first.GetEnabledDDisks()) {
            ++ddiskCountByHost[host];
        }
        for (THostIndex host: second.GetEnabledDDisks()) {
            ++ddiskCountByHost[host];
        }
        // The pending allocation of the first VChunk is already on host 3.
        ++ddiskCountByHost[3];

        const auto requests =
            PlanDDiskBalance(vChunks, THostMask::MakeAll(5), ddiskCountByHost);

        UNIT_ASSERT_VALUES_EQUAL(1u, requests.size());
        UNIT_ASSERT_VALUES_EQUAL(5u, requests[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(2u, requests[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, requests[0].TargetHost);
    }

    Y_UNIT_TEST(ShouldRespectDisabledAndUnavailableHosts)
    {
        auto first = TVChunkConfig::MakeDefault(0, 5, 3);
        first.DisableHost(3);
        const auto second = TVChunkConfig::MakeDefault(5, 5, 3);
        const TVector<const TVChunkConfig*> vChunks = {&first, &second};
        const std::array<size_t, MaxHostCount> ddiskCountByHost =
            {2, 2, 2, 0, 0};

        const auto requests =
            PlanDDiskBalance(vChunks, THostMask::MakeAll(5), ddiskCountByHost);
        UNIT_ASSERT_VALUES_EQUAL(2u, requests.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, requests[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(1u, requests[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, requests[0].TargetHost);
        UNIT_ASSERT_VALUES_EQUAL(5u, requests[1].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(2u, requests[1].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(3u, requests[1].TargetHost);

        const auto unavailable = THostMask::MakeMask({0, 1, 2});
        UNIT_ASSERT(
            PlanDDiskBalance(vChunks, unavailable, ddiskCountByHost).empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
