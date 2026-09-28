#include "ddisk_balance.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDDiskBalanceTest)
{
    Y_UNIT_TEST(ShouldCalculateImbalanceOnlyForAllowedHosts)
    {
        const std::array<size_t, MaxHostCount> ddiskCountByHost =
            {5, 5, 5, 0, 20};

        const auto allHosts =
            CalculateDDiskImbalance(ddiskCountByHost, THostMask::MakeAll(5));
        UNIT_ASSERT_VALUES_EQUAL(13, allHosts.Moves);
        UNIT_ASSERT_VALUES_EQUAL(35, allHosts.TotalDDiskCount);
        UNIT_ASSERT_VALUES_EQUAL(37, allHosts.Percent);

        const auto allowedHosts = CalculateDDiskImbalance(
            ddiskCountByHost,
            THostMask::MakeMask({0, 1, 2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(3, allowedHosts.Moves);
        UNIT_ASSERT_VALUES_EQUAL(15, allowedHosts.TotalDDiskCount);
        UNIT_ASSERT_VALUES_EQUAL(20, allowedHosts.Percent);

        const auto noHosts =
            CalculateDDiskImbalance(ddiskCountByHost, THostMask::MakeEmpty());
        UNIT_ASSERT_VALUES_EQUAL(0, noHosts.Moves);
        UNIT_ASSERT_VALUES_EQUAL(0, noHosts.TotalDDiskCount);
        UNIT_ASSERT_VALUES_EQUAL(0, noHosts.Percent);
    }

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

    Y_UNIT_TEST(ShouldPreferVChunkWithFewerEnabledDDisks)
    {
        auto fourDDisks = TVChunkConfig::MakeDefault(0, 5, 3);
        fourDDisks.PromoteHost(3);
        const auto threeDDisks = TVChunkConfig::MakeDefault(5, 5, 3);
        std::array<size_t, MaxHostCount> ddiskCountByHost{};
        for (THostIndex host: fourDDisks.GetEnabledDDisks()) {
            ++ddiskCountByHost[host];
        }
        for (THostIndex host: threeDDisks.GetEnabledDDisks()) {
            ++ddiskCountByHost[host];
        }

        const auto allowedForBalancing = THostMask::MakeAll(5);
        const TVector<const TVChunkConfig*> vChunks = {
            &fourDDisks,
            &threeDDisks};
        const auto requests =
            PlanDDiskBalance(vChunks, allowedForBalancing, ddiskCountByHost);

        UNIT_ASSERT_VALUES_EQUAL(1u, requests.size());
        UNIT_ASSERT_VALUES_EQUAL(5u, requests[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(2u, requests[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, requests[0].TargetHost);

        const TVector<const TVChunkConfig*> onlyFourDDisks = {&fourDDisks};
        const auto fallback = PlanDDiskBalance(
            onlyFourDDisks,
            allowedForBalancing,
            ddiskCountByHost);
        UNIT_ASSERT_VALUES_EQUAL(1u, fallback.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, fallback[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(2u, fallback[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, fallback[0].TargetHost);

        fourDDisks.DisableHost(3);
        --ddiskCountByHost[3];
        UNIT_ASSERT_VALUES_EQUAL(4u, fourDDisks.GetDDisks().Count());
        UNIT_ASSERT_VALUES_EQUAL(3u, fourDDisks.GetEnabledDDisks().Count());

        const auto withDisabledDDisk =
            PlanDDiskBalance(vChunks, allowedForBalancing, ddiskCountByHost);
        UNIT_ASSERT_VALUES_EQUAL(2u, withDisabledDDisk.size());
        UNIT_ASSERT_VALUES_EQUAL(0u, withDisabledDDisk[0].VChunkId);
        UNIT_ASSERT_VALUES_EQUAL(1u, withDisabledDDisk[0].SourceHost);
        UNIT_ASSERT_VALUES_EQUAL(4u, withDisabledDDisk[0].TargetHost);
    }

    Y_UNIT_TEST(ShouldSkipVChunksBelowQuorum)
    {
        auto first = TVChunkConfig::MakeDefault(0, 5, 3);
        first.DisableHost(2);
        auto second = TVChunkConfig::MakeDefault(5, 5, 3);
        second.DisableHost(2);
        const TVector<const TVChunkConfig*> vChunks = {&first, &second};
        const std::array<size_t, MaxHostCount> ddiskCountByHost =
            {2, 2, 0, 0, 0};

        const auto requests =
            PlanDDiskBalance(vChunks, THostMask::MakeAll(5), ddiskCountByHost);

        UNIT_ASSERT(requests.empty());
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
