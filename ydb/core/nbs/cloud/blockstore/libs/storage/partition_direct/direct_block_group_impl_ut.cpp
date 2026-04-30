#include "direct_block_group_impl.h"

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <map>
#include <vector>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectBlockGroupTest)
{
    Y_UNIT_TEST(SelectBestPBufferHostShouldPickHostWithLowestInflight)
    {
        const TVector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};
        // Host 2 has the lowest inflight count (zero), all others are higher.
        const std::array<size_t, 5> inflight = {3, 5, 0, 7, 2};

        // Run multiple times to ensure deterministic selection (no ties at
        // the minimum).
        for (size_t iter = 0; iter < 100; ++iter) {
            const THostIndex selected =
                TDirectBlockGroup::SelectBestPBufferHost(
                    hostIndexes,
                    [&](THostIndex hostIndex) { return inflight[hostIndex]; });

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldHandleSingleHost)
    {
        const TVector<THostIndex> hostIndexes = {4};

        const THostIndex selected = TDirectBlockGroup::SelectBestPBufferHost(
            hostIndexes,
            [](THostIndex) { return 999u; });

        UNIT_ASSERT_VALUES_EQUAL(4u, selected);
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldRespectSubsetOfHosts)
    {
        // Even if some non-listed host has a lower inflight count, the
        // selection must be limited to the supplied hostIndexes.
        const TVector<THostIndex> hostIndexes = {1, 3};
        const std::array<size_t, 5> inflight = {0, 5, 0, 2, 0};

        for (size_t iter = 0; iter < 100; ++iter) {
            const THostIndex selected =
                TDirectBlockGroup::SelectBestPBufferHost(
                    hostIndexes,
                    [&](THostIndex hostIndex) { return inflight[hostIndex]; });

            // Out of {1, 3}, host 3 has the lower inflight count.
            UNIT_ASSERT_VALUES_EQUAL(3u, selected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldDistributeTiesAcrossAllCandidates)
    {
        // All three hosts have the same (zero) inflight count. With reservoir
        // sampling, every tied host must have a roughly equal probability of
        // being selected. We verify this by sampling a large number of times
        // and checking that every candidate appears at least once.
        const TVector<THostIndex> hostIndexes = {1, 2, 4};

        std::map<THostIndex, size_t> counts;
        const size_t iterations = 3000;
        for (size_t iter = 0; iter < iterations; ++iter) {
            const THostIndex selected =
                TDirectBlockGroup::SelectBestPBufferHost(
                    hostIndexes,
                    [](THostIndex) { return 0u; });
            ++counts[selected];
        }

        UNIT_ASSERT_VALUES_EQUAL(3u, counts.size());

        // Each tied host must be picked a meaningful share of the time.
        // Expected ~1/3 of iterations each, allow generous tolerance.
        const size_t expected = iterations / 3;
        const size_t tolerance = expected / 2;   // 50% tolerance
        for (auto hostIndex: hostIndexes) {
            const size_t count = counts[hostIndex];
            UNIT_ASSERT_C(
                count + tolerance >= expected && count <= expected + tolerance,
                TStringBuilder()
                    << "host " << static_cast<ui32>(hostIndex) << " was picked "
                    << count << " times, expected ~" << expected);
        }
    }

    Y_UNIT_TEST(SelectBestPBufferHostShouldIgnoreTiesAboveMinimum)
    {
        // Hosts with inflight equal to the (non-best) tie value must never be
        // picked - only ties at the global minimum are randomized.
        const TVector<THostIndex> hostIndexes = {0, 1, 2, 3, 4};
        const std::array<size_t, 5> inflight = {5, 5, 1, 5, 5};

        for (size_t iter = 0; iter < 200; ++iter) {
            const THostIndex selected =
                TDirectBlockGroup::SelectBestPBufferHost(
                    hostIndexes,
                    [&](THostIndex hostIndex) { return inflight[hostIndex]; });

            UNIT_ASSERT_VALUES_EQUAL(2u, selected);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
