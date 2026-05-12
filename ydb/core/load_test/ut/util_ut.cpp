#include <ydb/core/load_test/util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <vector>

namespace NKikimr {
namespace {

std::vector<ui64> SampleDistribution(const TWeightedIndices& indices, ui32 entriesCount, ui64 samples) {
    std::vector<ui64> picksByIndex(entriesCount, 0);
    for (ui64 i = 0; i < samples; ++i) {
        const ui32 index = indices.GetRandomIndex();
        UNIT_ASSERT_C(index < picksByIndex.size(), "Weighted index out of bounds");
        ++picksByIndex[index];
    }
    return picksByIndex;
}

void AssertCountsApproximate(const std::vector<ui64>& picksByIndex, const std::vector<ui64>& weights, ui64 samples) {
    const ui64 totalWeight = std::accumulate(weights.begin(), weights.end(), ui64{0});
    UNIT_ASSERT_C(totalWeight > 0, "Total weight must be non-zero");
    UNIT_ASSERT_VALUES_EQUAL(picksByIndex.size(), weights.size());
    for (size_t i = 0; i < weights.size(); ++i) {
        const double p = static_cast<double>(weights[i]) / static_cast<double>(totalWeight);
        const double expected = static_cast<double>(samples) * p;
        const double stdDev = std::sqrt(static_cast<double>(samples) * p * (1.0 - p));
        const double tolerance = std::max(25.0, 8.0 * stdDev);
        UNIT_ASSERT_C(
            std::abs(static_cast<double>(picksByIndex[i]) - expected) <= tolerance,
            "Distribution mismatch at index " << i
            << ", expected~ " << expected << " tolerance " << tolerance
            << ", got " << picksByIndex[i]);
    }
}

void AssertApproximateDistribution(const std::vector<ui64>& weights, ui64 samples = 250000) {
    TWeightedIndices indices;
    for (ui64 weight : weights) {
        indices.AddWeight(weight);
    }
    const std::vector<ui64> picksByIndex = SampleDistribution(indices, indices.Size(), samples);
    AssertCountsApproximate(picksByIndex, weights, samples);
}

} // namespace

Y_UNIT_TEST_SUITE(WeightedIndicesTest) {
    Y_UNIT_TEST(SingleWeightAlwaysPicksOnlyIndex) {
        TWeightedIndices indices;
        const ui32 index = indices.AddWeight(17);
        UNIT_ASSERT_VALUES_EQUAL(index, 0u);

        const TWeightedIndices& constIndices = indices;
        for (ui64 i = 0; i < 5000; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(constIndices.GetRandomIndex(), 0u);
        }
    }

    Y_UNIT_TEST(SmallDistributionsAreWeighted) {
        AssertApproximateDistribution(std::vector<ui64>{1, 1});
        AssertApproximateDistribution(std::vector<ui64>{1, 2, 1});
        AssertApproximateDistribution(std::vector<ui64>{2, 1, 3, 4});
    }

    Y_UNIT_TEST(UnsortedDistributionsAreWeighted) {
        AssertApproximateDistribution(std::vector<ui64>{1, 100, 2, 50, 3, 77});
    }

    Y_UNIT_TEST(RandomApiReturnsInRange) {
        TWeightedIndices indices;
        indices.AddWeight(3);
        indices.AddWeight(1);
        indices.AddWeight(2);

        for (size_t i = 0; i < 1000; ++i) {
            const ui32 index = indices.GetRandomIndex();
            UNIT_ASSERT_C(index < indices.Size(), "Random API returned out-of-range index");
        }
    }

    Y_UNIT_TEST(DuplicatedWeightsFollowDistribution) {
        AssertApproximateDistribution(std::vector<ui64>{5, 5, 5, 5});
        AssertApproximateDistribution(std::vector<ui64>{3, 7, 7, 1, 7, 3, 3});
    }

    Y_UNIT_TEST(EqualWeightsAreUniform) {
        AssertApproximateDistribution(std::vector<ui64>{4, 4, 4});
    }

    Y_UNIT_TEST(LargeWeightsDoNotBreakSelection) {
        TWeightedIndices indices;
        const ui64 big = std::numeric_limits<ui64>::max() / 4;
        indices.AddWeight(big); // index 0
        indices.AddWeight(big); // index 1
        indices.AddWeight(big); // index 2
        indices.AddWeight(big); // index 3

        const auto picks = SampleDistribution(indices, indices.Size(), 300000);
        UNIT_ASSERT_VALUES_EQUAL(picks.size(), 4u);
        for (ui64 count : picks) {
            UNIT_ASSERT_C(count > 0, "Large weight bucket was never selected");
        }
    }

    Y_UNIT_TEST(RebuildsLazilyAfterAppendingWeights) {
        TWeightedIndices indices;
        indices.AddWeight(2); // index 0
        indices.AddWeight(1); // index 1
        {
            const auto before = SampleDistribution(indices, indices.Size(), 80000);
            UNIT_ASSERT_C(before[0] > before[1], "Expected index 0 to be selected more often before append");
        }

        const ui32 newIndex = indices.AddWeight(3); // index 2
        UNIT_ASSERT_VALUES_EQUAL(newIndex, 2u);

        const auto after = SampleDistribution(indices, indices.Size(), 150000);
        UNIT_ASSERT_C(after[2] > after[0], "Expected new heavier index to dominate after append");
        UNIT_ASSERT_C(after[2] > after[1], "Expected new heavier index to dominate after append");
    }

    Y_UNIT_TEST(ClearResetsPreparedStateAndIndices) {
        TWeightedIndices indices;
        indices.AddWeight(5);
        for (ui64 i = 0; i < 1000; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(indices.GetRandomIndex(), 0u);
        }

        indices.Clear();
        UNIT_ASSERT(indices.Empty());
        UNIT_ASSERT_VALUES_EQUAL(indices.Size(), 0u);

        const ui32 first = indices.AddWeight(1);
        const ui32 second = indices.AddWeight(2);
        UNIT_ASSERT_VALUES_EQUAL(first, 0u);
        UNIT_ASSERT_VALUES_EQUAL(second, 1u);
        const auto picks = SampleDistribution(indices, indices.Size(), 120000);
        AssertCountsApproximate(picks, std::vector<ui64>{1, 2}, 120000);
    }
}

} // namespace NKikimr
