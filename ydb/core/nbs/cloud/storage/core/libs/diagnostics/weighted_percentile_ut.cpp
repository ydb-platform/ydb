#include "weighted_percentile.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AssertEqual(
    const TVector<double>& l,
    const TVector<double>& r,
    double epsilon)
{
    UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        UNIT_ASSERT_C(
            (fabs(l[i] - r[i]) <= epsilon),
            TStringBuilder() << "Wrong value l:" << l[i] << " r:" << r[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWeightedPercentileTest)
{
    Y_UNIT_TEST(ShouldCalculateWeightedPercentiles)
    {
        {
            TVector<TBucketInfo> buckets{
                {100, 10},
                {200, 40},
                {300, 20},
                {400, 30},
                {500, 0}};
            TVector<TPercentileDesc> percentiles{
                {0.2, "20"},
                {0.4, "40"},
                {0.5, "50"},
                {0.7, "70"},
                {0.85, "85"},
                {1, "100"}};

            auto result = CalculateWeightedPercentiles(buckets, percentiles);
            AssertEqual(result, {125, 175, 200, 300, 350, 400}, 0.01);
        }

        {
            TVector<TBucketInfo> buckets{
                {1, 10},
                {2, 40},
                {3, 20},
                {4, 30},
                {5, 0}};
            TVector<TPercentileDesc> percentiles{
                {0, "0"},
                {0.5, "50"},
                {0.6, "60"},
                {0.7, "70"},
                {0.8, "80"},
                {0.9, "90"},
                {0.99, "99"}};

            auto result = CalculateWeightedPercentiles(buckets, percentiles);
            AssertEqual(result, {0, 2, 2.5, 3, 3.33, 3.66, 3.97}, 0.01);
        }

        {
            const double max = std::numeric_limits<double>::max();
            TVector<TBucketInfo> buckets{
                {0.001, 1},
                {0.01, 2},
                {0.1, 3},
                {1, 4},
                {10, 5},
                {100, 6},
                {max, 7}};
            TVector<TPercentileDesc> percentiles{
                {0, "0"},
                {0.5, "50"},
                {1, "100"}};

            auto result = CalculateWeightedPercentiles(buckets, percentiles);
            AssertEqual(result, {0, 8.2, 100}, 0.01);
        }

        {
            const double inf = std::numeric_limits<double>::infinity();
            TVector<TBucketInfo> buckets{{1, 1}, {5, 2}, {10, 3}, {inf, 4}};
            TVector<TPercentileDesc> percentiles{
                {0.9, "90"},
                {0.99, "99"},
                {1, "100"}};

            auto result = CalculateWeightedPercentiles(buckets, percentiles);
            AssertEqual(result, {10, 10, 10}, 0.01);
        }
    }
}

}   // namespace NYdb::NBS
