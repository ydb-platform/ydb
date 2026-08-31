#include "weighted_percentile.h"

#include <util/generic/algorithm.h>
#include <util/system/yassert.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

const TVector<TPercentileDesc>& GetDefaultPercentiles()
{
    static const TVector<TPercentileDesc> DefaultPercentiles = {
        {{000.5, "50"},
         {000.9, "90"},
         {00.99, "99"},
         {0.999, "99.9"},
         {1.000, "100"}}};

    return DefaultPercentiles;
}

const TVector<TString>& GetDefaultPercentileNames()
{
    static const TVector<TString> DefaultPercentiles = {
        {"50", "90", "99", "99.9", "100"}};

    return DefaultPercentiles;
}

////////////////////////////////////////////////////////////////////////////////

TVector<double> CalculateWeightedPercentiles(
    const TVector<TBucketInfo>& buckets,
    const TVector<TPercentileDesc>& percentiles)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(
        buckets.begin(),
        buckets.end(),
        [](const auto& l, const auto& r) { return l.first < r.first; }));

    TVector<double> result(Reserve(percentiles.size()));

    ui64 prevSum = 0;
    double prevLimit = 0;
    ui64 totalSum = Accumulate(
        buckets.begin(),
        buckets.end(),
        (ui64)0,
        [](const auto& l, const auto& r) { return l + r.second; });

    const double greatestFiniteLimit = [&]
    {
        for (size_t i = buckets.size(); i; --i) {
            if (buckets[i - 1].first < std::numeric_limits<i64>::max()) {
                return buckets[i - 1].first;
            }
        }
        return 0.0;
    }();

    auto pit = percentiles.begin();
    auto bit = buckets.begin();

    while (bit != buckets.end() && pit != percentiles.end()) {
        double current = 0;
        if (totalSum) {
            current = (double)(prevSum + bit->second) / totalSum;
        }
        if (current >= pit->first) {
            const double delta = pit->first * totalSum - prevSum;
            const double part = (bit->first - prevLimit) *
                                (delta / (bit->second ? bit->second : 1));
            result.push_back(std::min(prevLimit + part, greatestFiniteLimit));
            ++pit;
        } else {
            prevSum += bit->second;
            prevLimit = bit->first;
            ++bit;
        }
    }

    if (pit != percentiles.end()) {
        auto last = result.size() ? result.back() : 0;
        for (; pit != percentiles.end(); ++pit) {
            result.push_back(last);
        }
    }

    return result;
}

}   // namespace NYdb::NBS
