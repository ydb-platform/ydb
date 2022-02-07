#include "common.h"

namespace NKiwiAggr {
    TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.first - left.first, left.first);
    }

    TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.second + left.second, left.first);
    }

    TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right) {
        const double N1 = left.second;
        const double N2 = right.second;
        const double mu1 = left.first;
        const double mu2 = right.first;
        return TWeightedValue(N1 * N2 / (N1 + N2) * (mu1 - mu2) * (mu1 - mu2), left.first);
    }
}
