#pragma once

#include <utility>

namespace NKiwiAggr {
    using TWeightedValue = std::pair<double, double>; // value, weight
    using TQualityFunction = TWeightedValue (*)(const TWeightedValue&, const TWeightedValue&);

    TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right);
    TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right);
    TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right);
}
