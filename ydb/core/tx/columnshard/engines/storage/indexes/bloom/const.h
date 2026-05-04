#pragma once

#include <cmath>
#include <optional>

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_defaults.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NIndexes {

struct TRequestSettings {
    std::optional<double> FalsePositiveProbability;

    template <class TBloomFilterProto>
    static TRequestSettings FromProtoFilter(const TBloomFilterProto& bFilter) {
        TRequestSettings result;
        result.FalsePositiveProbability = bFilter.HasFalsePositiveProbability()
            ? std::optional<double>(bFilter.GetFalsePositiveProbability())
            : std::nullopt;
        return result;
    }

    template <class TBloomFilterProto>
    void SerializeToProtoFilter(TBloomFilterProto& bFilter) const {
        if (FalsePositiveProbability) {
            bFilter.SetFalsePositiveProbability(*FalsePositiveProbability);
        }
    }

    double ResolvedFalsePositiveProbability() const {
        return FalsePositiveProbability.value_or(NDefaults::FalsePositiveProbability);
    }

    ui32 ResolvedHashesCount() const {
        return std::max<ui32>(1, static_cast<ui32>(-std::log(ResolvedFalsePositiveProbability()) / std::log(2.0)));
    }
};

inline TConclusionStatus ValidateRequest(const TRequestSettings& request) {
    const double fpp = request.ResolvedFalsePositiveProbability();
    if (fpp <= 0 || fpp >= 1) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field in interval (0, 1)");
    }

    return TConclusionStatus::Success();
}

} // namespace NKikimr::NOlap::NIndexes
