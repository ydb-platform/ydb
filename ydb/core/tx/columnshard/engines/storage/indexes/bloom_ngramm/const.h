#pragma once

#include <algorithm>
#include <cmath>
#include <optional>

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_defaults.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

struct TRequestSettings {
    std::optional<ui32> NGrammSize;
    std::optional<double> FalsePositiveProbability;
    std::optional<bool> CaseSensitive;
    std::optional<ui32> DeprecatedHashesCount;
    std::optional<ui32> DeprecatedFilterSizeBytes;
    std::optional<ui32> DeprecatedRecordsCount;

    template <class TBloomFilterProto>
    static TRequestSettings FromProtoFilter(const TBloomFilterProto& bFilter) {
        TRequestSettings result;
        result.FalsePositiveProbability = bFilter.HasFalsePositiveProbability()
            ? std::optional<double>(bFilter.GetFalsePositiveProbability()) : std::nullopt;
        result.NGrammSize = bFilter.HasNGrammSize()
            ? std::optional<ui32>(bFilter.GetNGrammSize()) : std::nullopt;
        result.CaseSensitive = bFilter.HasCaseSensitive()
            ? std::optional<bool>(bFilter.GetCaseSensitive()) : std::nullopt;
        result.DeprecatedHashesCount = bFilter.HasHashesCount()
            ? std::optional<ui32>(bFilter.GetHashesCount()) : std::nullopt;
        result.DeprecatedFilterSizeBytes = bFilter.HasFilterSizeBytes()
            ? std::optional<ui32>(bFilter.GetFilterSizeBytes()) : std::nullopt;
        result.DeprecatedRecordsCount = bFilter.HasRecordsCount()
            ? std::optional<ui32>(bFilter.GetRecordsCount()) : std::nullopt;
        return result;
    }

    template <class TBloomFilterProto>
    void SerializeToProtoFilterRaw(TBloomFilterProto& bFilter) const {
        bFilter.SetCaseSensitive(ResolvedCaseSensitive());

        if (NGrammSize) {
            bFilter.SetNGrammSize(*NGrammSize);
        }

        const bool useOldSizing = IsOldSizingMode();
        if (useOldSizing) {
            if (DeprecatedHashesCount) {
                bFilter.SetHashesCount(*DeprecatedHashesCount);
            }

            if (DeprecatedFilterSizeBytes) {
                bFilter.SetFilterSizeBytes(*DeprecatedFilterSizeBytes);
            }

            if (DeprecatedRecordsCount) {
                bFilter.SetRecordsCount(*DeprecatedRecordsCount);
            }
        } else if (FalsePositiveProbability) {
            bFilter.SetFalsePositiveProbability(*FalsePositiveProbability);
        }
    }

    bool IsOldSizingMode() const;
    ui32 ResolvedNGrammSize() const;
    bool ResolvedCaseSensitive() const;
    double ResolvedFalsePositiveProbability() const;
    ui32 ResolvedHashesCount() const;
    ui32 ResolvedFilterSizeBytes() const;
    ui32 ResolvedRecordsCount() const;
};

class TConstants {
public:
    static constexpr ui32 MinNGrammSize = 3;
    static constexpr ui32 MaxNGrammSize = 8;
    static constexpr ui32 MinHashesCount = 1;
    static constexpr ui32 MaxHashesCount = 8;
    static constexpr ui32 DeprecatedRecordsCount = 10000;
    static constexpr ui32 MinFilterSizeBytes = 128;
    static constexpr ui32 MinFilterSizeBits = MinFilterSizeBytes * 8;
    static constexpr ui32 MaxFilterSizeBytes = 1 << 20;
    static constexpr ui32 MaxFilterSizeBits = MaxFilterSizeBytes * 8;
    static constexpr ui32 MinRecordsCount = 128;
    static constexpr ui32 MaxRecordsCount = 1000000;

    static bool CheckRecordsCount(const ui32 value) {
        return MinRecordsCount <= value && value <= MaxRecordsCount;
    }

    static bool CheckNGrammSize(const ui32 value) {
        return MinNGrammSize <= value && value <= MaxNGrammSize;
    }

    static bool CheckHashesCount(const ui32 value) {
        return MinHashesCount <= value && value <= MaxHashesCount;
    }

    static bool CheckFilterSizeBytes(const ui32 value) {
        return MinFilterSizeBytes <= value && value <= MaxFilterSizeBytes;
    }

    static ui32 CalcHashesCount(const double falsePositiveProbability) {
        return std::max<ui32>(1, static_cast<ui32>(-std::log(falsePositiveProbability) / std::log(2.0)));
    }

    static ui32 CalcDeprecatedRecordsCount(const double /*falsePositiveProbability*/) {
        return DeprecatedRecordsCount;
    }

    static ui32 CalcDeprecatedFilterSizeBytes(const double falsePositiveProbability) {
        const double probability = (std::isfinite(falsePositiveProbability) && falsePositiveProbability > 0.0 && falsePositiveProbability < 1.0)
            ? falsePositiveProbability
            : NDefaults::FalsePositiveProbability;
        const double hashesCount = static_cast<double>(CalcHashesCount(probability));
        const double recordsCount = static_cast<double>(CalcDeprecatedRecordsCount(probability));
        const double bitsCount =
            std::ceil((-hashesCount * recordsCount) / std::log(1.0 - std::pow(probability, 1.0 / hashesCount)));
        return std::clamp<ui32>(
            static_cast<ui32>(std::ceil(bitsCount / 8.0)), MinFilterSizeBytes, MaxFilterSizeBytes);
    }

    static double FalsePositiveProbabilityFromDeprecatedSizing(
        const std::optional<ui32> hashesCount,
        const std::optional<ui32> filterSizeBytes,
        const std::optional<ui32> recordsCount) {
        const double k = static_cast<double>(hashesCount.value_or(NDefaults::HashesCount));
        const double m = static_cast<double>(filterSizeBytes.value_or(CalcDeprecatedFilterSizeBytes(NDefaults::FalsePositiveProbability)) * 8);
        const double n = static_cast<double>(recordsCount.value_or(DeprecatedRecordsCount));
        const double oneMinus = 1.0 - std::exp(-(k * n) / m);
        return std::pow(std::clamp(oneMinus, 0.0, 1.0), k); 
    }

    static TString GetHashesCountIntervalString();
    static TString GetFilterSizeBytesIntervalString();
    static TString GetNGrammSizeIntervalString();
    static TString GetRecordsCountIntervalString();

    static TConclusionStatus ValidateParams(const double falsePositiveProbability, const ui32 nGrammSize) {
        return ValidateParams(falsePositiveProbability, nGrammSize, std::nullopt, std::nullopt);
    }

    static TConclusionStatus ValidateParams(const double falsePositiveProbability, const ui32 nGrammSize,
        const std::optional<ui32>& hashesCount, const std::optional<ui32>& filterSizeBytes) {
        if (falsePositiveProbability <= 0 || falsePositiveProbability >= 1) {
            return TConclusionStatus::Fail("FalsePositiveProbability have to be in interval (0, 1)");
        }

        if (!CheckNGrammSize(nGrammSize)) {
            return TConclusionStatus::Fail("ngramm_size have to be in bloom ngramm filter in interval " + GetNGrammSizeIntervalString());
        }

        const ui32 validatedHashesCount = hashesCount.value_or(CalcHashesCount(falsePositiveProbability));
        if (!CheckHashesCount(validatedHashesCount)) {
            return TConclusionStatus::Fail(
                "false_positive_probability have to produce hashes_count in interval " + GetHashesCountIntervalString());
        }

        if (filterSizeBytes && !CheckFilterSizeBytes(*filterSizeBytes)) {
            return TConclusionStatus::Fail(
                "filter_size_bytes have to be in bloom ngramm filter in interval " + GetFilterSizeBytesIntervalString());
        }

        return TConclusionStatus::Success();
    }

    static TConclusionStatus ValidateRequest(const TRequestSettings& request) {
        if (request.IsOldSizingMode() && !CheckRecordsCount(request.ResolvedRecordsCount())) {
            return TConclusionStatus::Fail(
                "records_count have to be in bloom ngramm filter in interval " + GetRecordsCountIntervalString());
        }

        return ValidateParams(
            request.ResolvedFalsePositiveProbability(),
            request.ResolvedNGrammSize(),
            request.ResolvedHashesCount(),
            request.ResolvedFilterSizeBytes());
    }
};

inline bool TRequestSettings::IsOldSizingMode() const {
    return (DeprecatedRecordsCount.has_value() || DeprecatedFilterSizeBytes.has_value()) && !FalsePositiveProbability.has_value();
}

inline ui32 TRequestSettings::ResolvedNGrammSize() const {
    return NGrammSize.value_or(NDefaults::NGrammSize);
}

inline bool TRequestSettings::ResolvedCaseSensitive() const {
    return CaseSensitive.value_or(NDefaults::CaseSensitive);
}

inline double TRequestSettings::ResolvedFalsePositiveProbability() const {
    if (FalsePositiveProbability) {
        return *FalsePositiveProbability;
    }

    if (!IsOldSizingMode() && !(DeprecatedHashesCount || DeprecatedFilterSizeBytes)) {
        return FalsePositiveProbability.value_or(NDefaults::FalsePositiveProbability);
    }

    return TConstants::FalsePositiveProbabilityFromDeprecatedSizing(
        DeprecatedHashesCount,
        DeprecatedFilterSizeBytes,
        DeprecatedRecordsCount);
}

inline ui32 TRequestSettings::ResolvedHashesCount() const {
    if (IsOldSizingMode()) {
        return DeprecatedHashesCount.value_or(NDefaults::HashesCount);
    }

    return TConstants::CalcHashesCount(ResolvedFalsePositiveProbability());
}

inline ui32 TRequestSettings::ResolvedFilterSizeBytes() const {
    if (IsOldSizingMode()) {
        return DeprecatedFilterSizeBytes.value_or(TConstants::CalcDeprecatedFilterSizeBytes(ResolvedFalsePositiveProbability()));
    }

    return TConstants::CalcDeprecatedFilterSizeBytes(ResolvedFalsePositiveProbability());
}

inline ui32 TRequestSettings::ResolvedRecordsCount() const {
    if (IsOldSizingMode()) {
        return DeprecatedRecordsCount.value_or(TConstants::DeprecatedRecordsCount);
    }

    return TConstants::DeprecatedRecordsCount;
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
