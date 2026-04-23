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

struct TCanonicalSettings {
    ui32 NgramSize = NDefaults::NGrammSize;
    bool CaseSensitive = NDefaults::CaseSensitive;
    double FalsePositiveProbability = NDefaults::FalsePositiveProbability;
    ui32 HashesCount = 0;
    ui32 FilterSizeBytes = 0;
    ui32 RecordsCount = 0;
    bool UseDeprecatedSizing = false;
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

    static bool IsDeprecatedSizingMode(const std::optional<ui32>& /*hashesCount*/,
        const std::optional<ui32>& /*filterSizeBytes*/,
        const std::optional<ui32>& recordsCount) {
        return recordsCount && *recordsCount != 0;
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

    static double FalsePositiveProbabilityFromDeprecatedSizing(const std::optional<ui32> hashesCount, const std::optional<ui32> filterSizeBytes, const std::optional<ui32> recordsCount) {
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

    static TConclusion<TCanonicalSettings> BuildCanonicalSettings(
        const std::optional<double>& falsePositiveProbability,
        const std::optional<ui32>& ngramSize,
        const std::optional<bool>& caseSensitive,
        const std::optional<ui32>& hashesCount,
        const std::optional<ui32>& filterSizeBytes,
        const std::optional<ui32>& recordsCount) {
        std::optional<ui32> normalizedRecordsCount = recordsCount;
        if (!falsePositiveProbability && (hashesCount || filterSizeBytes) && !normalizedRecordsCount) {
            normalizedRecordsCount = DeprecatedRecordsCount;
        }

        TCanonicalSettings result;
        result.NgramSize = ngramSize.value_or(NDefaults::NGrammSize);
        result.CaseSensitive = caseSensitive.value_or(NDefaults::CaseSensitive);
        result.UseDeprecatedSizing = IsDeprecatedSizingMode(hashesCount, filterSizeBytes, normalizedRecordsCount);
        if (result.UseDeprecatedSizing) {
            result.FalsePositiveProbability = falsePositiveProbability.value_or(
                FalsePositiveProbabilityFromDeprecatedSizing(hashesCount, filterSizeBytes, normalizedRecordsCount));
            result.HashesCount = hashesCount.value_or(NDefaults::HashesCount);
            result.FilterSizeBytes = filterSizeBytes.value_or(CalcDeprecatedFilterSizeBytes(result.FalsePositiveProbability));
            result.RecordsCount = *normalizedRecordsCount;
            if (!CheckRecordsCount(result.RecordsCount)) {
                return TConclusionStatus::Fail(
                    "records_count have to be in bloom ngramm filter in interval " + GetRecordsCountIntervalString());
            }
        } else {
            result.FalsePositiveProbability = falsePositiveProbability.value_or(NDefaults::FalsePositiveProbability);
            result.HashesCount = CalcHashesCount(result.FalsePositiveProbability);
            result.FilterSizeBytes = CalcDeprecatedFilterSizeBytes(result.FalsePositiveProbability);
            result.RecordsCount = DeprecatedRecordsCount;
        }

        if (auto c = ValidateParams(result.FalsePositiveProbability, result.NgramSize, result.HashesCount, result.FilterSizeBytes);
            c.IsFail()) {
            return c;
        }

        return result;
    }

    template <class TBloomFilterProto>
    static TConclusion<TCanonicalSettings> BuildCanonicalSettingsFromProtoFilter(const TBloomFilterProto& bFilter) {
        const auto recordsCount = bFilter.HasRecordsCount() && bFilter.GetRecordsCount() != 0
            ? std::optional<ui32>(bFilter.GetRecordsCount()) : std::nullopt;
        return BuildCanonicalSettings(
            bFilter.HasFalsePositiveProbability() ? std::optional<double>(bFilter.GetFalsePositiveProbability()) : std::nullopt,
            bFilter.HasNGrammSize() ? std::optional<ui32>(bFilter.GetNGrammSize()) : std::nullopt,
            bFilter.HasCaseSensitive() ? std::optional<bool>(bFilter.GetCaseSensitive()) : std::nullopt,
            bFilter.HasHashesCount() ? std::optional<ui32>(bFilter.GetHashesCount()) : std::nullopt,
            bFilter.HasFilterSizeBytes() ? std::optional<ui32>(bFilter.GetFilterSizeBytes()) : std::nullopt,
            recordsCount);
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
