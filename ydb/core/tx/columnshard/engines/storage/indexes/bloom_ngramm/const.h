#pragma once

#include <algorithm>
#include <cmath>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

struct TDerivedSettings {
    ui32 NgramSize = 0;
    bool CaseSensitive = false;
    double FalsePositiveProbability = 0.0;
    ui32 HashesCount = 0;
    ui32 FilterSizeBytes = 0;
    ui32 RecordsCount = 0;
};

class TConstants {
public:
    static constexpr ui32 MinNGrammSize = 3;
    static constexpr ui32 MaxNGrammSize = 8;
    static constexpr ui32 MinHashesCount = 1;
    static constexpr ui32 MaxHashesCount = 8;
    static constexpr ui32 DeprecatedRecordsCount = 10000;
    static constexpr ui32 MinFilterSizeBytes = 128;
    static constexpr ui32 MaxFilterSizeBytes = 1 << 20;
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
            : 0.1;
        const double hashesCount = static_cast<double>(CalcHashesCount(probability));
        const double recordsCount = static_cast<double>(CalcDeprecatedRecordsCount(probability));
        const double bitsCount =
            std::ceil((-hashesCount * recordsCount) / std::log(1.0 - std::pow(probability, 1.0 / hashesCount)));
        return std::clamp<ui32>(
            static_cast<ui32>(std::ceil(bitsCount / 8.0)), MinFilterSizeBytes, MaxFilterSizeBytes);
    }

    static TDerivedSettings BuildDerivedSettings(const double falsePositiveProbability, const ui32 ngramSize, const bool caseSensitive) {
        TDerivedSettings result;
        result.NgramSize = ngramSize;
        result.CaseSensitive = caseSensitive;
        result.FalsePositiveProbability = falsePositiveProbability;
        result.HashesCount = CalcHashesCount(falsePositiveProbability);
        result.FilterSizeBytes = CalcDeprecatedFilterSizeBytes(falsePositiveProbability);
        result.RecordsCount = CalcDeprecatedRecordsCount(falsePositiveProbability);
        return result;
    }

    static TString GetHashesCountIntervalString();
    static TString GetFilterSizeBytesIntervalString();
    static TString GetNGrammSizeIntervalString();
    static TString GetRecordsCountIntervalString();

    static TConclusionStatus ValidateParams(const double falsePositiveProbability, const ui32 nGrammSize) {
        if (falsePositiveProbability <= 0 || falsePositiveProbability >= 1) {
            return TConclusionStatus::Fail("FalsePositiveProbability have to be in interval (0, 1)");
        }

        if (!CheckNGrammSize(nGrammSize)) {
            return TConclusionStatus::Fail("ngramm_size have to be in bloom ngramm filter in interval " + GetNGrammSizeIntervalString());
        }

        const ui32 hashesCount = CalcHashesCount(falsePositiveProbability);
        if (!CheckHashesCount(hashesCount)) {
            return TConclusionStatus::Fail(
                "false_positive_probability have to produce hashes_count in interval " + GetHashesCountIntervalString());
        }

        return TConclusionStatus::Success();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
