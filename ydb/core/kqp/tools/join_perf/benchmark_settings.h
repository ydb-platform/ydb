#pragma once
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

enum class ETestedJoinAlgo { kScalarGrace, kScalarMap, kBlockMap, kBlockHash, kScalarHash };
enum class ETestedJoinKeyType { kString, kInteger };

struct TTableSizes {
    int Left;
    int Right;
};

struct TBenchmarkSettings {
    struct TPreset {
        TVector<TTableSizes> Cases;
        TString PresetName;
    };

    TVector<TPreset> Presets;
    TSet<ETestedJoinKeyType> KeyTypes;
    TSet<ETestedJoinAlgo> Algorithms;
};

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TBenchmarkSettings::TPreset& preset,
                 TTableSizes size);

namespace NBenchmarkSizes {
TVector<TTableSizes> ExponentialSizeIncrease();
TVector<TTableSizes> LinearSizeIncrease();
TVector<TTableSizes> VerySmallSizes();
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
