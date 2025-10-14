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
struct TPreset {
    TVector<TTableSizes> Cases;
    TString PresetName;
};

struct TBenchmarkSettings {
    int Seed;
    TVector<TPreset> Presets;
    TSet<ETestedJoinKeyType> KeyTypes;
    TSet<ETestedJoinAlgo> Algorithms;
};

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TPreset& preset,
                 TTableSizes size);

namespace NBenchmarkSizes {
TPreset ExponentialSizeIncrease(int samples, int scale);
TPreset LinearSizeIncrease(int samples, int scale);
TPreset VerySmallSizes(int samples, int scale);
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
