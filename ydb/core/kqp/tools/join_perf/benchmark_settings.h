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
    TTableSizes Size;
    int Samples;
    TString PresetName;
};

struct TBenchmarkSettings {
    int Seed;
    TVector<TPreset> Presets;
    TSet<ETestedJoinKeyType> KeyTypes;
    TSet<ETestedJoinAlgo> Algorithms;
};

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TPreset& preset);

namespace NBenchmarkSizes {
TVector<TPreset> ExponentialSizeIncrease(int samples, int scale);
TVector<TPreset> LinearSizeIncrease8Points(int samples, int scale);
TVector<TPreset> LinearSizeIncrease16Points(int samples, int scale);
TVector<TPreset> VerySmallSizes(int samples, int scale);
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
