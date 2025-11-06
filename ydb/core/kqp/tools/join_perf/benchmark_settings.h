#pragma once
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

enum class ETestedJoinAlgo { kScalarGrace, kScalarMap, kBlockMap, kBlockHash, kScalarHash };
enum class ETestedJoinKeyType { kString, kInteger };
enum class ETestedInputFlavour { kSameSizeTable, kLittleRightTable };

struct TTableSizes {
    int Left;
    int Right;
};

struct TPreset {
    TVector<TTableSizes> Sizes;
    TString PresetName;
};

struct TBenchmarkSettings {
    int Seed;
    int Scale;
    int Samples;
    TPreset Preset;
    TSet<ETestedJoinKeyType> KeyTypes;
    TSet<ETestedJoinAlgo> Algorithms;
    TSet<ETestedInputFlavour> Flavours;
};

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, ETestedInputFlavour inputFlavour,
                 const TBenchmarkSettings& preset, TTableSizes sizes);

TVector<TPreset> ParsePresetsFile(const TString& path);

} // namespace NKikimr::NMiniKQL
