#include "benchmark_settings.h"

namespace NKikimr::NMiniKQL {

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TPreset& preset) {
    TString algoName = [&] {
        switch (algo) {

        case ETestedJoinAlgo::kScalarGrace:
            return "ScalarGrace";
        case ETestedJoinAlgo::kScalarMap:
            return "ScalarMap";
        case ETestedJoinAlgo::kBlockMap:
            return "BlockMap";
        case ETestedJoinAlgo::kBlockHash:
            return "BlockHash";
        case ETestedJoinAlgo::kScalarHash:
            return "ScalarHash";
        default:
            Y_ABORT("unreachable");
        }
    }();
    TString keyTypeName = [&] {
        switch (keyType) {
        case NKikimr::NMiniKQL::ETestedJoinKeyType::kString:
            return "String";
        case NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger:
            return "Integer";
        default:
            Y_ABORT("unreachable");
        }
    }();

    return algoName + "_" + keyTypeName + "_" + preset.PresetName + "_" + std::to_string(preset.Size.Left) + "_" +
           std::to_string(preset.Size.Right);
}

namespace NBenchmarkSizes {
TVector<TPreset> ExponentialSizeIncrease(int samples, int scale) {
    TVector<TPreset> ret;
    TPreset preset;
    preset.PresetName = "ExpGrowth";
    preset.Samples = samples;
    int init = 1 << 18;
    init *= scale;
    for (int index = 0; index < 8; index++) {
        int thisNum = init * (1 << index);
        preset.Size = {thisNum, thisNum};
        ret.push_back(preset);
    }
    return ret;
}

TVector<TPreset> LinearSizeIncrease8Points(int samples, int scale) {
    TVector<TPreset> ret;
    TPreset preset;
    preset.PresetName = "LinearGrowth";
    preset.Samples = samples;
    int init = 1 << 18;
    init *= scale;
    for (int index = 1; index < 9; index++) {
        int thisNum = init * index;
        preset.Size = {thisNum, thisNum};
        ret.emplace_back(preset);
    }
    return ret;
}

TVector<TPreset> LinearSizeIncrease16Points(int samples, int scale) {
    TVector<TPreset> ret;
    TPreset preset;
    preset.PresetName = "LinearGrowth";
    preset.Samples = samples;
    int init = 1 << 18;
    init *= scale;
    for (int index = 1; index < 17; index++) {
        int thisNum = init * index;
        preset.Size = {thisNum, thisNum};
        ret.emplace_back(preset);
    }
    return ret;
}

TVector<TPreset> VerySmallSizes(int, int) {
    return {{{512, 512}, 1, "VerySmall"}, {{1024, 1024}, 1, "VerySmall"}};
}
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
