#include "benchmark_settings.h"

namespace NKikimr::NMiniKQL {

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TPreset& preset,
                 TTableSizes size) {
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

    return algoName + "_" + keyTypeName + "_" + preset.PresetName + "_" + std::to_string(size.Left) + "_" +
           std::to_string(size.Right);
}

namespace NBenchmarkSizes {
TPreset ExponentialSizeIncrease(int samples, int scale) {
    TPreset ret;
    ret.PresetName = "ExpGrowth";
    int init = 1 << 18;
    init *= scale;
    for (int index = 0; index < 8; index++) {
        int thisNum = init * (1 << index);
        for (int _ = 0; _ < samples; ++_){
            ret.Cases.emplace_back(thisNum, thisNum);
        }
    }
    return ret;
}

TPreset LinearSizeIncrease(int samples, int scale) {
    TPreset ret;
    ret.PresetName = "LinearGrowth";
    int init = 1 << 18;
    init *= scale; 
    for (int index = 1; index < 9; index++) {
        int thisNum = init * index;
        for (int _ = 0; _ < samples; ++_){
            ret.Cases.emplace_back(thisNum, thisNum);
        }
    }
    return ret;
}

TPreset VerySmallSizes(int, int) {
    TPreset ret;
    ret.PresetName = "VerySmall";
    ret.Cases.emplace_back(512, 512);
    ret.Cases.emplace_back(1024, 1024);
    return ret;
}
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
