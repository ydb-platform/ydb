#include "benchmark_settings.h"

namespace NKikimr::NMiniKQL {

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, const TBenchmarkSettings::TPreset& preset,
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
TVector<TTableSizes> ExponentialSizeIncrease() {
    TVector<TTableSizes> ret;
    int init = 1 << 18;
    for (int index = 0; index < 8; index++) {
        int thisNum = init * (1 << index);
        ret.emplace_back(thisNum, thisNum);
    }
    return ret;
}

TVector<TTableSizes> LinearSizeIncrease() {
    TVector<TTableSizes> ret;
    int init = 1 << 22;
    for (int index = 1; index < 9; index++) {
        int thisNum = init * index;
        ret.emplace_back(thisNum, thisNum);
    }
    return ret;
}

TVector<TTableSizes> VerySmallSizes() {
    return {{512, 512}, {1024, 1024}};
}
} // namespace NBenchmarkSizes

} // namespace NKikimr::NMiniKQL
