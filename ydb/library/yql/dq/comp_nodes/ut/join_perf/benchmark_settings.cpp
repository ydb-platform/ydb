#include "benchmark_settings.h"
#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>

namespace NKikimr::NMiniKQL {

TString CaseName(ETestedJoinAlgo algo, ETestedJoinKeyType keyType, ETestedInputFlavour inputFlavour,
                 const TBenchmarkSettings& benchSettings, TTableSizes sizes) {
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

    TString flavourName = [&] {
        switch (inputFlavour) {

        case ETestedInputFlavour::kSameSizeTable:
            return "SameSize";
        case ETestedInputFlavour::kLittleRightTable:
            return "LittleRight";
            break;
        }
    }();

    return algoName + "_" + keyTypeName + "_" + benchSettings.Preset.PresetName + "_" +
           std::to_string(benchSettings.Seed) + "_" + flavourName + "_" + std::to_string(sizes.Left) + "_" +
           std::to_string(sizes.Right);
}

TVector<TPreset> ParsePresetsFile(const TString& path) {
    TVector<TPreset> ret;
    auto inputFile = TMappedFileInput{path};
    auto json = NJson::ReadJsonFastTree(inputFile.ReadAll());
    const auto& map = json.GetMapSafe();
    for (auto& kv : map) {
        ret.emplace_back();
        ret.back().PresetName = kv.first;
        auto arr = kv.second.GetArraySafe();
        for (auto& val : arr) {
            ret.back().Sizes.emplace_back(val[0].GetInteger(), val[1].GetInteger());
        }
    }
    return ret;
}

} // namespace NKikimr::NMiniKQL
