#include "benchmark_settings.h"
#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NKikimr::NMiniKQL {

TString CaseName(ETestedJoinAlgo algo, const TKeySchema& keySchema, ETestedPayload payload, TSelectivity selectivity,
                 EJoinKind joinKind, ETestedFilter filter, ETestedInputFlavour inputFlavour,
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
    // A one column key keeps the historical names "Integer" and "String"; a
    // composite key spells out its columns, so {int, int, string} becomes
    // "IntegerIntegerString".
    TString keyTypeName = [&] {
        TString name;
        for (auto keyType : keySchema) {
            switch (keyType) {
            case NKikimr::NMiniKQL::ETestedJoinKeyType::kString:
                name += "String";
                break;
            case NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger:
                name += "Integer";
                break;
            }
        }
        return name;
    }();

    TString payloadName = [&] {
        switch (payload) {
        case ETestedPayload::kNarrow:
            return "Narrow";
        case ETestedPayload::kWide:
            return "Wide";
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

    const TString selectivityName = "M" + std::to_string(static_cast<int>(selectivity.MatchRate * 100)) + "D" +
                                    std::to_string(selectivity.DupsPerKey);
    TString filterName;
    if (filter != ETestedFilter::kNone) {
        filterName = "_F" + FilterOptionName(filter);
    }
    const int payloadColumns = payload == ETestedPayload::kWide ? benchSettings.WidePayloadColumns : 1;
    const TString shapeName = "_S" + std::to_string(benchSettings.StringBytes) + "_P" + std::to_string(payloadColumns);

    return algoName + "_" + JoinKindOptionName(joinKind) + "_" + keyTypeName + "_" + payloadName + "_" +
           selectivityName + filterName + shapeName + "_" + benchSettings.Preset.PresetName + "_" +
           std::to_string(benchSettings.Seed) + "_" + flavourName + "_" + std::to_string(sizes.Left) + "_" +
           std::to_string(sizes.Right);
}

TString AlgoOptionName(ETestedJoinAlgo algo) {
    switch (algo) {
    case ETestedJoinAlgo::kScalarGrace:
        return "scalar-grace";
    case ETestedJoinAlgo::kScalarMap:
        return "scalar-map";
    case ETestedJoinAlgo::kBlockMap:
        return "block-map";
    case ETestedJoinAlgo::kBlockHash:
        return "block-hash";
    case ETestedJoinAlgo::kScalarHash:
        return "scalar-hash";
    }
    Y_ABORT("unreachable");
}

TString KeySchemaOptionName(const TKeySchema& keySchema) {
    TString name;
    for (auto keyType : keySchema) {
        if (name) {
            name += ",";
        }
        switch (keyType) {
        case ETestedJoinKeyType::kString:
            name += "string";
            break;
        case ETestedJoinKeyType::kInteger:
            name += "int";
            break;
        }
    }
    return name;
}

TString PayloadOptionName(ETestedPayload payload) { return payload == ETestedPayload::kWide ? "wide" : "narrow"; }

TString FlavourOptionName(ETestedInputFlavour flavour) {
    return flavour == ETestedInputFlavour::kLittleRightTable ? "little-right" : "same-size";
}

TString JoinKindOptionName(EJoinKind joinKind) {
    switch (joinKind) {
    case EJoinKind::Inner:
        return "inner";
    case EJoinKind::Left:
        return "left";
    case EJoinKind::Right:
        return "right";
    case EJoinKind::Full:
        return "full";
    case EJoinKind::LeftSemi:
        return "left-semi";
    case EJoinKind::RightSemi:
        return "right-semi";
    case EJoinKind::LeftOnly:
        return "left-only";
    case EJoinKind::RightOnly:
        return "right-only";
    case EJoinKind::Exclusion:
        return "exclusion";
    case EJoinKind::Cross:
        return "cross";
    default:
        throw std::runtime_error{"join kind is not supported by DQ hash joins"};
    }
}

TString FilterOptionName(ETestedFilter filter) {
    switch (filter) {
    case ETestedFilter::kNone:
        return "none";
    case ETestedFilter::kLeft:
        return "left";
    case ETestedFilter::kRight:
        return "right";
    case ETestedFilter::kCommon:
        return "common";
    case ETestedFilter::kAll:
        return "all";
    }
    Y_ABORT("unreachable");
}

TKeySchema ParseKeySchema(const TString& spec) {
    TKeySchema schema;
    for (const auto& part : StringSplitter(spec).Split(',').SkipEmpty()) {
        const TStringBuf name = part.Token();
        if (name == "int" || name == "i") {
            schema.push_back(ETestedJoinKeyType::kInteger);
        } else if (name == "string" || name == "s") {
            schema.push_back(ETestedJoinKeyType::kString);
        } else {
            throw std::runtime_error{"unknown key column type '" + TString(name) + "', expected int or string"};
        }
    }
    if (schema.empty()) {
        throw std::runtime_error{"key schema is empty"};
    }
    return schema;
}

TSelectivity ParseSelectivity(const TString& spec) {
    TVector<TString> parts;
    StringSplitter(spec).Split(':').SkipEmpty().Collect(&parts);
    if (parts.size() != 2) {
        throw std::runtime_error{"selectivity '" + spec + "' is not of the form matchRate:dupsPerKey"};
    }
    TSelectivity selectivity;
    if (!TryFromString(parts[0], selectivity.MatchRate) || !TryFromString(parts[1], selectivity.DupsPerKey)) {
        throw std::runtime_error{"selectivity '" + spec + "' is not numeric"};
    }
    if (selectivity.MatchRate < 0.0 || selectivity.MatchRate > 1.0) {
        throw std::runtime_error{"match rate in '" + spec + "' must be within [0, 1]"};
    }
    if (selectivity.DupsPerKey < 1) {
        throw std::runtime_error{"dups per key in '" + spec + "' must be at least 1"};
    }
    return selectivity;
}

ETestedFilter ParseFilter(const TString& spec) {
    if (spec == "none") {
        return ETestedFilter::kNone;
    }
    if (spec == "left") {
        return ETestedFilter::kLeft;
    }
    if (spec == "right") {
        return ETestedFilter::kRight;
    }
    if (spec == "common") {
        return ETestedFilter::kCommon;
    }
    if (spec == "all") {
        return ETestedFilter::kAll;
    }
    throw std::runtime_error{"unknown filter '" + spec + "', expected none, left, right, common or all"};
}

EJoinKind ParseJoinKind(const TString& spec) {
    if (spec == "inner") {
        return EJoinKind::Inner;
    }
    if (spec == "left") {
        return EJoinKind::Left;
    }
    if (spec == "right") {
        return EJoinKind::Right;
    }
    if (spec == "full") {
        return EJoinKind::Full;
    }
    if (spec == "left-semi") {
        return EJoinKind::LeftSemi;
    }
    if (spec == "right-semi") {
        return EJoinKind::RightSemi;
    }
    if (spec == "left-only") {
        return EJoinKind::LeftOnly;
    }
    if (spec == "right-only") {
        return EJoinKind::RightOnly;
    }
    if (spec == "exclusion") {
        return EJoinKind::Exclusion;
    }
    if (spec == "cross") {
        return EJoinKind::Cross;
    }
    throw std::runtime_error{"unknown join kind '" + spec +
                             "', expected inner, left, right, full, left-semi, right-semi, left-only, "
                             "right-only, exclusion or cross"};
}

TVector<int> ParseBlockSizes(const TString& spec) {
    TVector<int> sizes;
    for (const auto& part : StringSplitter(spec).Split(',').SkipEmpty()) {
        int size = 0;
        if (!TryFromString(part.Token(), size) || size < 1) {
            throw std::runtime_error{"block size '" + TString(part.Token()) + "' is not a positive number"};
        }
        sizes.push_back(size);
    }
    if (sizes.empty()) {
        throw std::runtime_error{"block size list is empty"};
    }
    return sizes;
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
