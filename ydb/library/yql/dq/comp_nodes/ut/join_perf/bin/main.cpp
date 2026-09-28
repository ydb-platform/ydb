#include <ydb/library/yql/dq/comp_nodes/ut/join_perf/benchmark_settings.h>
#include <ydb/library/yql/dq/comp_nodes/ut/join_perf/joins.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/getopt/small/last_getopt_opts.h>
#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <library/cpp/getopt/small/last_getopt_parser.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <filesystem>

namespace {

void PrintJson(const NJson::TJsonValue& value) {
    Cout << NJson::WriteJson(value, false, false, false) << Endl;
}

NJson::TJsonValue ResultJson(const NKikimr::NMiniKQL::TBenchmarkCaseResult& result,
                             const NKikimr::NMiniKQL::TBenchmarkSettings& params) {
    NJson::TJsonValue out;
    out["testName"] = result.CaseName;
    out["algo"] = NKikimr::NMiniKQL::AlgoOptionName(result.Algo);
    out["keySchema"] = NKikimr::NMiniKQL::KeySchemaOptionName(result.KeySchema);
    out["payload"] = NKikimr::NMiniKQL::PayloadOptionName(result.Payload);
    out["payloadColumns"] = result.Payload == NKikimr::NMiniKQL::ETestedPayload::kWide ? params.WidePayloadColumns : 1;
    out["stringBytes"] = params.StringBytes;
    out["flavour"] = NKikimr::NMiniKQL::FlavourOptionName(result.Flavour);
    out["preset"] = params.Preset.PresetName;
    out["scale"] = params.Scale;
    out["seed"] = params.Seed;
    out["resultTime"] = result.RunDuration.MilliSeconds();
    out["medianCpuUs"] = result.RunDuration.MicroSeconds();
    out["minCpuUs"] = result.MinCpu.MicroSeconds();
    out["maxCpuUs"] = result.MaxCpu.MicroSeconds();
    out["meanCpuUs"] = result.MeanCpu.MicroSeconds();
    out["stdevCpuUs"] = result.StdevCpu.MicroSeconds();
    out["medianWallUs"] = result.MedianWall.MicroSeconds();
    out["cvPercent"] = result.CvPercent;
    out["samples"] = result.Samples;
    out["minItersPerSample"] = result.MinItersPerSample;
    out["maxItersPerSample"] = result.MaxItersPerSample;
    out["outputRows"] = result.OutputRows;
    out["leftRows"] = result.Sizes.Left;
    out["rightRows"] = result.Sizes.Right;
    out["matchRate"] = result.Selectivity.MatchRate;
    out["dupsPerKey"] = result.Selectivity.DupsPerKey;
    out["joinKind"] = NKikimr::NMiniKQL::JoinKindOptionName(result.JoinKind);
    out["filter"] = NKikimr::NMiniKQL::FilterOptionName(result.Filter);
    out["leftBlocks"] = result.LeftBlocks;
    out["rightBlocks"] = result.RightBlocks;
    for (int size : params.BlockSizes) {
        out["blockSizes"].AppendValue(size);
    }
    return out;
}

} // namespace

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    NKikimr::NMiniKQL::TBenchmarkSettings params;
    params.Flavours = {NKikimr::NMiniKQL::ETestedInputFlavour::kLittleRightTable,
                       NKikimr::NMiniKQL::ETestedInputFlavour::kSameSizeTable};

    // The sweep options below may be repeated, and every combination of them is
    // measured. Each one replaces its default set on the first occurrence and
    // adds to it afterwards.
    TSet<NKikimr::NMiniKQL::ETestedPayload> payloads;
    TSet<NKikimr::NMiniKQL::ETestedJoinAlgo> algorithms;
    TVector<NKikimr::NMiniKQL::TKeySchema> keySchemas;
    TVector<NKikimr::NMiniKQL::TSelectivity> selectivities;
    TVector<NKikimr::NMiniKQL::EJoinKind> joinKinds;
    TVector<NKikimr::NMiniKQL::ETestedFilter> filters;

    TString presetName;
    TString presetsPath;
    TString keyType;
    TString blockSizes;
    opts.AddLongOption("preset")
        .Help("left and right table sizes to choose for joins benchmark.")
        .DefaultValue("VerySmall")
        .StoreResult(&presetName);
    opts.AddLongOption("presets-path")
        .Help("directory that contains presets.json")
        .DefaultValue(std::filesystem::current_path().string())
        .StoreResult(&presetsPath);
    opts.AddLongOption("samples")
        .Help("number representing how much to repeat single case. useful for "
              "noise reduction.")
        .DefaultValue(1)
        .StoreResult(&params.Samples);
    opts.AddLongOption("scale").Help("size of smallest table in case").DefaultValue(1).StoreResult(&params.Scale);
    opts.AddLongOption("key-type")
        .Help("legacy shorthand for one int or string key column, do not specify "
              "for both")
        .Choices({"string", "int"})
        .StoreResult(&keyType);
    opts.AddLongOption("preset-flavour")
        .Help("specific preset, do not specify for all")
        .Choices({"same-size", "little-right"})
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            params.Flavours.clear();
            params.Flavours.emplace([&]() {
                if (val == "same-size") {
                    return NKikimr::NMiniKQL::ETestedInputFlavour::kSameSizeTable;
                } else if (val == "little-right") {
                    return NKikimr::NMiniKQL::ETestedInputFlavour::kLittleRightTable;
                } else {
                    Y_ABORT("unknown option for benchmark_sizes");
                }
            }());
        });
    opts.AddLongOption("payload")
        .Help("payload shape: narrow is one column per side, wide uses "
              "--wide-payload-columns integers. May be repeated")
        .Choices({"narrow", "wide"})
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            payloads.emplace([&]() {
                if (val == "narrow") {
                    return NKikimr::NMiniKQL::ETestedPayload::kNarrow;
                } else if (val == "wide") {
                    return NKikimr::NMiniKQL::ETestedPayload::kWide;
                } else {
                    Y_ABORT("unknown option for payload");
                }
            }());
        });
    opts.AddLongOption("string-bytes")
        .Help("exact byte length of generated string key columns")
        .DefaultValue(NKikimr::NMiniKQL::DefaultStringBytes)
        .StoreResult(&params.StringBytes);
    opts.AddLongOption("wide-payload-columns")
        .Help("number of integer payload columns per side for --payload wide")
        .DefaultValue(NKikimr::NMiniKQL::DefaultWidePayloadColumns)
        .StoreResult(&params.WidePayloadColumns);
    opts.AddLongOption("algo")
        .Help("join algorithm, do not specify for all. May be repeated")
        .Choices({"block-hash", "block-map", "scalar-hash", "scalar-map", "scalar-grace"})
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            algorithms.emplace([&]() {
                using enum NKikimr::NMiniKQL::ETestedJoinAlgo;
                if (val == "block-hash") {
                    return kBlockHash;
                } else if (val == "block-map") {
                    return kBlockMap;
                } else if (val == "scalar-hash") {
                    return kScalarHash;
                } else if (val == "scalar-map") {
                    return kScalarMap;
                } else if (val == "scalar-grace") {
                    return kScalarGrace;
                } else {
                    Y_ABORT("unknown option for algo");
                }
            }());
        });
    opts.AddLongOption("key-schema")
        .Help("explicit key column types, comma separated, e.g. 'int,string' or "
              "'int,int,string'. "
              "May be repeated. Mutually exclusive with --key-type")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            keySchemas.push_back(NKikimr::NMiniKQL::ParseKeySchema(TString(option->CurVal())));
        });
    opts.AddLongOption("warmup")
        .Help("runs to execute and discard before measuring, to warm caches and "
              "allocator")
        .DefaultValue(0)
        .StoreResult(&params.Warmup);
    opts.AddLongOption("min-sample-ms")
        .Help("repeat a case within one sample until the sample lasts at least "
              "this long")
        .DefaultValue(0)
        .StoreResult(&params.MinSampleMs);
    opts.AddLongOption("max-iters-per-sample")
        .Help("upper bound on repetitions used to reach --min-sample-ms")
        .DefaultValue(1)
        .StoreResult(&params.MaxItersPerSample);
    opts.AddLongOption("block-size")
        .Help("length of the input blocks. A comma separated list, e.g. "
              "'8192,1024,128', makes the lengths "
              "cycle through it, so that one run feeds blocks of uneven size")
        .DefaultValue("128")
        .StoreResult(&blockSizes);
    opts.AddLongOption("selectivity")
        .Help("selectivity point as 'matchRate:dupsPerKey', e.g. '0.05:1'. May "
              "be repeated")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            selectivities.push_back(NKikimr::NMiniKQL::ParseSelectivity(TString(option->CurVal())));
        });
    opts.AddLongOption("join-kind")
        .Help("join kind: inner, left, right, full, left-semi, right-semi, "
              "left-only, "
              "right-only, exclusion or cross. May be repeated")
        .Choices({"inner", "left", "right", "full", "left-semi", "right-semi", "left-only", "right-only", "exclusion",
                  "cross"})
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            joinKinds.push_back(NKikimr::NMiniKQL::ParseJoinKind(TString(option->CurVal())));
        });
    opts.AddLongOption("filter")
        .Help("join filter pushed into the hash joins: none, left, right, common "
              "or all. "
              "May be repeated. Map and grace skip anything but none")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            filters.push_back(NKikimr::NMiniKQL::ParseFilter(TString(option->CurVal())));
        });
    opts.AddLongOption("seed").Help("seed for keys generation").DefaultValue(123).StoreResult(&params.Seed);
    NLastGetopt::TOptsParseResult(&opts, argc, argv);
    params.BlockSizes = NKikimr::NMiniKQL::ParseBlockSizes(blockSizes);
    if (params.StringBytes < NKikimr::NMiniKQL::DefaultStringBytes) {
        throw std::runtime_error{"--string-bytes must be at least 27"};
    }
    if (params.WidePayloadColumns < 1) {
        throw std::runtime_error{"--wide-payload-columns must be at least 1"};
    }

    if (!selectivities.empty()) {
        params.Selectivities = selectivities;
    }
    if (!filters.empty()) {
        params.Filters = filters;
    }
    if (!joinKinds.empty()) {
        params.JoinKinds = joinKinds;
    }

    using enum NKikimr::NMiniKQL::ETestedJoinAlgo;
    params.Payloads = payloads.empty()
                          ? TSet<NKikimr::NMiniKQL::ETestedPayload>{NKikimr::NMiniKQL::ETestedPayload::kNarrow}
                          : payloads;
    params.Algorithms = algorithms.empty() ? TSet<NKikimr::NMiniKQL::ETestedJoinAlgo>{kBlockMap, kBlockHash, kScalarMap,
                                                                                      kScalarHash, kScalarGrace}
                                           : algorithms;

    using enum NKikimr::NMiniKQL::ETestedJoinKeyType;
    if (!keySchemas.empty()) {
        if (keyType) {
            throw std::runtime_error{"--key-schema cannot be combined with --key-type"};
        }
        params.KeySchemas = keySchemas;
    } else if (keyType) {
        params.KeySchemas = {{keyType == "string" ? kString : kInteger}};
    } else {
        params.KeySchemas = {{kInteger}, {kString}};
    }

    TVector<NKikimr::NMiniKQL::TPreset> presets = NKikimr::NMiniKQL::ParsePresetsFile(presetsPath + "/presets.json");
    auto it = std::ranges::find_if(presets, [&](const auto& preset) { return preset.PresetName == presetName; });
    if (it == presets.end()) {
        throw std::runtime_error{"no " + presetName + " in presets"};
    }
    params.Preset = *it;

    if (params.Preset.PresetName == "VerySmall") {
        params.Flavours.clear();
        params.Flavours.insert(NKikimr::NMiniKQL::ETestedInputFlavour::kSameSizeTable);
    }

    NKikimr::NMiniKQL::RunJoinsBench(params, [&](const NKikimr::NMiniKQL::TBenchmarkCaseResult& result) {
        PrintJson(ResultJson(result, params));
    });
}
