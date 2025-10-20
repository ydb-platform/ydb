#include <ydb/core/kqp/tools/join_perf/benchmark_settings.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/getopt/small/last_getopt_opts.h>
#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <library/cpp/getopt/small/last_getopt_parser.h>
#include <ydb/core/kqp/tools/combiner_perf/fs_utils.h>

#include <filesystem>
#include <util/string/printf.h>
#include <ydb/core/kqp/tools/join_perf/joins.h>

std::filesystem::path MakeJoinPerfPath() {
    auto p = std::filesystem::path{std::getenv("HOME")} / ".join_perf" / "json";
    std::filesystem::create_directories(p);
    p = p / Sprintf("%i.jsonl", NKikimr::NMiniKQL::FilesIn(p)).ConstRef();
    Cout << p.string() << Endl;
    return p;
}

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    NKikimr::NMiniKQL::TBenchmarkSettings params;
    params.KeyTypes = {NKikimr::NMiniKQL::ETestedJoinKeyType::kString, NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger};
    params.Flavours = {NKikimr::NMiniKQL::ETestedInputFlavour::kLittleRightTable,
                       NKikimr::NMiniKQL::ETestedInputFlavour::kSameSizeTable};
    params.Algorithms = {
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockMap,
        NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockHash,
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarMap, // slow
        NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarHash,
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarGrace,
    };

    TString presetName;
    TString presetsPath;
    opts.AddLongOption("preset")
        .Help("left and right table sizes to choose for joins benchmark.")
        .DefaultValue("VerySmall")
        .StoreResult(&presetName);
    opts.AddLongOption("presets-path")
        .Help("directory that contains presets.json")
        .DefaultValue(std::filesystem::current_path().string())
        .StoreResult(&presetsPath);
    opts.AddLongOption("samples")
        .Help("number representing how much to repeat single case. useful for noise reduction.")
        .DefaultValue(1)
        .StoreResult(&params.Samples);
    opts.AddLongOption("scale").Help("size of smallest table in case").DefaultValue(1).StoreResult(&params.Scale);
    opts.AddLongOption("key-type")
        .Help("specific key type, do not specify for all")
        .Choices({"string", "int"})
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            params.KeyTypes.clear();
            params.KeyTypes.emplace([&]() {
                if (val == "string") {
                    return NKikimr::NMiniKQL::ETestedJoinKeyType::kString;
                } else if (val == "int") {
                    return NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger;
                } else {
                    Y_ABORT("unknown option for benchmark_sizes");
                }
            }());
        });
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
    opts.AddLongOption("seed").Help("seed for keys generation").DefaultValue(123).StoreResult(&params.Seed);
    NLastGetopt::TOptsParseResult parsedOptions(&opts, argc, argv);

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

    auto benchmarkResults = NKikimr::NMiniKQL::RunJoinsBench(params);
    TFixedBufferFileOutput file{MakeJoinPerfPath()};
    for (auto result : benchmarkResults) {
        NJson::TJsonValue out;
        out["testName"] = result.CaseName;
        out["resultTime"] = result.RunDuration.MilliSeconds();
        NKikimr::NMiniKQL::SaveJsonAt(out, &file);
    }
}