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

void AddLittleLeftTablePreset(NKikimr::NMiniKQL::TBenchmarkSettings& settings) {
    TVector<NKikimr::NMiniKQL::TPreset> littleRight;
    for(auto& preset: settings.Presets) {
        littleRight.push_back(preset);
        littleRight.back().PresetName += "LittleRightTable";
        littleRight.back().Size.Right /= 128;
        Y_ABORT_UNLESS(littleRight.back().Size.Right != 0, "right was too small");
        preset.PresetName += "SameSizeTables";
    }
    std::copy(littleRight.begin(), littleRight.end(), std::back_inserter(settings.Presets));
}

void MinusOne(NKikimr::NMiniKQL::TBenchmarkSettings& settings) {
    for(auto& preset: settings.Presets) {
        preset.Size.Right -= 1;
    }

}

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    NKikimr::NMiniKQL::TBenchmarkSettings params;
    params.KeyTypes = {
        NKikimr::NMiniKQL::ETestedJoinKeyType::kString,
        NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger,
    };
    TVector<NKikimr::NMiniKQL::TPreset> (*presetWithSamples)(int, int);
    int samples = 1;
    int scale = 1;
    opts.AddHelpOption().Help("visit NBenchmarkSizes namespace in benchmark_settings.cpp for explanation");
    opts.AddLongOption('p', "preset")
        .Help("left and right table sizes to choose for joins benchmark.")
        .Choices({"exp", "linear8", "linear16", "small"})
        .DefaultValue("small")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            presetWithSamples = [&]() {
                if (val == "exp") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::ExponentialSizeIncrease;
                } else if (val == "linear8") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::LinearSizeIncrease8Points;
                } else if (val == "linear16") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::LinearSizeIncrease16Points;
                } else if (val == "small") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::VerySmallSizes;
                } else {
                    Y_ABORT("unknown option for benchmark_sizes");
                }
            }();
        });
    opts.AddLongOption("samples")
        .Help("number representing how much to repeat single case. useful for noise reduction.")
        .DefaultValue(1)
        .StoreResult(&samples);
    opts.AddLongOption("scale").Help("size of smallest table in case").DefaultValue(1).StoreResult(&scale);
    opts.AddLongOption("key_type")
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
    // opts.AddLongOption("preset-flavour").Help("specific preset, do not specify for all").Choices({"same-size",
    // "little-right"}).Handler1([&](const NLastGetopt::TOptsParser* option) {
    //         auto val = TStringBuf(option->CurVal());
    //         params.KeyTypes.clear();
    //         params.KeyTypes.emplace([&]() {
    //             if (val == "string") {
    //                 return NKikimr::NMiniKQL::ETestedJoinKeyType::kString;
    //             } else if (val == "int") {
    //                 return NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger;
    //             } else {
    //                 Y_ABORT("unknown option for benchmark_sizes");
    //             }
    //         }());
    //     });

    opts.AddLongOption("seed").Help("seed for keys generation").DefaultValue(123).StoreResult(&params.Seed);
    params.Algorithms = {
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockMap,
        NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockHash,
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarMap, // slow
        NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarHash,
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarGrace,
    };

    NLastGetopt::TOptsParseResult parsedOptions(&opts, argc, argv);
    params.Presets = presetWithSamples(samples, scale);
    AddLittleLeftTablePreset(params);
    params.Presets.erase(params.Presets.begin(), params.Presets.begin() + std::ssize(params.Presets) / 2);
    // MinusOne(params);

    auto benchmarkResults = NKikimr::NMiniKQL::RunJoinsBench(params);
    TFixedBufferFileOutput file{MakeJoinPerfPath()};
    for (auto result : benchmarkResults) {
        NJson::TJsonValue out;
        out["testName"] = result.CaseName;
        out["resultTime"] = result.RunDuration.MilliSeconds();
        NKikimr::NMiniKQL::SaveJsonAt(out, &file);
    }
}