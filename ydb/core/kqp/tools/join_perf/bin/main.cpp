#include <ydb/core/kqp/tools/join_perf/benchmark_settings.h>

#include <ydb/core/kqp/tools/combiner_perf/fs_utils.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/getopt/small/last_getopt_opts.h>
#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <library/cpp/getopt/small/last_getopt_parser.h>

#include <ydb/core/kqp/tools/join_perf/joins.h>
#include <filesystem>
#include <util/string/printf.h>

std::filesystem::path MakeJoinPerfPath() {
    auto p = std::filesystem::path{std::getenv("HOME")} / ".join_perf" / "json";
    std::filesystem::create_directories(p);
    p = p / Sprintf("%i.jsonl", NKikimr::NMiniKQL::FilesIn(p)).ConstRef();
    Cout << p.string() << Endl;
    return p;
}

void AddLittleLeftTablePreset(NKikimr::NMiniKQL::TBenchmarkSettings& settings) {
    Y_ABORT_UNLESS(settings.Presets.size() == 1, "should be only 1 preset with same sizes");
    settings.Presets.push_back(settings.Presets[0]);
    settings.Presets[0].PresetName += "SameSizeTables";
    settings.Presets[1].PresetName += "LittleRightTable";
    for (auto& tableSizes : settings.Presets[1].Cases) {
        tableSizes.Right /= 128;
        if (tableSizes.Right == 0) {
            tableSizes.Right = 1;
        }
    }
}

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    NKikimr::NMiniKQL::TBenchmarkSettings params;
    NKikimr::NMiniKQL::TPreset(*presetWithSamples)(int, int);
    int samples = 1;
    int scale = 1;
    opts.AddHelpOption().Help("visit NBenchmarkSizes namespace in benchmark_settings.cpp for explanation");
    opts.AddLongOption('p', "preset")
        .Help("left and right table sizes to choose for joins benchmark.")
        .Choices({"exp", "linear", "small"})
        .DefaultValue("small")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            presetWithSamples = [&]() {
                if (val == "exp") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::ExponentialSizeIncrease;
                } else if (val == "linear") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::LinearSizeIncrease;
                } else if (val == "small") {
                    return &NKikimr::NMiniKQL::NBenchmarkSizes::VerySmallSizes;
                } else {
                    Y_ABORT("unknown option for benchmark_sizes");
                }
            }();
        });
    opts.AddLongOption("samples").Help("number representing how much to repeat single case. useful for noise reduction.").DefaultValue(1).StoreResult(&samples);
    opts.AddLongOption("scale").Help("size of smallest table in case").DefaultValue(1).StoreResult(&scale);
    opts.AddLongOption("seed").Help("seed for keys generation").DefaultValue(123).StoreResult(&params.Seed);
    params.Algorithms = {
        NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockMap,
        NKikimr::NMiniKQL::ETestedJoinAlgo::kBlockHash,
        // NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarMap, // slow
        NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarHash,
        NKikimr::NMiniKQL::ETestedJoinAlgo::kScalarGrace,
    };
    params.KeyTypes = {
        NKikimr::NMiniKQL::ETestedJoinKeyType::kString,
        NKikimr::NMiniKQL::ETestedJoinKeyType::kInteger,
    };

    NLastGetopt::TOptsParseResult parsedOptions(&opts, argc, argv);
    params.Presets.push_back(presetWithSamples(samples, scale));
    AddLittleLeftTablePreset(params);

    auto benchmarkResults = NKikimr::NMiniKQL::RunJoinsBench(params);
    TFixedBufferFileOutput file{MakeJoinPerfPath()};
    for (auto result : benchmarkResults) {
        NJson::TJsonValue out;
        out["testName"] = result.CaseName;
        out["resultTime"] = result.RunDuration.MilliSeconds();
        NKikimr::NMiniKQL::SaveJsonAt(out, &file);
    }
}