#include <array>
#include <filesystem>
#include <ydb/core/kqp/tools/combiner_perf/dq_combine_vs.h>
#include <ydb/core/kqp/tools/combiner_perf/fs_utils.h>
#include <ydb/core/kqp/tools/combiner_perf/dq_block.h>
#include <ydb/core/kqp/tools/combiner_perf/printout.h>
#include <ydb/core/kqp/tools/combiner_perf/simple.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_block.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_grace_join.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_last.h>
#include <ydb/core/kqp/tools/combiner_perf/subprocess.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>

#include <util/generic/buffer.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/compiler.h>

using NKikimr::NMiniKQL::TRunParams;

TStringBuf HashMapTypeName(NKikimr::NMiniKQL::EHashMapImpl implType)
{
    switch (implType) {
    case NKikimr::NMiniKQL::EHashMapImpl::UnorderedMap:
        return "std";
    case NKikimr::NMiniKQL::EHashMapImpl::Absl:
        return "absl";
    case NKikimr::NMiniKQL::EHashMapImpl::YqlRobinHood:
        return "robinhood";
    default:
        ythrow yexception() << "Unknown hashmap impl type";
    }
}

class TPrintingResultCollector : public TTestResultCollector {
  public:
    virtual void SubmitMetrics(const TRunParams& runParams, const TRunResult& result, const char* testName,
                               const std::optional<bool> llvm, const std::optional<bool> spilling) override {
        Cout << "------------------------------" << Endl;
        Cout << testName;
        if (llvm.has_value()) {
            Cout << ", " << (llvm.value() ? "+" : "-") << "llvm";
        }
        if (spilling.has_value()) {
            Cout << ", " << (spilling.value() ? "+" : "-") << "spilling";
        }
        Cout << Endl;
        const bool dqBlock = TStringBuf(testName).Contains("DqBlock");
        Cout << "Data rows total: " << runParams.RowsPerRun << " x " << runParams.NumRuns << Endl;
        Cout << "Random seed: " << *runParams.RandomSeed << Endl;
        if (dqBlock) {
            if (runParams.DqBlockGenerator.empty()) {
                Cout << "Input file: " << runParams.DqBlockFile << Endl;
            } else {
                Cout << "Input generator: " << runParams.DqBlockGenerator << Endl;
                Cout << runParams.NumKeys << " distinct numeric keys" << Endl;
            }
            Cout << "Columns: " << JoinSeq(",", runParams.DqBlockColumns) << Endl;
            if (runParams.DqBlockAstFile.empty()) {
                Cout << "Keys: " << JoinSeq(",", runParams.DqBlockKeyColumns) << Endl;
                Cout << "Aggregations: " << JoinSeq(",", runParams.DqBlockAggregations) << Endl;
            } else {
                Cout << "Aggregation AST: " << runParams.DqBlockAstFile << Endl;
            }
            Cout << "Block size: " << runParams.BlockSize << Endl;
        } else {
            Cout << runParams.NumKeys << " distinct numeric keys" << Endl;
            Cout << "Block size: " << runParams.BlockSize << Endl;
            Cout << "Long strings: " << (runParams.LongStringKeys ? "yes" : "no") << Endl;
            Cout << "Combiner mem limit: " << runParams.WideCombinerMemLimit << Endl;
            Cout << "Hash map type: " << HashMapTypeName(runParams.ReferenceHashType) << Endl;
            Cout << "Join overlap: " << runParams.JoinOverlap << Endl;
        }
        Cout << Endl;

        Cout << "Graph runtime is: " << result.ResultTime;
        if (!dqBlock) {
            Cout << " vs. reference C++ implementation: " << result.ReferenceTime;
        }
        Cout << Endl;

        if (result.GeneratorTime) {
            Cout << "Input stream own iteration time: " << result.GeneratorTime << Endl;
            Cout << "Graph time - stream own time = "
                 << (result.GeneratorTime <= result.ResultTime ? result.ResultTime - result.GeneratorTime
                                                               : TDuration::Zero())
                 << Endl;
            Cout << "C++ implementation time - devnull time = "
                 << (result.GeneratorTime <= result.ReferenceTime ? result.ReferenceTime - result.GeneratorTime
                                                                  : TDuration::Zero())
                 << Endl;
        }

        if (result.MaxRSSDelta >= 0) {
            Cout << "MaxRSS delta, kB: " << (result.MaxRSSDelta / 1024) << Endl;
        }
        if (result.ReferenceMaxRSSDelta >= 0) {
            Cout << "MaxRSS delta, kB: " << (result.ReferenceMaxRSSDelta / 1024) << Endl;
        }
    }
};

NJson::TJsonValue MakeJsonMetrics(const TRunParams& runParams, const TRunResult& result, const char* testName,
                                  const std::optional<bool> llvm, const std::optional<bool> spilling) {
    NJson::TJsonValue out;

    out["testName"] = testName;
    if (llvm.has_value()) {
        out["llvm"] = *llvm;
    }
    if (spilling.has_value()) {
        out["spilling"] = *spilling;
    }
    out["rowsPerRun"] = runParams.RowsPerRun;
    out["numRuns"] = runParams.NumRuns;
    out["randomSeed"] = *runParams.RandomSeed;
    const bool dqBlock = TStringBuf(testName).Contains("DqBlock");
    if (TStringBuf(testName).Contains("Block") || dqBlock) {
        out["blockSize"] = runParams.BlockSize;
    }
    if (dqBlock) {
        out["dqBlockFile"] = runParams.DqBlockFile;
        out["dqBlockGenerator"] = runParams.DqBlockGenerator;
        if (!runParams.DqBlockGenerator.empty()) {
            out["numKeys"] = runParams.NumKeys;
        }
        out["dqBlockColumns"] = JoinSeq(",", runParams.DqBlockColumns);
        out["dqBlockKeys"] = JoinSeq(",", runParams.DqBlockKeyColumns);
        out["dqBlockAggregations"] = JoinSeq(",", runParams.DqBlockAggregations);
        out["dqBlockAstFile"] = runParams.DqBlockAstFile;
    } else {
        out["longStringKeys"] = runParams.LongStringKeys;
        out["numKeys"] = runParams.NumKeys;
        out["joinOverlap"] = runParams.JoinOverlap;
        out["joinRightRows"] = runParams.JoinRightRows;
        out["combinerMemLimit"] = runParams.WideCombinerMemLimit;
        out["hashType"] = HashMapTypeName(runParams.ReferenceHashType);
        out["dqTestColumns"] = runParams.CombineVsTestColumnSet;
    }

    out["generatorTime"] = result.GeneratorTime.MilliSeconds();
    out["resultTime"] = result.ResultTime.MilliSeconds();
    out["refTime"] = result.ReferenceTime.MilliSeconds();
    out["maxRssDelta"] = result.MaxRSSDelta;
    out["referenceMaxRssDelta"] = result.ReferenceMaxRSSDelta;
    return out;
}

class TJsonResultCollector : public TTestResultCollector {
    static std::filesystem::path MakePath() {
        auto p = std::filesystem::path{std::getenv("HOME")} / ".combiner_perf" / "json";
        std::filesystem::create_directories(p);
        p = p / Sprintf("%i.jsonl", NKikimr::NMiniKQL::FilesIn(p)).ConstRef();
        return p;
    }

  public:
    TJsonResultCollector()
        : Path(MakePath()), OutFile(Path)
    {}

    virtual void SubmitMetrics(const TRunParams& runParams, const TRunResult& result, const char* testName,
                               const std::optional<bool> llvm, const std::optional<bool> spilling) override {
        NJson::TJsonValue out = MakeJsonMetrics(runParams, result, testName, llvm, spilling);
        NKikimr::NMiniKQL::SaveJsonAt(out, &OutFile);
    }

    ~TJsonResultCollector() {
        Cerr << "Saved results at " << Path.string() << Endl;
    }

    std::filesystem::path Path;
    TFixedBufferFileOutput OutFile;
};

void DoFullPass(TRunParams runParams, bool withSpilling)
{
    using namespace NKikimr::NMiniKQL;

    TJsonResultCollector printout;

    const std::vector<size_t> numKeys = {4u, 1000u, 100'000u, 1'000'000u, 10'000'000};
    // const std::vector<size_t> numKeys = {60'000'000, 120'000'000};
    // const std::vector<size_t> numKeys = {30'000'000u};
    const std::vector<size_t> blockSizes = {128u, 8192u};

    auto doSimple = [&printout, numKeys](const TRunParams& params) {
        for (size_t memLimit : {0ULL, 30ULL << 20}) {
            for (size_t keyCount : numKeys) {
                auto runParams = params;
                runParams.NumKeys = keyCount;
                runParams.WideCombinerMemLimit = memLimit;
                RunTestSimple<false>(runParams, printout);
                RunTestSimple<true>(runParams, printout);
            }
        }
    };

    auto doSimpleLast = [&printout, &numKeys, withSpilling](const TRunParams& params) {
        for (size_t keyCount : numKeys) {
            auto runParams = params;
            runParams.NumKeys = keyCount;
            RunTestCombineLastSimple<false, false>(runParams, printout);
            RunTestCombineLastSimple<true, false>(runParams, printout);
            if (withSpilling) {
                RunTestCombineLastSimple<false, true>(runParams, printout);
                RunTestCombineLastSimple<true, true>(runParams, printout);
            }
        }
    };

    auto doBlockHashed = [&printout, &numKeys, &blockSizes](const TRunParams& params) {
        for (size_t keyCount : numKeys) {
            for (size_t blockSize : blockSizes) {
                auto runParams = params;
                runParams.NumKeys = keyCount;
                runParams.BlockSize = blockSize;
                RunTestBlockCombineHashedSimple<false, false>(runParams, printout);
            }
        }
    };

    Y_UNUSED(doBlockHashed, doSimple, doSimpleLast);

    doSimple(runParams);
    doSimpleLast(runParams);
    doBlockHashed(runParams);
}

enum class ETestType {
    All,
    SimpleCombiner,
    SimpleLastCombiner,
    BlockCombiner,
    DqHashCombinerVs,
    DqBlock,
    SimpleGraceJoin,
};

void DoSelectedTest(TRunParams params, ETestType testType, bool llvm, bool spilling)
{
    TJsonResultCollector printout;

    if (testType == ETestType::SimpleCombiner) {
        if (llvm) {
            NKikimr::NMiniKQL::RunTestSimple<true>(params, printout);
        } else {
            NKikimr::NMiniKQL::RunTestSimple<false>(params, printout);
        }
    } else if (testType == ETestType::BlockCombiner) {
        if (llvm) {
            NKikimr::NMiniKQL::RunTestBlockCombineHashedSimple<true, false>(params, printout);
        } else {
            NKikimr::NMiniKQL::RunTestBlockCombineHashedSimple<false, false>(params, printout);
        }
    } else if (testType == ETestType::SimpleLastCombiner) {
        if (spilling) {
            if (llvm) {
                NKikimr::NMiniKQL::RunTestCombineLastSimple<true, true>(params, printout);
            } else {
                NKikimr::NMiniKQL::RunTestCombineLastSimple<false, true>(params, printout);
            }
        } else {
            if (llvm) {
                NKikimr::NMiniKQL::RunTestCombineLastSimple<true, false>(params, printout);
            } else {
                NKikimr::NMiniKQL::RunTestCombineLastSimple<false, false>(params, printout);
            }
        }
    } else if (testType == ETestType::DqHashCombinerVs) {
        if (spilling) {
            if (llvm) {
                NKikimr::NMiniKQL::RunTestDqHashCombineVsWideCombine<true, true>(params, printout);
            } else {
                NKikimr::NMiniKQL::RunTestDqHashCombineVsWideCombine<false, true>(params, printout);
            }
        } else {
            if (llvm) {
                NKikimr::NMiniKQL::RunTestDqHashCombineVsWideCombine<true, false>(params, printout);
            } else {
                NKikimr::NMiniKQL::RunTestDqHashCombineVsWideCombine<false, false>(params, printout);
            }
        }
    } else if (testType == ETestType::DqBlock) {
        if (spilling) {
            if (llvm) {
                NKikimr::NMiniKQL::RunTestDqBlock<true, true>(params, printout);
            } else {
                NKikimr::NMiniKQL::RunTestDqBlock<false, true>(params, printout);
            }
        } else if (llvm) {
            NKikimr::NMiniKQL::RunTestDqBlock<true, false>(params, printout);
        } else {
            NKikimr::NMiniKQL::RunTestDqBlock<false, false>(params, printout);
        }
    } else if (testType == ETestType::SimpleGraceJoin) {
        if (params.NumRuns != 1) {
            Cerr << "Join tests only support run-count == 1. Force-setting run-count to 1" << Endl;
            params.NumRuns = 1;
        }
        NKikimr::NMiniKQL::RunTestGraceJoinSimple(params, printout);
    }
}

int main(int argc, const char* argv[])
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    TRunParams runParams;

    runParams.NumAttempts = 1;
    runParams.RowsPerRun = 10'000'000;
    runParams.NumRuns = 1;
    runParams.NumKeys = 1000;
    runParams.LongStringKeys = false;
    runParams.MeasureReferenceMemory = false;
    runParams.BlockSize = 8192;
    runParams.WideCombinerMemLimit = 0;

    ETestType testType = ETestType::All;
    bool spilling = false;
    bool llvm = false;
    double joinOverlap = .0;

    NLastGetopt::TOpts options;
    options.SetTitle("A sandbox to run combiners (and other compute nodes) while measuring performance/RAM usage");
    options.AddHelpOption('h');
    options.SetFreeArgsNum(0);

    options.AddLongOption("rand-seed")
        .RequiredArgument()
        .StoreResult(&runParams.RandomSeed)
        .Help("Random seed for the input dataset generator");
    options.AddLongOption("num-attempts")
        .RequiredArgument()
        .StoreResult(&runParams.NumAttempts)
        .DefaultValue(runParams.NumAttempts)
        .Help("Number of time measurement runs to filter out random fluctuations");
    options.AddLongOption("rows-per-run")
        .RequiredArgument()
        .StoreResult(&runParams.RowsPerRun)
        .DefaultValue(runParams.RowsPerRun)
        .Help("Rows per single loop of the input stream; 0 reads all rows from a dq-block file");
    options.AddLongOption("run-count")
        .RequiredArgument()
        .StoreResult(&runParams.NumRuns)
        .DefaultValue(runParams.NumRuns)
        .Help("Number of loops of the input stream");
    options.AddLongOption("num-keys")
        .RequiredArgument()
        .StoreResult(&runParams.NumKeys)
        .DefaultValue(runParams.NumKeys)
        .Help("Number of distinct keys in the input set (uniformly distributed)");
    options.AddLongOption("long-string-keys")
        .NoArgument()
        .SetFlag(&runParams.LongStringKeys)
        .Help("String keys are short and embedded by default; specify this option to use heap-allocated strings");
    options.AddLongOption("measure-reference-ram")
        .NoArgument()
        .SetFlag(&runParams.MeasureReferenceMemory)
        .Help("Do a separate run to measure the MaxRSS delta of a reference implementation");
    options.AddLongOption("block-size")
        .RequiredArgument()
        .StoreResult(&runParams.BlockSize)
        .DefaultValue(runParams.BlockSize)
        .Help("Block size (rows = column height) for block operators, when applicable");
    options.AddLongOption("mem-limit")
        .RequiredArgument()
        .StoreResult(&runParams.WideCombinerMemLimit)
        .DefaultValue(runParams.WideCombinerMemLimit)
        .Help("Memory limit for the wide combiner, in MB, or 0 to disable");

    options.AddLongOption("sampler")
        .Choices({"stringnum", "numnum"})
        .RequiredArgument()
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            if (val == "stringnum") {
                runParams.SamplerType = NKikimr::NMiniKQL::ESamplerType::StringKeysUI64Values;
            } else if (val == "numnum") {
                runParams.SamplerType = NKikimr::NMiniKQL::ESamplerType::UI64KeysUI64Values;
            }
        })
        .Help("Input data type: string key -> ui64 numeric value or ui64 numeric key -> ui64 numeric value");

    options.AddLongOption("quota-mode")
        .Choices({"none", "steady", "pressure"})
        .RequiredArgument()
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            if (val == "none") {
                runParams.QuotaMode = NKikimr::NMiniKQL::EQuotaMode::None;
            } else if (val == "steady") {
                runParams.QuotaMode = NKikimr::NMiniKQL::EQuotaMode::Steady;
            } else if (val == "pressure") {
                runParams.QuotaMode = NKikimr::NMiniKQL::EQuotaMode::Pressure;
            }
        })
        .Help("Operator memory quota binding for the DqHashCombine tests: none (allocator heuristics), "
              "steady (bound, availability positive), pressure (bound, availability negative at the spilling trigger)");

    options.AddLongOption("hashmap")
        .Choices({"std", "absl", "robinhood"})
        .RequiredArgument()
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            if (val == "std") {
                runParams.ReferenceHashType = NKikimr::NMiniKQL::EHashMapImpl::UnorderedMap;
            } else if (val == "absl") {
                runParams.ReferenceHashType = NKikimr::NMiniKQL::EHashMapImpl::Absl;
            } else if (val == "robinhood") {
                runParams.ReferenceHashType = NKikimr::NMiniKQL::EHashMapImpl::YqlRobinHood;
            } else {
                ythrow yexception() << "Unimplemented hash map type: " << val;
            }
        })
        .Help("Hash map type (std::unordered_map or absl::dense_hash_map)");

    options.AddLongOption('t', "test")
        .Choices({"combiner", "last-combiner", "block-combiner", "dq-hash-combiner", "dq-block", "grace-join"})
        .RequiredArgument("TEST_TYPE")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            if (val == "combiner") {
                testType = ETestType::SimpleCombiner;
            } else if (val == "last-combiner") {
                testType = ETestType::SimpleLastCombiner;
            } else if (val == "block-combiner") {
                testType = ETestType::BlockCombiner;
            } else if (val == "dq-hash-combiner") {
                testType = ETestType::DqHashCombinerVs;
            } else if (val == "dq-block") {
                testType = ETestType::DqBlock;
            } else if (val == "grace-join") {
                testType = ETestType::SimpleGraceJoin;
            } else {
                ythrow yexception() << "Unknown test type: " << val;
            }
        })
        .Help("Enable single test run mode");

    options.AddLongOption("no-verify")
        .NoArgument()
        .Handler0([&]() { runParams.EnableVerification = false; })
        .Help("Don't check that the graph and the reference results are actually equal");

    options.AddLongOption('m', "mode")
        .Choices({"gen", "ref", "graph", "all"})
        .DefaultValue("all")
        .Handler1([&](const NLastGetopt::TOptsParser* option) {
            auto val = TStringBuf(option->CurVal());
            if (val == "gen") {
                runParams.TestMode = NKikimr::NMiniKQL::ETestMode::GeneratorOnly;
            } else if (val == "ref") {
                runParams.TestMode = NKikimr::NMiniKQL::ETestMode::RefOnly;
            } else if (val == "graph") {
                runParams.TestMode = NKikimr::NMiniKQL::ETestMode::GraphOnly;
            }
        })
        .Help("Specify partial test mode (gen = only generate input, ref = only run the reference implementation, "
              "graph = only run the compute graph implementation)");

    options.AddLongOption("llvm").NoArgument().SetFlag(&llvm).Help(
        "Enable LLVM for the single test mode, if applicable");
    options.AddLongOption("spilling")
        .NoArgument()
        .SetFlag(&spilling)
        .Help(
            "Enable spilling for the single test mode, if applicable, or enable spilling tests in the full test suite");

    options.AddLongOption("join-overlap")
        .RequiredArgument()
        .DefaultValue(100.0)
        .StoreResult(&joinOverlap)
        .Help("Percentage of overlapping keys in left and right tables for join tests (relative to the number of "
              "distinct keys)");

    options.AddLongOption("join-right-rows")
        .RequiredArgument()
        .StoreResult(&runParams.JoinRightRows)
        .Help("Size for the right table in the join; defaults to num-keys");

    auto colConfigs = NKikimr::NMiniKQL::GetColumnConfigurationNames();
    options.AddLongOption("dq-test-columns")
        .Choices(THashSet<TString>(colConfigs.begin(), colConfigs.end()))
        .DefaultValue(colConfigs.front())
        .StoreResult(&runParams.CombineVsTestColumnSet)
        .Help("Select the set of columns for the dq-hash-combiner test from a list of named configurations");

    options.AddLongOption("dq-block-file")
        .RequiredArgument("PATH")
        .StoreResult(&runParams.DqBlockFile)
        .Help("Input file for the dq-block test (currently Parquet)");
    options.AddLongOption("dq-block-generator")
        .RequiredArgument("shuffle")
        .StoreResult(&runParams.DqBlockGenerator)
        .Help("Generated dq-block input; shuffle produces a shuffled Uint32 column named i");
    options.AddLongOption("dq-block-columns")
        .RequiredArgument("NAME,...")
        .SplitHandler(&runParams.DqBlockColumns, ',')
        .Help("Input columns to preload, in input order");
    options.AddLongOption("dq-block-keys")
        .RequiredArgument("NAME,...")
        .SplitHandler(&runParams.DqBlockKeyColumns, ',')
        .Help("Key columns for the dq-block aggregation");
    options.AddLongOption("dq-block-aggregations")
        .RequiredArgument("AGG,...")
        .SplitHandler(&runParams.DqBlockAggregations, ',')
        .Help("Aggregations: sum:column_name or count");
    options.AddLongOption("dq-block-ast")
        .RequiredArgument("PATH")
        .StoreResult(&runParams.DqBlockAstFile)
        .Help("Textual input transform, aggregation lambdas, and output key width");

    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    const std::array<TString, 6> dqBlockOptions = {
        "dq-block-file", "dq-block-generator", "dq-block-columns", "dq-block-keys",
        "dq-block-aggregations", "dq-block-ast"};
    if (testType != ETestType::DqBlock) {
        for (const auto& option : dqBlockOptions) {
            if (parsedOptions.Has(option)) {
                ythrow yexception() << "--" << option << " is only valid with -t dq-block";
            }
        }
    } else {
        const bool hasFile = parsedOptions.Has("dq-block-file");
        const bool hasGenerator = parsedOptions.Has("dq-block-generator");
        Y_ENSURE(hasFile != hasGenerator,
            "Specify exactly one of --dq-block-file and --dq-block-generator");
        if (hasFile) {
            Y_ENSURE(parsedOptions.Has("dq-block-columns"),
                "--dq-block-columns is required with --dq-block-file");
        } else {
            Y_ENSURE(!runParams.DqBlockGenerator.empty(),
                "--dq-block-generator cannot be empty");
            Y_ENSURE(runParams.RowsPerRun > 0,
                "A positive --rows-per-run is required with --dq-block-generator");
            Y_ENSURE(runParams.NumKeys >= 1,
                "A positive --num-keys is required with --dq-block-generator");
            Y_ENSURE(runParams.NumKeys <= runParams.RowsPerRun,
                "--num-keys cannot exceed --rows-per-run with --dq-block-generator");
            if (parsedOptions.Has("dq-block-columns")) {
                Y_ENSURE(runParams.DqBlockColumns == std::vector<std::string>{"i"},
                    "The shuffle generator only provides column i");
            } else {
                runParams.DqBlockColumns = {"i"};
            }
        }
        const bool hasAst = parsedOptions.Has("dq-block-ast");
        const bool hasKeys = parsedOptions.Has("dq-block-keys");
        const bool hasAggregations = parsedOptions.Has("dq-block-aggregations");
        Y_ENSURE(hasAst || (hasKeys && hasAggregations),
            "Specify either --dq-block-ast or both --dq-block-keys and --dq-block-aggregations");
        Y_ENSURE(!hasAst || (!hasKeys && !hasAggregations),
            "--dq-block-ast cannot be combined with --dq-block-keys or --dq-block-aggregations");
        Y_ENSURE(runParams.TestMode == NKikimr::NMiniKQL::ETestMode::Full ||
                runParams.TestMode == NKikimr::NMiniKQL::ETestMode::GraphOnly,
            "The dq-block test only supports mode=all and mode=graph");
    }

    if (testType != ETestType::DqBlock) {
        Y_ENSURE(runParams.NumKeys >= 1);
        Y_ENSURE(runParams.NumKeys <= runParams.RowsPerRun);
    }
    Y_ENSURE(runParams.NumRuns >= 1);
    Y_ENSURE(runParams.NumAttempts >= 1);
    Y_ENSURE(runParams.BlockSize >= 1);
    Y_ENSURE(joinOverlap >= 0.0 && joinOverlap <= 100.0);

    joinOverlap /= 100.0;
    runParams.JoinOverlap = std::min(static_cast<size_t>(runParams.NumKeys * joinOverlap), runParams.NumKeys);

    runParams.JoinRightRows = runParams.NumKeys;

    runParams.WideCombinerMemLimit <<= 20;

    if (!runParams.RandomSeed.has_value()) {
        runParams.RandomSeed = std::time(nullptr);
    }

    if (false) {
        NAllocProfiler::StartAllocationSampling(true);
    }

    if (testType != ETestType::All) {
        DoSelectedTest(runParams, testType, llvm, spilling);
    } else {
        runParams.AlwaysSubprocess = true;
        DoFullPass(runParams, spilling);
    }

    if (false) {
        TFileOutput out("memory_profile");
        NAllocProfiler::StopAllocationSampling(out, 10000);
    }
}
