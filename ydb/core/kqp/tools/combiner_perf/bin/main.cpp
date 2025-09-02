#include <ydb/core/kqp/tools/combiner_perf/subprocess.h>
#include <ydb/core/kqp/tools/combiner_perf/printout.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_last.h>
#include <ydb/core/kqp/tools/combiner_perf/simple.h>
#include <ydb/core/kqp/tools/combiner_perf/tpch_last.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_block.h>
#include <ydb/core/kqp/tools/combiner_perf/dq_combine_vs.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>
#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
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

class TPrintingResultCollector : public TTestResultCollector
{
public:
    virtual void SubmitMetrics(const TRunParams& runParams, const TRunResult& result, const char* testName, const std::optional<bool> llvm, const std::optional<bool> spilling) override
    {
        Cout << "------------------------------" << Endl;
        Cout << testName;
        if (llvm.has_value()) {
            Cout << ", " << (llvm.value() ? "+" : "-") << "llvm";
        }
        if (spilling.has_value()) {
            Cout << ", " << (spilling.value() ? "+" : "-") << "spilling";
        }
        Cout << Endl;
        Cout << "Data rows total: " << runParams.RowsPerRun << " x " << runParams.NumRuns << Endl;
        Cout << runParams.NumKeys << " distinct numeric keys" << Endl;
        Cout << "Block size: " << runParams.BlockSize << Endl;
        Cout << "Long strings: " << (runParams.LongStringKeys ? "yes" : "no") << Endl;
        Cout << "Combiner mem limit: " << runParams.WideCombinerMemLimit << Endl;
        Cout << "Hash map type: " << HashMapTypeName(runParams.ReferenceHashType) << Endl;
        Cout << Endl;

        Cout << "Graph runtime is: " << result.ResultTime << " vs. reference C++ implementation: " << result.ReferenceTime << Endl;

        if (result.GeneratorTime) {
            Cout << "Input stream own iteration time: " << result.GeneratorTime << Endl;
            Cout << "Graph time - stream own time = "
                 << (result.GeneratorTime <= result.ResultTime ? result.ResultTime - result.GeneratorTime : TDuration::Zero()) << Endl;
            Cout << "C++ implementation time - devnull time = "
                 << (result.GeneratorTime <= result.ReferenceTime ? result.ReferenceTime - result.GeneratorTime : TDuration::Zero()) << Endl;
        }

        if (result.MaxRSSDelta >= 0) {
            Cout << "MaxRSS delta, kB: " << (result.MaxRSSDelta / 1024) << Endl;
        }
        if (result.ReferenceMaxRSSDelta >= 0) {
            Cout << "MaxRSS delta, kB: " << (result.ReferenceMaxRSSDelta / 1024) << Endl;
        }
    }
};

class TJsonResultCollector : public TTestResultCollector
{
public:
    virtual void SubmitMetrics(const TRunParams& runParams, const TRunResult& result, const char* testName, const std::optional<bool> llvm, const std::optional<bool> spilling) override
    {
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
        if (TStringBuf(testName).Contains("Block")) {
            out["blockSize"] = runParams.BlockSize;
        }
        out["longStringKeys"] = runParams.LongStringKeys;
        out["numKeys"] = runParams.NumKeys;
        out["combinerMemLimit"] = runParams.WideCombinerMemLimit;
        out["hashType"] = HashMapTypeName(runParams.ReferenceHashType);

        out["generatorTime"] = result.GeneratorTime.MilliSeconds();
        out["resultTime"] = result.ResultTime.MilliSeconds();
        out["refTime"] = result.ReferenceTime.MilliSeconds();
        out["maxRssDelta"] = result.MaxRSSDelta;
        out["referenceMaxRssDelta"] = result.ReferenceMaxRSSDelta;

        Cout << NJson::WriteJson(out, false, false, false) << Endl;
        Cout.Flush();
    }
};

void DoFullPass(TRunParams runParams, bool withSpilling)
{
    using namespace NKikimr::NMiniKQL;

    TJsonResultCollector printout;

    const std::vector<size_t> numKeys = {4u, 1000u, 100'000u, 1'000'000u, 10'000'000};
    //const std::vector<size_t> numKeys = {60'000'000, 120'000'000};
    //const std::vector<size_t> numKeys = {30'000'000u};
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
        if (llvm || spilling) {
            ythrow yexception() << "LLVM/spilling are not supported for DqHashCombiner perf test";
        }
        NKikimr::NMiniKQL::RunTestDqHashCombineVsWideCombine<false>(params, printout);
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

    NLastGetopt::TOpts options;
    options.SetTitle("A sandbox to run combiners (and other compute nodes) while measuring performance/RAM usage");
    options.AddHelpOption('h');
    options.SetFreeArgsNum(0);

    options.AddLongOption("rand-seed").RequiredArgument().StoreResult(&runParams.RandomSeed)
        .Help("Random seed for the input dataset generator");
    options.AddLongOption("num-attempts").RequiredArgument().StoreResult(&runParams.NumAttempts).DefaultValue(runParams.NumAttempts)
        .Help("Number of time measurement runs to filter out random fluctuations");
    options.AddLongOption("rows-per-run").RequiredArgument().StoreResult(&runParams.RowsPerRun).DefaultValue(runParams.RowsPerRun)
        .Help("Rows per single loop of the input stream");
    options.AddLongOption("run-count").RequiredArgument().StoreResult(&runParams.NumRuns).DefaultValue(runParams.NumRuns)
        .Help("Number of loops of the input stream");
    options.AddLongOption("num-keys").RequiredArgument().StoreResult(&runParams.NumKeys).DefaultValue(runParams.NumKeys)
        .Help("Number of distinct keys in the input set (uniformly distributed)");
    options.AddLongOption("long-string-keys").NoArgument().SetFlag(&runParams.LongStringKeys)
        .Help("String keys are short and embedded by default; specify this option to use heap-allocated strings");
    options.AddLongOption("measure-reference-ram").NoArgument().SetFlag(&runParams.MeasureReferenceMemory)
        .Help("Do a separate run to measure the MaxRSS delta of a reference implementation");
    options.AddLongOption("block-size").RequiredArgument().StoreResult(&runParams.BlockSize).DefaultValue(runParams.BlockSize)
        .Help("Block size (rows = column height) for block operators, when applicable");
    options.AddLongOption("mem-limit").RequiredArgument().StoreResult(&runParams.WideCombinerMemLimit).DefaultValue(runParams.WideCombinerMemLimit)
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

    options
        .AddLongOption('t', "test")
        .Choices({"combiner", "last-combiner", "block-combiner", "dq-hash-combiner"})
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
            } else {
                ythrow yexception() << "Unknown test type: " << val;
            }
        })
        .Help("Enable single test run mode");

    options.AddLongOption("no-verify").NoArgument().Handler0([&](){
        runParams.EnableVerification = false;
    });

    options
        .AddLongOption('m', "mode")
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
        .Help("Specify partial test mode (gen = only generate input, ref = only run the reference implementation, graph = only run the compute graph implementation)");

    options.AddLongOption("llvm").NoArgument().SetFlag(&llvm)
        .Help("Enable LLVM for the single test mode, if applicable");
    options.AddLongOption("spilling").NoArgument().SetFlag(&spilling)
        .Help("Enable spilling for the single test mode, if applicable, or enable spilling tests in the full test suite");


    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    Y_ENSURE(runParams.NumKeys >= 1);
    Y_ENSURE(runParams.NumKeys <= runParams.RowsPerRun);
    Y_ENSURE(runParams.NumRuns >= 1);
    Y_ENSURE(runParams.NumAttempts >= 1);
    Y_ENSURE(runParams.BlockSize >= 1);

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
