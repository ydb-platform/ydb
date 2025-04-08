#include <ydb/core/kqp/tools/combiner_perf/printout.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_last.h>
#include <ydb/core/kqp/tools/combiner_perf/simple.h>
#include <ydb/core/kqp/tools/combiner_perf/tpch_last.h>
#include <ydb/core/kqp/tools/combiner_perf/simple_block.h>

#include <library/cpp/lfalloc/alloc_profiler/profiler.h>

#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/system/compiler.h>

using NKikimr::NMiniKQL::TRunParams;

class TPrintingResultCollector : public TTestResultCollector
{
public:
    virtual void SubmitTestNameAndParams(const TRunParams& runParams, const char* testName, const std::optional<bool> llvm, const std::optional<bool> spilling) override
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
        Cout << (runParams.MaxKey + 1) << " distinct numeric keys" << Endl;
        Cout << "Block size: " << runParams.BlockSize << Endl;
        Cout << "Long strings: " << (runParams.LongStringKeys ? "yes" : "no") << Endl;
        Cout << Endl;
    }

    virtual void SubmitTimings(const TDuration& graphTime, const TDuration& referenceTime, const std::optional<TDuration> streamTime) override
    {
        Cout << "Graph runtime is: " << graphTime << " vs. reference C++ implementation: " << referenceTime << Endl;

        if (streamTime.has_value()) {
            Cout << "Input stream own iteration time: " << *streamTime << Endl;
            Cout << "Graph time - stream own time = " << (*streamTime <= graphTime ? graphTime - *streamTime : TDuration::Zero()) << Endl;
            Cout << "C++ implementation time - devnull time = " << (*streamTime <= referenceTime ? referenceTime - *streamTime : TDuration::Zero()) << Endl;
        }
    }
};

class TWikiResultCollector : public TTestResultCollector
{
public:
    TWikiResultCollector()
    {
        Cout << "#|" << Endl;
        Cout << "|| Test name | LLVM | Spilling | RowsTotal | Distinct keys | Block size | Input stream own time (s) | Graph time - stream time (s) | C++ time - stream time (s) | Shame ratio ||" << Endl;
    }

    ~TWikiResultCollector()
    {
        Cout << "|#" << Endl;
    }

    virtual void SubmitTestNameAndParams(const TRunParams& runParams, const char* testName, const std::optional<bool> llvm, const std::optional<bool> spilling) override
    {
        Cout << "|| ";
        Cout << testName << " | ";
        if (llvm.has_value()) {
            Cout << (llvm.value() ? "+" : " ");
        }
        Cout << " | ";
        if (spilling.has_value()) {
            Cout << (spilling.value() ? "+" : " ");
        }
        Cout << " | ";

        Cout << (runParams.RowsPerRun * runParams.NumRuns) << " | " << (runParams.MaxKey + 1) << " | ";
        if (TStringBuf(testName).Contains("Block")) {
            Cout << runParams.BlockSize;
        }
        Cout << " | ";
    }

    static TString FancyDuration(const TDuration duration)
    {
        const auto ms = duration.MilliSeconds();
        if (!ms) {
            return " ";
        }
        return Sprintf("%.2f", (ms / 1000.0));
    }

    virtual void SubmitTimings(const TDuration& graphTime, const TDuration& referenceTime, const std::optional<TDuration> streamTime) override
    {
        TDuration streamTimeOrZero = (streamTime.has_value()) ? streamTime.value() : TDuration::Zero();
        TDuration corrGraphTime = streamTimeOrZero <= graphTime ? graphTime - streamTimeOrZero : TDuration::Zero();
        TDuration corrRefTime = streamTimeOrZero <= referenceTime ? referenceTime - streamTimeOrZero : TDuration::Zero();

        TString diff;
        if (corrRefTime.MilliSeconds() > 0) {
            diff = Sprintf("%.2f", corrGraphTime.MilliSeconds() * 1.0 / corrRefTime.MilliSeconds());
        }

        Cout << FancyDuration(streamTimeOrZero) << " | " << FancyDuration(corrGraphTime) << " | " << FancyDuration(corrRefTime) << " | " << diff << " ||" << Endl;
        Cout.Flush();
    }
};

void DoFullPass(bool withSpilling)
{
    using namespace NKikimr::NMiniKQL;

    TWikiResultCollector printout;

    TRunParams runParams;

    runParams.NumRuns = 20;
    runParams.RowsPerRun = 5'000'000;
    runParams.MaxKey = 1'00 - 1;
    runParams.LongStringKeys = false;

    const std::vector<size_t> numKeys = {4u, 1000u, 100'000u, 200'000u};
    const std::vector<size_t> blockSizes = {128u, 8192u};

    auto doSimple = [&printout, numKeys](const TRunParams& params) {
        for (size_t keyCount : numKeys) {
            auto runParams = params;
            runParams.MaxKey = keyCount - 1;
            RunTestSimple<false>(runParams, printout);
            RunTestSimple<true>(runParams, printout);
        }
    };

    auto doSimpleLast = [&printout, &numKeys, withSpilling](const TRunParams& params) {
        for (size_t keyCount : numKeys) {
            auto runParams = params;
            runParams.MaxKey = keyCount - 1;
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
                runParams.MaxKey = keyCount - 1;
                runParams.BlockSize = blockSize;
                RunTestBlockCombineHashedSimple<false, false>(runParams, printout);
            }
        }
    };

    doSimple(runParams);
    doSimpleLast(runParams);
    doBlockHashed(runParams);
}

int main(int argc, const char* argv[])
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    if (false) {
        NAllocProfiler::StartAllocationSampling(true);
    }

    constexpr int NumIterations = 1;

    for (int i = 0; i < NumIterations; ++i) {
        DoFullPass(false);
    }

    if (false) {
        TFileOutput out("memory_profile");
        NAllocProfiler::StopAllocationSampling(out, 10000);
    }
}
