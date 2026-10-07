#include "bench.h"
#include "func.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/info.h>

namespace {

bool IsPowerOfTwo(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

TVector<ui32> ParseCoreList(const TString& value) {
    TVector<ui32> cores;
    for (const auto& part : StringSplitter(value).Split(',').SkipEmpty()) {
        cores.push_back(FromString<ui32>(part.Token()));
    }
    return cores;
}

void VerifySiblingPairs(
    const TVector<ui32>& left,
    const TVector<ui32>& right)
{
    for (size_t i = 0; i < left.size(); ++i) {
        const TString path = TString("/sys/devices/system/cpu/cpu") +
            ToString(left[i]) + "/topology/thread_siblings_list";
        try {
            TFileInput file(path);
            const TString siblings = StripString(file.ReadAll());
            if (!siblings.Contains(ToString(right[i]))) {
                Cout << "Warning: right core " << right[i]
                     << " may not be the SMT sibling of left core " << left[i]
                     << " (read " << siblings << " from " << path << ")\n";
            }
        } catch (const std::exception& e) {
            // sysfs may be unavailable in containers; don't abort, just warn.
            Cout << "Warning: could not verify SMT sibling for core " << left[i]
                 << " via " << path << ": " << e.what() << '\n';
        }
    }
}

TVector<TScenarioDesc> BuildScenarios(
    const TVector<ui32>& left,
    const TVector<ui32>& right,
    bool hasRight)
{
    TVector<TScenarioDesc> scenarios;
    if (left.empty()) {
        return scenarios;
    }

    scenarios.push_back({"1T", {left[0]}, false});

    if (left.size() >= 2) {
        scenarios.push_back({"2T-phys", {left[0], left[1]}, false});
    }

    if (hasRight) {
        scenarios.push_back({"2T-ht", {left[0], right[0]}, true});
    } else {
        Cout << "Skipping 2T-ht scenario: --right-cores not provided\n";
    }

    if (left.size() > 2) {
        scenarios.push_back({"NT-phys", left, false});
    }

    if (!hasRight) {
        if (left.size() > 1) {
            Cout << "Skipping 2NT-ht scenario: --right-cores not provided\n";
        }
    } else if (left.size() <= 1) {
        Cout << "Skipping 2NT-ht scenario: requires more than 1 left core\n";
    } else {
        TScenarioDesc both;
        both.Name = "2NT-ht";
        both.SharesPhysicalCore = true;
        both.Cores = left;
        both.Cores.insert(both.Cores.end(), right.begin(), right.end());
        scenarios.push_back(std::move(both));
    }

    return scenarios;
}

int RunFunctional(
    bool hasOutput, bool hasInput,
    const TString& outputPath, double outputSizeGiB,
    const TString& inputPath, bool verifyData,
    ui32 threads, ui64 seed, ui32 durationS)
{
    if (hasOutput && hasInput) {
        Cerr << "--output and --input are mutually exclusive\n";
        return 1;
    }
    if (hasOutput && outputSizeGiB <= 0) {
        Cerr << "--output requires --output-size > 0\n";
        return 1;
    }
    if (threads == 0) {
        Cerr << "--threads must be positive\n";
        return 1;
    }

    if (!RunStartupSelfChecks()) {
        return 1;
    }

    if (hasOutput) {
        return RunFileWrite(outputPath, outputSizeGiB, threads, seed) ? 0 : 1;
    }
    if (hasInput) {
        return RunFileVerify(inputPath, threads, verifyData) ? 0 : 1;
    }
    return RunFunctionalSweep(threads, seed, durationS) ? 0 : 1;
}

int RunPerf(int argc, char** argv) {
    using namespace NLastGetopt;

    TBenchParams params;
    bool perf = false;
    bool csv = false;
    TVector<ui32> leftCores;
    TVector<ui32> rightCores;

    bool functional = false;
    ui32 funcThreads = 0; // 0 => default to online CPU count
    ui64 funcSeed = 1;
    ui32 funcDurationS = 10;
    TString outputPath;
    double outputSizeGiB = 0;
    TString inputPath;
    bool verifyData = false;

    TOpts opts = TOpts::Default();
    opts.AddLongOption("perf", "run bandwidth benchmark").NoArgument().SetFlag(&perf);
    opts.AddLongOption("functional", "run equivalence sweep / file write / file verify")
        .NoArgument().SetFlag(&functional);
    opts.AddLongOption("threads", "worker threads for --functional (default: online CPUs)")
        .RequiredArgument("N").StoreResult(&funcThreads);
    opts.AddLongOption("seed", "PRNG seed for --functional (default: 1)")
        .RequiredArgument("N").StoreResult(&funcSeed);
    opts.AddLongOption("duration-s", "in-memory sweep duration, seconds (default: 10)")
        .RequiredArgument("SEC").StoreResult(&funcDurationS);
    opts.AddLongOption("output", "--functional: write a cross-machine comparison file")
        .RequiredArgument("FILE").StoreResult(&outputPath);
    opts.AddLongOption("output-size", "--output: target file size, GiB")
        .RequiredArgument("GIB").StoreResult(&outputSizeGiB);
    opts.AddLongOption("input", "--functional: verify an existing comparison file")
        .RequiredArgument("FILE").StoreResult(&inputPath);
    opts.AddLongOption("verify-data", "--input: also re-generate and compare raw PRNG data")
        .NoArgument().SetFlag(&verifyData);
    opts.AddLongOption("l1", "L1d size per core, KiB").RequiredArgument("KIB")
        .StoreResult(&params.L1KiB);
    opts.AddLongOption("l2", "L2 size per core, KiB").RequiredArgument("KIB")
        .StoreResult(&params.L2KiB);
    opts.AddLongOption("l3", "L3 size (shared), KiB").RequiredArgument("KIB")
        .StoreResult(&params.L3KiB);
    opts.AddLongOption("block-size", "bytes fed to one hash/encipher call")
        .RequiredArgument("BYTES").StoreResult(&params.BlockSize);
    opts.AddLongOption("left-cores", "physical cores (comma-separated)")
        .RequiredArgument("LIST").Handler1T<TString>([&](const TString& v) {
            leftCores = ParseCoreList(v);
        });
    opts.AddLongOption("right-cores", "HT siblings of left-cores (comma-separated)")
        .RequiredArgument("LIST").Handler1T<TString>([&](const TString& v) {
            rightCores = ParseCoreList(v);
        });
    opts.AddLongOption("run-ms", "duration of one timed sample, ms")
        .RequiredArgument("MS").StoreResult(&params.RunMs);
    opts.AddLongOption("samples", "samples per cell; median is reported")
        .RequiredArgument("N").StoreResult(&params.Samples);
    opts.AddLongOption("filter", "run only kernels whose name contains substr")
        .RequiredArgument("SUBSTR").StoreResult(&params.Filter);
    opts.AddLongOption("csv", "machine-readable output in addition to the table")
        .NoArgument().SetFlag(&csv);

    opts.SetFreeArgsNum(0);
    TOptsParseResult parsed(&opts, argc, argv);

    if (perf && functional) {
        Cerr << "--perf and --functional are mutually exclusive\n";
        return 1;
    }

    if (functional) {
        const ui32 threads = funcThreads != 0 ? funcThreads
            : static_cast<ui32>(NSystemInfo::CachedNumberOfCpus());
        return RunFunctional(
            !outputPath.empty(), !inputPath.empty(),
            outputPath, outputSizeGiB, inputPath, verifyData,
            threads, funcSeed, funcDurationS);
    }

    if (!perf) {
        return 0;
    }

    if (leftCores.empty()) {
        Cerr << "--left-cores is required for --perf\n";
        return 1;
    }
    if (!rightCores.empty() && rightCores.size() != leftCores.size()) {
        Cerr << "--right-cores must have the same length as --left-cores\n";
        return 1;
    }
    if (!IsPowerOfTwo(params.BlockSize) || params.BlockSize < 1024) {
        Cerr << "--block-size must be a power of two and >= 1024\n";
        return 1;
    }
    if (params.RunMs == 0 || params.Samples == 0) {
        Cerr << "--run-ms and --samples must be positive\n";
        return 1;
    }

    if (!rightCores.empty()) {
        VerifySiblingPairs(leftCores, rightCores);
    }

    if (!RunStartupSelfChecks()) {
        return 1;
    }
    if (!RunCipherSelfCheck()) {
        return 1;
    }
    if (!RunHashSelfCheck()) {
        return 1;
    }

    const TVector<const TKernelDesc*> kernels = GetAvailableKernels(params.Filter);
    if (kernels.empty()) {
        Cerr << "No kernels matched the filter / CPU features\n";
        return 1;
    }

    const bool hasRight = !rightCores.empty();
    const TVector<TScenarioDesc> scenarios = BuildScenarios(leftCores, rightCores, hasRight);
    if (scenarios.empty()) {
        Cerr << "No scenarios to run\n";
        return 1;
    }

    if (csv) {
        Cout << "scenario,kernel,level,threads,ws_per_thread_bytes,block_size,"
                "gbps_median,gbps_min,gbps_max,gbps_per_thread_avg\n";
    }

    for (const TScenarioDesc& scenario : scenarios) {
        RunScenario(scenario, params, kernels, csv);
    }

    if (hasRight) {
        RunMixedHtScenario(leftCores[0], rightCores[0], params, csv);
    } else {
        Cout << "Skipping mixed-ht scenario: --right-cores not provided\n";
    }

    return 0;
}

} // namespace

int main(int argc, char** argv) {
#if defined(_win_) || defined(_arm64_)
    // --perf relies on Linux-only affinity/pinning machinery; --functional
    // (equivalence sweep) still works here with a reduced variant set, and
    // file mode prints its own "not supported" message (ai_func.md §2).
    for (int i = 1; i < argc; ++i) {
        if (TString(argv[i]) == "--perf") {
            Cerr << "hash_test --perf is not supported on this platform\n";
            return 1;
        }
    }
#endif
    return RunPerf(argc, argv);
}
