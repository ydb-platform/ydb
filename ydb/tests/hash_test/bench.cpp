#include "bench.h"

#include <ydb/core/blobstorage/crypto/chacha.h>
#if !(defined(_win_) || defined(_arm64_))
#include <ydb/core/blobstorage/crypto/chacha_vec.h>
#include <ydb/core/blobstorage/crypto/chacha_512/chacha_512.h>
#include "xxHash/xxh_x86dispatch.h"
#endif

#include <ydb/library/actors/util/affinity.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/align.h>
#include <util/system/cpu_id.h>
#include <util/system/hp_timer.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

static const TKernelDesc* const AllKernels[] = {
    &KernelMemRead,
#if !(defined(_win_) || defined(_arm64_))
    &KernelMemReadAvx2,
#endif
    &KernelChaChaInt,
#if !(defined(_win_) || defined(_arm64_))
    &KernelChaChaSse,
    &KernelChaChaAvx512,
    &KernelXxhSse2,
    &KernelXxhAvx2,
#endif
#if !defined(_win_)
    &KernelXxhScalar,
#endif
#if defined(_arm64_) && !defined(_win_)
    &KernelXxhNeon,
#endif
    &KernelCity64,
};

namespace {

constexpr double ResidencyFactor = 0.5;
constexpr size_t BufferAlign = 4096;

struct alignas(128) TWorkerSlot {
    ui8* Data = nullptr;
    ui8* RawData = nullptr;
    void* Ctx = nullptr;
    ui64 Sink = 0;
    ui64 Chunks = 0;
    // Calibrated by this thread for whatever kernel it is currently running
    // (spec ai.md §9a): mixed-kernel cells pair kernels whose rates can
    // differ by more than 10x, so chunkPasses can never be shared across
    // threads the way it used to be for homogeneous cells.
    ui64 ChunkPasses = 0;
    double Seconds = 0;
    ui32 CpuId = 0;
    char Pad[128 - 60];
};

static_assert(sizeof(TWorkerSlot) == 128);

void FillBuffer(ui8* data, size_t size) {
    ui64 seed = 0x9E3779B97F4A7C15ULL;
    for (size_t i = 0; i < size; ++i) {
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        data[i] = static_cast<ui8>(seed * 0x2545F4914F6CDD1DULL);
    }
}

ui8* AllocateAlignedBuffer(size_t size, ui8** rawOut) {
    ui8* raw = new ui8[size + BufferAlign];
    ui8* aligned = reinterpret_cast<ui8*>(AlignUp<intptr_t>(intptr_t(raw), BufferAlign));
    *rawOut = raw;
    return aligned;
}

void FreeAlignedBuffer(ui8* raw) {
    delete[] raw;
}

void PinThread(ui32 cpuId) {
    const ui32 maskSize = cpuId + 1;
    TVector<ui8> mask(maskSize, 0);
    mask[cpuId] = 1;
    TAffinity(mask.data(), maskSize).Set();
}

void ArriveAndWait(
    std::atomic<ui32>& counter,
    std::atomic<ui32>& generation,
    ui32 threadCount)
{
    const ui32 gen = generation.load(std::memory_order_acquire);
    if (counter.fetch_add(1, std::memory_order_acq_rel) + 1 == threadCount) {
        counter.store(0, std::memory_order_release);
        generation.fetch_add(1, std::memory_order_release);
    } else {
        while (generation.load(std::memory_order_acquire) == gen) {
            SpinLockPause();
        }
    }
}

// Every worker runs this concurrently for ~100ms (spec §8): this both warms
// the thread's own working set and calibrates under realistic SMT/cache
// contention. Every thread derives its own chunkPasses from its own measured
// rate (ai.md §9a): in mixed-kernel cells each thread runs a different
// kernel, so a single shared rate would be wrong for one of the two sides.
void WarmupAndCalibrate(
    const TKernelDesc* kernel,
    ui8* data,
    size_t wsSize,
    size_t blockSize,
    void* ctx,
    ui64* outChunkPasses)
{
    THPTimer timer;
    ui64 passes = 0;
    while (timer.Passed() < 0.1) {
        kernel->Run(data, wsSize, blockSize, 1, ctx);
        ++passes;
    }
    const double rate = passes / timer.Passed();
    *outChunkPasses = Max<ui64>(1ULL, static_cast<ui64>(rate * 100e-6));
}

double Median(TVector<double> values) {
    if (values.empty()) {
        return 0;
    }
    Sort(values.begin(), values.end());
    const size_t mid = values.size() / 2;
    if (values.size() % 2 == 1) {
        return values[mid];
    }
    return (values[mid - 1] + values[mid]) / 2.0;
}

bool KernelSupported(const TKernelDesc* kernel) {
    return !kernel->IsSupported || kernel->IsSupported();
}

TString FormatKiB(size_t bytes) {
    if (bytes >= 1024 * 1024) {
        return ToString(bytes / (1024 * 1024)) + " MiB";
    }
    return ToString(bytes / 1024) + " KiB";
}

// Per-thread median/min/max GB/s across samples (ai.md §9a): mixed-kernel
// cells must report each side separately, never summed, since the two
// threads run different kernels.
struct TPerThreadStats {
    double Median = 0;
    double Min = 0;
    double Max = 0;
};

struct TScenarioRunner {
    TVector<ui32> Cores;
    TVector<TWorkerSlot> Slots;
    TVector<std::thread> Threads;

    size_t MaxWs = 0;
    size_t BlockSize = 0;

    std::mutex JobMutex;
    std::condition_variable JobCv;
    bool Shutdown = false;
    bool CellDone = false;
    // Bumped by Main under JobMutex for every new job (RunCell call). Workers
    // remember the last epoch they processed and only start new work when
    // JobEpoch has actually advanced, so a worker that loops back to the top
    // of its wait faster than others can never spuriously re-run the job
    // that just completed (which previously desynced the barrier and hung).
    ui64 JobEpoch = 0;

    // One kernel per worker thread (ai.md §9a): homogeneous cells fill this
    // with the same pointer for every slot; mixed cells put a different
    // kernel per SMT sibling. Size always equals Cores.size().
    TVector<const TKernelDesc*> JobKernels;
    size_t JobWsSize = 0;
    ui32 JobSamples = 0;

    // Worker-only, self-resetting barrier: only the Cores.size() worker
    // threads ever call ArriveAndWait / touch these two atomics. The main
    // thread must never write to them directly.
    alignas(128) std::atomic<ui32> BarrierCounter{0};
    alignas(128) std::atomic<ui32> BarrierGeneration{0};

    alignas(128) std::atomic<bool> Stop{false};
    alignas(128) std::atomic<ui32> WorkersFinished{0};

    std::atomic<ui32> SampleToRun{0};
    std::atomic<bool> Calibrated{false};

    void WorkerMain(ui32 threadIdx) {
        TWorkerSlot& slot = Slots[threadIdx];
        slot.CpuId = Cores[threadIdx];
        PinThread(slot.CpuId);
        slot.Data = AllocateAlignedBuffer(MaxWs, &slot.RawData);
        FillBuffer(slot.Data, MaxWs);

        const TKernelDesc* activeKernel = nullptr;
        void* ctx = nullptr;
        ui64 lastEpoch = 0;

        while (true) {
            std::unique_lock<std::mutex> lock(JobMutex);
            JobCv.wait(lock, [&]() { return Shutdown || JobEpoch != lastEpoch; });
            if (Shutdown) {
                break;
            }
            lastEpoch = JobEpoch;

            const TKernelDesc* kernel = JobKernels[threadIdx];
            const size_t wsSize = JobWsSize;
            const ui32 samples = JobSamples;
            lock.unlock();

            // Ctx is recreated only when the kernel itself changes (not on
            // every working-set size), since kernel state is independent of
            // wsSize and buffers are sized to the scenario's max ws anyway.
            if (kernel != activeKernel) {
                if (activeKernel && activeKernel->DestroyCtx && ctx) {
                    activeKernel->DestroyCtx(ctx);
                    ctx = nullptr;
                }
                activeKernel = kernel;
                if (kernel->CreateCtx) {
                    ctx = kernel->CreateCtx();
                }
            }

            WarmupAndCalibrate(
                kernel, slot.Data, wsSize, BlockSize, ctx, &slot.ChunkPasses);

            ArriveAndWait(BarrierCounter, BarrierGeneration, Cores.size());
            if (threadIdx == 0) {
                Calibrated.store(true, std::memory_order_release);
            }

            for (ui32 sample = 1; sample <= samples; ++sample) {
                while (SampleToRun.load(std::memory_order_acquire) < sample) {
                    SpinLockPause();
                }

                // Start-of-sample barrier: workers only, ensures the timed
                // loop begins truly simultaneously across threads.
                ArriveAndWait(BarrierCounter, BarrierGeneration, Cores.size());

                THPTimer timer;
                slot.Chunks = 0;
                slot.Sink = 0;
                const ui64 chunkPasses = slot.ChunkPasses;
                while (!Stop.load(std::memory_order_relaxed)) {
                    slot.Sink ^= kernel->Run(
                        slot.Data, wsSize, BlockSize, chunkPasses, ctx);
                    ++slot.Chunks;
                }
                slot.Seconds = timer.Passed();

                WorkersFinished.fetch_add(1, std::memory_order_release);
            }

            if (threadIdx == 0) {
                std::lock_guard<std::mutex> doneLock(JobMutex);
                CellDone = true;
                JobCv.notify_all();
            }
        }

        if (activeKernel && activeKernel->DestroyCtx && ctx) {
            activeKernel->DestroyCtx(ctx);
        }
        if (slot.RawData) {
            FreeAlignedBuffer(slot.RawData);
            slot.RawData = nullptr;
            slot.Data = nullptr;
        }
    }

    void Start(const TVector<ui32>& cores, size_t maxWs, size_t blockSize) {
        Cores = cores;
        MaxWs = maxWs;
        BlockSize = blockSize;
        Slots.resize(cores.size());
        for (ui32 i = 0; i < cores.size(); ++i) {
            Threads.emplace_back([this, i]() { WorkerMain(i); });
        }
    }

    void StopRunner() {
        {
            std::lock_guard<std::mutex> lock(JobMutex);
            Shutdown = true;
        }
        JobCv.notify_all();
        for (auto& t : Threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        Threads.clear();
        Slots.clear();
    }

    // Core primitive: runs one cell where each worker thread executes its
    // own kernel (kernels.size() must equal Cores.size()). Homogeneous
    // scenarios call this with the same kernel repeated in every slot via
    // RunCell() below; the mixed-HT scenario (ai.md §9a) passes a different
    // kernel per SMT sibling and reads perThreadOut to report each side's
    // bandwidth separately instead of summing them.
    TCellResult RunCellMulti(
        const TVector<const TKernelDesc*>& kernels,
        size_t wsSize,
        ui32 runMs,
        ui32 samples,
        TVector<TPerThreadStats>* perThreadOut = nullptr)
    {
        TCellResult result;
        result.WsPerThreadBytes = wsSize;
        result.Ran = true;

        Calibrated.store(false, std::memory_order_release);
        SampleToRun.store(0, std::memory_order_release);

        {
            std::lock_guard<std::mutex> lock(JobMutex);
            JobKernels = kernels;
            JobWsSize = wsSize;
            JobSamples = samples;
            CellDone = false;
            ++JobEpoch;
        }
        JobCv.notify_all();

        // Calibration takes ~100ms; sleep rather than busy-spin so the OS
        // doesn't have a reason to schedule this (unpinned) thread onto one
        // of the benchmark cores and skew the measured rate that chunkPasses
        // is derived from. This only affects chunk sizing, never results.
        while (!Calibrated.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }

        TVector<double> sampleTotals;
        sampleTotals.reserve(samples);
        TVector<TVector<double>> perThreadSamples(Cores.size());

        for (ui32 sample = 1; sample <= samples; ++sample) {
            // Main never touches BarrierCounter/BarrierGeneration: that
            // barrier is worker-only and self-resetting. Main only drives
            // sample progression via SampleToRun/Stop and waits for
            // completion via WorkersFinished.
            WorkersFinished.store(0, std::memory_order_release);
            Stop.store(false, std::memory_order_release);
            SampleToRun.store(sample, std::memory_order_release);

            std::this_thread::sleep_for(std::chrono::milliseconds(runMs));
            Stop.store(true, std::memory_order_release);

            while (WorkersFinished.load(std::memory_order_acquire) <
                   static_cast<ui32>(Cores.size())) {
                SpinLockPause();
            }

            double totalGbps = 0;
            for (size_t i = 0; i < Slots.size(); ++i) {
                const TWorkerSlot& slot = Slots[i];
                const double bytes = static_cast<double>(slot.Chunks) *
                    static_cast<double>(slot.ChunkPasses) *
                    static_cast<double>(wsSize);
                const double gbps = slot.Seconds > 0 ? bytes / slot.Seconds / 1e9 : 0;
                totalGbps += gbps;
                perThreadSamples[i].push_back(gbps);
            }
            sampleTotals.push_back(totalGbps);
        }

        {
            std::unique_lock<std::mutex> lock(JobMutex);
            JobCv.wait(lock, [&]() { return CellDone; });
        }

        result.GbpsMedian = Median(sampleTotals);
        result.GbpsMin = *MinElement(sampleTotals.begin(), sampleTotals.end());
        result.GbpsMax = *MaxElement(sampleTotals.begin(), sampleTotals.end());
        result.GbpsPerThreadAvg = result.GbpsMedian / Cores.size();

        if (perThreadOut) {
            perThreadOut->resize(Cores.size());
            for (size_t i = 0; i < Cores.size(); ++i) {
                (*perThreadOut)[i].Median = Median(perThreadSamples[i]);
                (*perThreadOut)[i].Min =
                    *MinElement(perThreadSamples[i].begin(), perThreadSamples[i].end());
                (*perThreadOut)[i].Max =
                    *MaxElement(perThreadSamples[i].begin(), perThreadSamples[i].end());
            }
        }

        return result;
    }

    TCellResult RunCell(
        const TKernelDesc* kernel,
        size_t wsSize,
        ui32 runMs,
        ui32 samples)
    {
        return RunCellMulti(
            TVector<const TKernelDesc*>(Cores.size(), kernel), wsSize, runMs, samples);
    }
};

} // namespace

const char* CacheLevelName(ECacheLevel level) {
    switch (level) {
    case ECacheLevel::L1:
        return "L1";
    case ECacheLevel::L2:
        return "L2";
    case ECacheLevel::L3:
        return "L3";
    case ECacheLevel::RAM:
        return "RAM";
    }
    return "?";
}

TVector<const TKernelDesc*> GetAvailableKernels(const TString& filter) {
    TVector<const TKernelDesc*> out;
    for (const TKernelDesc* kernel : AllKernels) {
        if (!KernelSupported(kernel)) {
            Cout << "Skipping unsupported kernel: " << kernel->Name << '\n';
            continue;
        }
        if (!filter.empty() && !TString(kernel->Name).Contains(filter)) {
            continue;
        }
        out.push_back(kernel);
    }
    return out;
}

TWsSizes ComputeWsSizes(
    const TScenarioDesc& scenario,
    const TBenchParams& params,
    TVector<TString>* warnings)
{
    TWsSizes ws;
    const ui32 threadCount = Max<ui32>(1, scenario.Cores.size());
    const ui32 l1Div = scenario.SharesPhysicalCore ? 2 : 1;
    const ui32 l2Div = scenario.SharesPhysicalCore ? 2 : 1;

    auto roundWs = [&](size_t raw) -> size_t {
        size_t value = AlignDown(raw, params.BlockSize);
        if (value < params.BlockSize) {
            if (warnings) {
                warnings->push_back(
                    "working set rounded down below block size; clamping to block size");
            }
            value = params.BlockSize;
        }
        return value;
    };

    ws.L1 = roundWs(static_cast<size_t>(params.L1KiB * 1024 * ResidencyFactor / l1Div));
    ws.L2 = roundWs(static_cast<size_t>(params.L2KiB * 1024 * ResidencyFactor / l2Div));
    ws.L3 = roundWs(static_cast<size_t>(params.L3KiB * 1024 * ResidencyFactor / threadCount));
    ws.RAM = roundWs(4 * params.L3KiB * 1024);
    return ws;
}

size_t WsBytesForLevel(ECacheLevel level, const TWsSizes& ws) {
    switch (level) {
    case ECacheLevel::L1:
        return ws.L1;
    case ECacheLevel::L2:
        return ws.L2;
    case ECacheLevel::L3:
        return ws.L3;
    case ECacheLevel::RAM:
        return ws.RAM;
    }
    return 0;
}

namespace {

constexpr size_t KernelNameColumnWidth = 16;
constexpr size_t ValueColumnWidth = 12;

static const ECacheLevel AllLevels[] = {
    ECacheLevel::L1,
    ECacheLevel::L2,
    ECacheLevel::L3,
    ECacheLevel::RAM,
};

void PrintWsWarnings(const TVector<TString>& warnings) {
    for (const auto& warning : warnings) {
        Cout << "Warning: " << warning << '\n';
    }
}

void PrintScenarioHeader(const TScenarioDesc& scenario, const TWsSizes& wsSizes) {
    Cout << "=== Scenario " << scenario.Name << ": cores {";
    for (size_t i = 0; i < scenario.Cores.size(); ++i) {
        if (i) {
            Cout << ", ";
        }
        Cout << scenario.Cores[i];
    }
    Cout << "} (" << scenario.Cores.size() << " threads";
    if (scenario.SharesPhysicalCore) {
        Cout << ", SMT siblings";
    }
    Cout << ") ===\n";

    Cout << "per-thread working set: L1=" << FormatKiB(wsSizes.L1)
         << "  L2=" << FormatKiB(wsSizes.L2)
         << "  L3=" << FormatKiB(wsSizes.L3)
         << "  RAM=" << FormatKiB(wsSizes.RAM) << "\n";

    TString header = Sprintf("%-*s", static_cast<int>(KernelNameColumnWidth), "kernel");
    for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
        header += Sprintf("%*s", static_cast<int>(ValueColumnWidth), CacheLevelName(AllLevels[li]));
    }
    Cout << header << '\n';
    Cout.Flush();
}

// Prints the human-readable row unconditionally and, when csv is set,
// additionally appends the machine-readable CSV lines for this kernel
// (spec §12: --csv is additive, not a replacement for the table).
void PrintKernelRow(
    const TScenarioDesc& scenario,
    const TKernelDesc* kernel,
    const TVector<TCellResult>& cells,
    bool csv,
    size_t blockSize)
{
    TString row = Sprintf("%-*s", static_cast<int>(KernelNameColumnWidth), kernel->Name);
    for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
        row += Sprintf("%*.1f", static_cast<int>(ValueColumnWidth), cells[li].GbpsMedian);
    }
    Cout << row << '\n';

    if (csv) {
        for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
            const TCellResult& cell = cells[li];
            Cout << scenario.Name << ','
                 << kernel->Name << ','
                 << CacheLevelName(AllLevels[li]) << ','
                 << scenario.Cores.size() << ','
                 << cell.WsPerThreadBytes << ','
                 << blockSize << ','
                 << cell.GbpsMedian << ','
                 << cell.GbpsMin << ','
                 << cell.GbpsMax << ','
                 << cell.GbpsPerThreadAvg << '\n';
        }
    }
    Cout.Flush();
}

} // namespace

void RunScenario(
    const TScenarioDesc& scenario,
    const TBenchParams& params,
    const TVector<const TKernelDesc*>& kernels,
    bool csv)
{
    TVector<TString> warnings;
    const TWsSizes wsSizes = ComputeWsSizes(scenario, params, &warnings);
    PrintWsWarnings(warnings);

    const size_t maxWs = Max(
        Max(wsSizes.L1, wsSizes.L2),
        Max(wsSizes.L3, wsSizes.RAM));

    TScenarioRunner runner;
    runner.Start(scenario.Cores, maxWs, params.BlockSize);

    PrintScenarioHeader(scenario, wsSizes);

    for (const TKernelDesc* kernel : kernels) {
        TVector<TCellResult> cells(Y_ARRAY_SIZE(AllLevels));
        for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
            const size_t ws = WsBytesForLevel(AllLevels[li], wsSizes);
            cells[li] = runner.RunCell(kernel, ws, params.RunMs, params.Samples);
        }
        PrintKernelRow(scenario, kernel, cells, csv, params.BlockSize);
    }

    runner.StopRunner();
}

namespace {

// Integer "victim" paired with a vector "aggressor" on the two SMT siblings
// of one physical core (ai.md §9a). Kept as a small data-driven table so new
// pairs are a one-line addition. Guarded the same way as the kernels
// themselves: the vector kernels (and xxh3-scalar, which only exists to give
// the xxh3 family an integer reference for this table) are only compiled on
// non-Windows, non-arm64 platforms.
struct TMixedPair {
    const TKernelDesc* Victim;
    const TKernelDesc* Aggressor;
};

static const TMixedPair MixedPairs[] = {
#if !(defined(_win_) || defined(_arm64_))
    {&KernelChaChaInt, &KernelChaChaSse},
    {&KernelChaChaInt, &KernelChaChaAvx512},
    {&KernelXxhScalar, &KernelXxhSse2},
    {&KernelXxhScalar, &KernelXxhAvx2},
#endif
};

bool PairMatchesFilter(const TMixedPair& pair, const TString& filter) {
    if (filter.empty()) {
        return true;
    }
    return TString(pair.Victim->Name).Contains(filter) ||
           TString(pair.Aggressor->Name).Contains(filter);
}

constexpr size_t MixedLabelColumnWidth = 30;
constexpr size_t MixedValueColumnWidth = 16;

void PrintMixedHtHeader(ui32 leftCore, ui32 rightCore, const TWsSizes& wsSizes) {
    Cout << "=== Mixed HT scenario: cores {" << leftCore << ", " << rightCore
         << "} (victim l0 + aggressor r0, SMT siblings) ===\n";
    Cout << "per-thread working set: L1=" << FormatKiB(wsSizes.L1)
         << "  L2=" << FormatKiB(wsSizes.L2)
         << "  L3=" << FormatKiB(wsSizes.L3)
         << "  RAM=" << FormatKiB(wsSizes.RAM) << "\n";

    TString header = Sprintf("%-*s", static_cast<int>(MixedLabelColumnWidth), "pair / side");
    for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
        header += Sprintf("%*s", static_cast<int>(MixedValueColumnWidth), CacheLevelName(AllLevels[li]));
    }
    Cout << header << '\n';
    Cout.Flush();
}

void PrintMixedHtCsvHeader() {
    Cout << "scenario,kernel,level,threads,ws_per_thread_bytes,block_size,"
            "gbps_median,gbps_min,gbps_max,sibling_kernel,solo_gbps,retention\n";
}

// One row = one side of one pair: this side's own contended bandwidth plus
// its retention (mixed / solo) at the same working set. The two sides of a
// pair are never summed (ai.md §9a) -- they ran different kernels.
void PrintMixedRow(
    const TKernelDesc* kernel,
    const TKernelDesc* sibling,
    ui32 threadCount,
    const TVector<size_t>& wsPerLevel,
    size_t blockSize,
    const TVector<TPerThreadStats>& stats,
    const TVector<double>& soloGbps,
    bool csv)
{
    const TString label = Sprintf("%s (+ %s)", kernel->Name, sibling->Name);
    TString row = Sprintf("%-*s", static_cast<int>(MixedLabelColumnWidth), label.c_str());

    TVector<double> retention(Y_ARRAY_SIZE(AllLevels));
    for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
        retention[li] = soloGbps[li] > 0 ? stats[li].Median / soloGbps[li] : 0.0;
        const TString cell = Sprintf("%.1f (%3.0f%%)", stats[li].Median, retention[li] * 100.0);
        row += Sprintf("%*s", static_cast<int>(MixedValueColumnWidth), cell.c_str());
    }
    Cout << row << '\n';

    if (csv) {
        for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
            Cout << "mixed-ht" << ','
                 << kernel->Name << ','
                 << CacheLevelName(AllLevels[li]) << ','
                 << threadCount << ','
                 << wsPerLevel[li] << ','
                 << blockSize << ','
                 << stats[li].Median << ','
                 << stats[li].Min << ','
                 << stats[li].Max << ','
                 << sibling->Name << ','
                 << soloGbps[li] << ','
                 << retention[li] << '\n';
        }
    }
    Cout.Flush();
}

} // namespace

void RunMixedHtScenario(
    ui32 leftCore,
    ui32 rightCore,
    const TBenchParams& params,
    bool csv)
{
    TVector<TMixedPair> pairs;
    for (const TMixedPair& pair : MixedPairs) {
        if (!KernelSupported(pair.Victim) || !KernelSupported(pair.Aggressor)) {
            Cout << "Skipping mixed-ht pair " << pair.Victim->Name << " + "
                 << pair.Aggressor->Name << ": unsupported on this CPU\n";
            continue;
        }
        if (!PairMatchesFilter(pair, params.Filter)) {
            continue;
        }
        pairs.push_back(pair);
    }

    if (pairs.empty()) {
        Cout << "Skipping mixed-ht scenario: no kernel pairs available\n";
        return;
    }

    // Same working-set formula as 2T-ht (halved L1/L2, L3 split across the
    // two threads): both sides of a pair must use an identical footprint or
    // the cache columns stop being comparable.
    const TScenarioDesc scenarioDesc{"mixed-ht", {leftCore, rightCore}, true};
    TVector<TString> warnings;
    const TWsSizes wsSizes = ComputeWsSizes(scenarioDesc, params, &warnings);
    PrintWsWarnings(warnings);

    TVector<const TKernelDesc*> distinctKernels;
    auto addDistinct = [&](const TKernelDesc* k) {
        for (const TKernelDesc* existing : distinctKernels) {
            if (existing == k) {
                return;
            }
        }
        distinctKernels.push_back(k);
    };
    for (const TMixedPair& pair : pairs) {
        addDistinct(pair.Victim);
        addDistinct(pair.Aggressor);
    }
    auto kernelIndex = [&](const TKernelDesc* k) -> size_t {
        for (size_t i = 0; i < distinctKernels.size(); ++i) {
            if (distinctKernels[i] == k) {
                return i;
            }
        }
        return 0;
    };

    const size_t maxWs = Max(
        Max(wsSizes.L1, wsSizes.L2),
        Max(wsSizes.L3, wsSizes.RAM));

    TVector<size_t> wsPerLevel(Y_ARRAY_SIZE(AllLevels));
    for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
        wsPerLevel[li] = WsBytesForLevel(AllLevels[li], wsSizes);
    }

    // Solo baselines: each distinct kernel run alone on leftCore, at the
    // *mixed* (halved) working set -- the homogeneous 1T table uses
    // full-cache sizes and would give a meaningless retention denominator.
    // Cached per (kernel, level) so a kernel shared by multiple pairs (e.g.
    // chacha-int, xxh3-scalar) is only measured once.
    TVector<TVector<double>> soloGbps(
        distinctKernels.size(), TVector<double>(Y_ARRAY_SIZE(AllLevels), 0.0));
    {
        TScenarioRunner soloRunner;
        soloRunner.Start({leftCore}, maxWs, params.BlockSize);
        for (size_t ki = 0; ki < distinctKernels.size(); ++ki) {
            for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
                const TCellResult r = soloRunner.RunCell(
                    distinctKernels[ki], wsPerLevel[li], params.RunMs, params.Samples);
                soloGbps[ki][li] = r.GbpsMedian;
            }
        }
        soloRunner.StopRunner();
    }

    PrintMixedHtHeader(leftCore, rightCore, wsSizes);
    if (csv) {
        PrintMixedHtCsvHeader();
    }

    TScenarioRunner mixedRunner;
    mixedRunner.Start({leftCore, rightCore}, maxWs, params.BlockSize);

    for (const TMixedPair& pair : pairs) {
        TVector<TPerThreadStats> victimStats(Y_ARRAY_SIZE(AllLevels));
        TVector<TPerThreadStats> aggressorStats(Y_ARRAY_SIZE(AllLevels));

        for (size_t li = 0; li < Y_ARRAY_SIZE(AllLevels); ++li) {
            const TVector<const TKernelDesc*> kernels = {pair.Victim, pair.Aggressor};
            TVector<TPerThreadStats> perThread;
            mixedRunner.RunCellMulti(
                kernels, wsPerLevel[li], params.RunMs, params.Samples, &perThread);
            victimStats[li] = perThread[0];
            aggressorStats[li] = perThread[1];
        }

        PrintMixedRow(
            pair.Victim, pair.Aggressor, 2, wsPerLevel, params.BlockSize,
            victimStats, soloGbps[kernelIndex(pair.Victim)], csv);
        PrintMixedRow(
            pair.Aggressor, pair.Victim, 2, wsPerLevel, params.BlockSize,
            aggressorStats, soloGbps[kernelIndex(pair.Aggressor)], csv);
    }

    mixedRunner.StopRunner();
}

bool RunStartupSelfChecks() {
    Cout << "CPU features: SSE2=" << (NX86::CachedHaveSSE2() ? "yes" : "no")
         << " AVX2=" << (NX86::CachedHaveAVX2() ? "yes" : "no")
         << " AVX512F=" << (NX86::CachedHaveAVX512F() ? "yes" : "no") << '\n';

    Cout << "Kernels:";
    for (const TKernelDesc* kernel : AllKernels) {
        const bool supported = KernelSupported(kernel);
        Cout << ' ' << kernel->Name << (supported ? "" : "(skip)");
    }
    Cout << '\n';

    return true;
}

const TKernelDesc* FindKernelByName(const char* name) {
    for (const TKernelDesc* kernel : AllKernels) {
        if (kernel->Name && strcmp(kernel->Name, name) == 0) {
            return kernel;
        }
    }
    return nullptr;
}

bool RunCipherSelfCheck() {
#if defined(_win_) || defined(_arm64_)
    Cout << "Cipher self-check skipped on this platform\n";
    return true;
#else
    alignas(16) static const ui8 Key[32] = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    };
    static const ui8 Iv[8] = {};

    constexpr size_t Len = 64 * 1024;
    TVector<ui8> plain(Len);
    FillBuffer(plain.data(), Len);

    TVector<ui8> outInt(Len);
    TVector<ui8> outSse(Len);
    TVector<ui8> out512(Len);

    {
        ChaCha cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(plain.data(), outInt.data(), Len);
    }
    {
        ChaChaVec cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(plain.data(), outSse.data(), Len);
    }
    if (memcmp(outInt.data(), outSse.data(), Len) != 0) {
        Cerr << "Cipher self-check failed: ChaCha vs ChaChaVec mismatch\n";
        return false;
    }

    const bool haveAvx512 = NX86::CachedHaveAVX512F();
    if (haveAvx512) {
        ChaCha512 cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(plain.data(), out512.data(), Len);
        if (memcmp(outInt.data(), out512.data(), Len) != 0) {
            Cerr << "Cipher self-check failed: ChaCha vs ChaCha512 mismatch\n";
            return false;
        }
    }

    // In-place vs out-of-place check (§11), for every available implementation.
    {
        TVector<ui8> inPlace = plain;
        ChaCha cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(inPlace.data(), inPlace.data(), Len);
        if (memcmp(inPlace.data(), outInt.data(), Len) != 0) {
            Cerr << "Cipher self-check failed: ChaCha in-place vs out-of-place mismatch\n";
            return false;
        }
    }
    {
        TVector<ui8> inPlace = plain;
        ChaChaVec cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(inPlace.data(), inPlace.data(), Len);
        if (memcmp(inPlace.data(), outSse.data(), Len) != 0) {
            Cerr << "Cipher self-check failed: ChaChaVec in-place vs out-of-place mismatch\n";
            return false;
        }
    }
    if (haveAvx512) {
        TVector<ui8> inPlace = plain;
        ChaCha512 cipher;
        cipher.SetKey(Key, 32);
        cipher.SetIV(Iv);
        cipher.Encipher(inPlace.data(), inPlace.data(), Len);
        if (memcmp(inPlace.data(), out512.data(), Len) != 0) {
            Cerr << "Cipher self-check failed: ChaCha512 in-place vs out-of-place mismatch\n";
            return false;
        }
    }

    Cout << "Cipher self-check passed\n";
    return true;
#endif
}

bool RunHashSelfCheck() {
#if defined(_win_) || defined(_arm64_)
    Cout << "Hash self-check skipped on this platform\n";
    return true;
#else
    constexpr size_t Len = 64 * 1024;
    TVector<ui8> buf(Len);
    FillBuffer(buf.data(), Len);

    const TKernelDesc* sse2 = FindKernelByName("xxh3-sse2");
    if (!sse2 || !KernelSupported(sse2)) {
        Cerr << "Hash self-check failed: xxh3-sse2 unavailable\n";
        return false;
    }

    void* ctx = sse2->CreateCtx();
    const ui64 sse2Hash = sse2->Run(buf.data(), Len, Len, 1, ctx);
    sse2->DestroyCtx(ctx);

    const TKernelDesc* avx2 = FindKernelByName("xxh3-avx2");
    if (avx2 && KernelSupported(avx2)) {
        ctx = avx2->CreateCtx();
        const ui64 avx2Hash = avx2->Run(buf.data(), Len, Len, 1, ctx);
        avx2->DestroyCtx(ctx);
        if (avx2Hash != sse2Hash) {
            Cerr << "Hash self-check failed: xxh3-sse2 vs xxh3-avx2 mismatch\n";
            return false;
        }
    }

    const ui64 dispatchHash = XXH3_64bits_dispatch(buf.data(), Len);
    if (dispatchHash != sse2Hash) {
        Cerr << "Hash self-check failed: xxh3-sse2 vs dispatch mismatch\n";
        return false;
    }

    Cout << "Hash self-check passed\n";
    return true;
#endif
}
