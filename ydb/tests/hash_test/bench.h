#pragma once

#include "kernels.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

enum class ECacheLevel {
    L1,
    L2,
    L3,
    RAM,
};

const char* CacheLevelName(ECacheLevel level);

struct TScenarioDesc {
    TString Name;
    TVector<ui32> Cores;
    bool SharesPhysicalCore = false;
};

struct TBenchParams {
    size_t L1KiB = 48;
    size_t L2KiB = 1280;
    size_t L3KiB = 49152;
    size_t BlockSize = 4096;
    ui32 RunMs = 500;
    ui32 Samples = 3;
    TString Filter;
};

struct TWsSizes {
    size_t L1 = 0;
    size_t L2 = 0;
    size_t L3 = 0;
    size_t RAM = 0;
};

struct TCellResult {
    double GbpsMedian = 0;
    double GbpsMin = 0;
    double GbpsMax = 0;
    double GbpsPerThreadAvg = 0;
    size_t WsPerThreadBytes = 0;
    bool Ran = false;
};

TVector<const TKernelDesc*> GetAvailableKernels(const TString& filter);

TWsSizes ComputeWsSizes(
    const TScenarioDesc& scenario,
    const TBenchParams& params,
    TVector<TString>* warnings = nullptr);

size_t WsBytesForLevel(ECacheLevel level, const TWsSizes& ws);

// Runs all (kernel x cache level) cells for one scenario, printing the
// scenario header up front and each kernel's row as soon as it is available
// (spec §12: partial results survive interruption). When csv is set, CSV
// lines are printed in addition to the human-readable row for that kernel.
void RunScenario(
    const TScenarioDesc& scenario,
    const TBenchParams& params,
    const TVector<const TKernelDesc*>& kernels,
    bool csv);

// Mixed-kernel HT scenario (ai.md §9a): runs an integer implementation and a
// vector implementation simultaneously on the two SMT siblings leftCore /
// rightCore, and reports each side's bandwidth (and its retention vs a solo
// baseline at the same working set) in a table separate from RunScenario's
// homogeneous output. Skips (with a printed notice) if no kernel pair from
// the fixed table is both supported and matches params.Filter.
void RunMixedHtScenario(
    ui32 leftCore,
    ui32 rightCore,
    const TBenchParams& params,
    bool csv);

bool RunStartupSelfChecks();
bool RunCipherSelfCheck();
bool RunHashSelfCheck();
