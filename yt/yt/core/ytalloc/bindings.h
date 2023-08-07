#pragma once

#include "public.h"

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

// Installs YTAlloc log handler that pushes event to YT logging infrastructure.
void EnableYTLogging();

// Enables periodic push of YTAlloc statistics to YT profiling infrastructure.
void EnableYTProfiling(const TYTProfilingConfigPtr& config = nullptr);

// Installs backtrace provider that invokes libunwind.
void InitializeLibunwindInterop();

// Configures YTAlloc from a given #config instance.
void Configure(const TYTAllocConfigPtr& config);

// Configures YTAlloc from |YT_ALLOC_CONFIG| environment variable.
// Never throws on error; just reports it via logging.
// Returns |true| if YTAlloc was successfully configured from the variable;
// |false| otherwise (the variable did not exist or could not be parsed).
bool ConfigureFromEnv();

//! Returns |true| if the current configuration is set by a successful
//! call to #ConfigureFromEnv.
bool IsConfiguredFromEnv();

// Builds a string containing some brief allocation statistics.
TString FormatAllocationCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
