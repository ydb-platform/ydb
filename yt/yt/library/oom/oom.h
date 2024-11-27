#pragma once

#include <optional>

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TOomWatchdogOptions
{
    std::optional<i64> MemoryLimit;
    TString HeapDumpPath = "oom.pb.gz";
};

void EnableEarlyOomWatchdog(TOomWatchdogOptions options);

////////////////////////////////////////////////////////////////////////////////

struct TTCMallocLimitHandlerOptions
{
    TString HeapDumpDirectory;

    // Files structure would have the following form:
    // HeapDumpDirectory/<ActualName>_FilenameSuffix_Timestamp.ext.
    std::optional<TString> FilenameSuffix;
    TDuration Timeout = TDuration::Minutes(5);
};

void EnableTCMallocLimitHandler(TTCMallocLimitHandlerOptions options);
void DisableTCMallocLimitHandler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
