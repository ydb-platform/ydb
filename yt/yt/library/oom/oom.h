#pragma once

#include <optional>

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TOOMOptions
{
    std::optional<i64> MemoryLimit;
    TString HeapDumpPath = "oom.pb.gz";
};

void EnableEarlyOOMWatchdog(TOOMOptions options);

////////////////////////////////////////////////////////////////////////////////

struct TTCMallocLimitHandlerOptions
{
    TString HeapDumpDirectory;
    TDuration Timeout = TDuration::Seconds(300);
};

void EnableTCMallocLimitHandler(TTCMallocLimitHandlerOptions options);
void DisableTCMallocLimitHandler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
