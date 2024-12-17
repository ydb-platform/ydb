#pragma once

#include <optional>

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TOomWatchdogOptions
{
    std::optional<i64> MemoryLimit;
    TString HeapDumpPath = "oom.pb.gz";
};

void EnableEarlyOomWatchdog(TOomWatchdogOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
