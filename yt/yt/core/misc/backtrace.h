#pragma once

#include "public.h"

#include <library/cpp/yt/memory/range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TBacktraceView = TRange<const void*>;
using TBacktrace = std::vector<const void*>;

//! Gets the current backtrace.
TBacktrace GetBacktrace();

//! Gets backtrace and symbolizes it passing result into #callback.
void DumpBacktrace(
    const std::function<void(TStringBuf)>& writeCallback,
    void* startPC = nullptr);

//! Gets backtrace and symbolizes it into a string.
std::string DumpBacktrace();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
