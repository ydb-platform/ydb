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
template <class TCallback>
void DumpBacktrace(TCallback callback, void* startPC = nullptr);

//! Gets backtrace and symbolizes it into a string.
std::string DumpBacktrace();

//! Symbolizes a backtrace into a string.
std::string SymbolizeBacktrace(TBacktraceView backtrace);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BACKTRACE_INL_H_
#include "backtrace-inl.h"
#undef BACKTRACE_INL_H_
