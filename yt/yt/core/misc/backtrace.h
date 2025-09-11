#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Gets backtrace and symbolizes it passing result into #callback.
template <class TCallback>
void DumpBacktrace(TCallback callback, void* startPC = nullptr);

//! Gets backtrace and symbolizes it into a string.
std::string DumpBacktrace(void* startPC = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BACKTRACE_INL_H_
#include "backtrace-inl.h"
#undef BACKTRACE_INL_H_
