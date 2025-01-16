#pragma once

#include <util/generic/strbuf.h>

#ifdef _unix_
#include <signal.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Writes the given buffer with the length to the standard error.
void WriteToStderr(const char* buffer, int length);
//! Writes the given zero-terminated buffer to the standard error.
void WriteToStderr(const char* buffer);
//! Same for TStringBuf.
void WriteToStderr(TStringBuf buffer);

#ifdef _unix_
// Dumps signal, stack frame information and codicils.
void CrashSignalHandler(int signal, siginfo_t* si, void* uc);
#else
void CrashSignalHandler(int signal);
#endif

template <class TCallback>
void DumpStackTrace(TCallback flushCallback, void* startPC = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CRASH_HANDLER_INL_H_
#include "crash_handler-inl.h"
#undef CRASH_HANDLER_INL_H_
