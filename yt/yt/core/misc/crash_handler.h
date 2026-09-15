#pragma once

#include <util/generic/strbuf.h>

#ifdef _unix_
#include <signal.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TCrashHandlerWriter = void(*)(TStringBuf);

////////////////////////////////////////////////////////////////////////////////

//! Writes the given string to the standard error.
void WriteToStderr(TStringBuf str);

//! Change default crash handler writer (WriteToStderr) to custom one.
void SetCrashHandlerWriter(TCrashHandlerWriter writer);

//! Writes the given string with crash handler writer (writes to stderr by default).
void WriteCrashHandlerOutput(TStringBuf str);

#ifdef _unix_
// Dumps signal, stack frame information and codicils.
void CrashSignalHandler(int signal, siginfo_t* si, void* uc);
#else
void CrashSignalHandler(int signal);
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
