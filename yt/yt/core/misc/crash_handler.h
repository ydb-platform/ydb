#pragma once

#include <util/generic/string.h>

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

// "Codicils" are short human- and machine-readable strings organized into a per-fiber stack.
// When the crash handler is invoked, it dumps (alongside with the other
// useful stuff like backtrace) the content of the latter stack.

//! Installs a new codicil into the stack.
void PushCodicil(const TString& data);

//! Removes the top codicils from the stack.
void PopCodicil();

//! Returns the list of the currently installed codicils.
std::vector<TString> GetCodicils();

//! Invokes #PushCodicil in ctor and #PopCodicil in dtor.
class TCodicilGuard
{
public:
    TCodicilGuard();
    explicit TCodicilGuard(const TString& data);
    ~TCodicilGuard();

    TCodicilGuard(const TCodicilGuard& other) = delete;
    TCodicilGuard(TCodicilGuard&& other);

    TCodicilGuard& operator=(const TCodicilGuard& other) = delete;
    TCodicilGuard& operator=(TCodicilGuard&& other);

private:
    bool Active_;

    void Release();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CRASH_HANDLER_INL_H_
#include "crash_handler-inl.h"
#undef CRASH_HANDLER_INL_H_
