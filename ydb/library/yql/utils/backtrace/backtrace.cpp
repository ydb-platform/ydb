#include "backtrace.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/malloc/api/malloc.h>

#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/system/backtrace.h>
#include <util/system/type_name.h>
#include <util/system/execpath.h>
#include <util/system/platform.h>
#include <util/system/mlock.h>

#ifdef _linux_
#include <dlfcn.h>
#include <link.h>
#include <signal.h>
size_t BackTrace(void** p, size_t len, ucontext_t* signal_ucontext);
#endif

#include <functional>
#include <vector>

#ifndef _win_

bool SetSignalHandler(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_RESETHAND;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    return sigaction(signo, &sa, nullptr) != -1;
}

namespace {
#if defined(_linux_) && defined(_x86_64_)
bool SetSignalAction(int signo, void (*handler)(int, siginfo_t*, void*)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_RESETHAND | SA_SIGINFO;
    sa.sa_sigaction = (decltype(sa.sa_sigaction))handler;
    sigfillset(&sa.sa_mask);
    return sigaction(signo, &sa, nullptr) != -1;
}
#endif
} // namespace
#endif // _win_

TAtomic BacktraceStarted = 0;

void SetFatalSignalHandler(void (*handler)(int))
{
    Y_UNUSED(handler);
#ifndef _win_
    for (int signo: {SIGSEGV, SIGILL, SIGABRT, SIGFPE}) {
        if (!SetSignalHandler(signo, handler)) {
            ythrow TSystemError() << "Cannot set handler for signal " << strsignal(signo);
        }
    }
#endif
}

#if defined(_linux_) && defined(_x86_64_)
void SetFatalSignalAction(void (*sigaction)(int, siginfo_t*, void*))
{
    Y_UNUSED(sigaction);
#ifndef _win_
    for (int signo: {SIGSEGV, SIGILL, SIGABRT, SIGFPE}) {
        if (!SetSignalAction(signo, sigaction)) {
            ythrow TSystemError() << "Cannot set sigaction for signal " << strsignal(signo);
        }
    }
#endif
}
#endif

#include <sstream>
#include <iostream>
namespace {
std::vector<std::function<void(int)>> Before, After;
void* Stack[300];
bool KikimrSymbolize = false;

void CallCallbacks(decltype(Before)& where, int signum) {
    for (const auto &fn: where) {
        if (fn) {
            fn(signum);
        }
    }
}

void StackFilledCallback(IOutputStream* out, size_t stack_size) {
    auto symbolizer = BuildSymbolizer(KikimrSymbolize);
    
    for (size_t i = 0; i < stack_size; ++i) {
        *out << symbolizer->SymbolizeFrame(Stack[i]);
    }

}

void SignalHandler(int signum)
{
    CallCallbacks(Before, signum);

    if (!NMalloc::IsAllocatorCorrupted) {
        if (!AtomicTryLock(&BacktraceStarted)) {
            return;
        }
        
        UnlockAllMemory();

        const size_t s = BackTrace(Stack, Y_ARRAY_SIZE(Stack));
        StackFilledCallback(&Cerr, s);
    }
    
    CallCallbacks(After, signum);

    raise(signum);
}

#if defined(_linux_) && defined(_x86_64_)
void SignalAction(int signum, siginfo_t* sinfo, void* context)
{
    Y_UNUSED(SignalHandler);
    CallCallbacks(Before, signum);
    Y_UNUSED(sinfo);

    if (!NMalloc::IsAllocatorCorrupted) {
        if (!AtomicTryLock(&BacktraceStarted)) {
            return;
        }

        UnlockAllMemory();
        const size_t s = BackTrace(Stack, Y_ARRAY_SIZE(Stack), (ucontext_t*)context);
        StackFilledCallback(&Cerr, s);
    }
    
    CallCallbacks(After, signum);

    raise(signum);
}
#endif

}

namespace NYql {

namespace NBacktrace {
    
THashMap<TString, TString> Mapping;

void SetModulesMapping(const THashMap<TString, TString>& mapping) {
    Mapping = mapping;
}

void AddBeforeFatalCallback(const std::function<void(int)>& before) {
    Before.push_back(before);
}

void AddAfterFatalCallback(const std::function<void(int)>& after) {
    After.push_back(after);
}

void RegisterKikimrFatalActions() {
    #if !defined(_linux_) || !defined(_x86_64_)
    SetFatalSignalHandler(SignalHandler);
    #else
    SetFatalSignalAction(SignalAction);
    #endif
}

void EnableKikimrSymbolize() {
    KikimrSymbolize = true;
}

void KikimrBackTrace() {
    KikimrBackTraceFormatImpl(&Cerr);
}

void KikimrBackTraceFormatImpl(IOutputStream* out) {
    KikimrSymbolize = true;
    UnlockAllMemory();
    void* array[300];
    const size_t s = BackTrace(array, Y_ARRAY_SIZE(array));
    StackFilledCallback(out, s);
}

void KikimrBacktraceFormatImpl(IOutputStream* out, void* const* stack, size_t stackSize) {
    KikimrSymbolize = true;
    for (size_t i = 0; i < stackSize; ++i) {
        Stack[i] = stack[i];
    }

    StackFilledCallback(out, stackSize);
}

}
}

TString IBacktraceSymbolizer::SymbolizeFrame(void* ptr) {
    Y_UNUSED(ptr);
    return "";
}
IBacktraceSymbolizer::~IBacktraceSymbolizer() {
}

void EnableKikimrBacktraceFormat() {
    SetFormatBackTraceFn(NYql::NBacktrace::KikimrBacktraceFormatImpl);
}