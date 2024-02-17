#include "backtrace.h"

#include <ydb/library/yql/utils/backtrace/libbacktrace/backtrace.h>
#include <ydb/library/yql/utils/backtrace/libbacktrace/symbolizer.h>

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
#include <signal.h>
#endif

#include <functional>
#include <vector>
#include <sstream>
#include <iostream>

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

void SetFatalSignalHandler(void (*handler)(int)) {
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
    for (int signo: {SIGSEGV, SIGILL, SIGABRT, SIGFPE}) {
        if (!SetSignalAction(signo, sigaction)) {
            ythrow TSystemError() << "Cannot set sigaction for signal " << strsignal(signo);
        }
    }
}
#endif

namespace {
    std::vector<std::function<void(int)>> Before, After;
    bool KikimrSymbolize = false;

    void CallCallbacks(decltype(Before)& where, int signum) {
        for (const auto &fn: where) {
            if (fn) {
                fn(signum);
            }
        }
    }

    void PrintFrames(IOutputStream* out, const TVector<NYql::NBacktrace::TCollectedFrame>& frames);

    void DoBacktrace(IOutputStream* out, void* data) {
        PrintFrames(out, NYql::NBacktrace::CollectFrames(data));
    }

    void DoBacktrace(IOutputStream* out, void** stack, size_t cnt) {
        PrintFrames(out, NYql::NBacktrace::CollectFrames(stack, cnt));
    }
    

    void SignalHandler(int signum) {
        CallCallbacks(Before, signum);

        if (!NMalloc::IsAllocatorCorrupted) {
            if (!AtomicTryLock(&BacktraceStarted)) {
                return;
            }
            
            UnlockAllMemory();
            DoBacktrace(&Cerr, nullptr);
        }
        
        CallCallbacks(After, signum);
        raise(signum);
    }

#if defined(_linux_) && defined(_x86_64_)
    void SignalAction(int signum, siginfo_t*, void* context) {
        Y_UNUSED(SignalHandler);
        CallCallbacks(Before, signum);

        if (!NMalloc::IsAllocatorCorrupted) {
            if (!AtomicTryLock(&BacktraceStarted)) {
                return;
            }

            UnlockAllMemory();
            DoBacktrace(&Cerr, context);
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
#if defined(_linux_) && defined(_x86_64_)
            SetFatalSignalAction(SignalAction);
#else
            SetFatalSignalHandler(SignalHandler);
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
            DoBacktrace(out, nullptr);
        }

        void KikimrBacktraceFormatImpl(IOutputStream* out, void* const* stack, size_t stackSize) {
            KikimrSymbolize = true;
            DoBacktrace(out, (void**)stack, stackSize);
        }

    }
}

void EnableKikimrBacktraceFormat() {
    SetFormatBackTraceFn(NYql::NBacktrace::KikimrBacktraceFormatImpl);
}

namespace {
    void PrintFrames(IOutputStream* out, const TVector<NYql::NBacktrace::TCollectedFrame>& frames) {
        auto& outp = *out;
        if (KikimrSymbolize) {
            TVector<NYql::NBacktrace::TStackFrame> sFrames(frames.size());
            for (size_t i = 0; i < frames.size(); ++i) {
                sFrames[i] = NYql::NBacktrace::TStackFrame{frames[i].File, frames[i].Address};
            }
            auto symbolized = NYql::NBacktrace::Symbolize(sFrames);
            for (auto& line: symbolized) {
                outp << line << "\n";
            }
            return;
        }
        outp << "StackFrames: " << frames.size() << "\n";
        for (auto &frame: frames) {
            auto fileName = frame.File;
            if (fileName == "/proc/self/exe") {
                fileName = "EXE";
            }
            auto it = NYql::NBacktrace::Mapping.find(frame.File);
            Cerr << "StackFrame: " << (it == NYql::NBacktrace::Mapping.end() ? frame.File : it->second) << " " << frame.Address << "\n";
        }
    }
}