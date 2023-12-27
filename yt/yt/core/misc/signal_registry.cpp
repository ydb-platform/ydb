#include "signal_registry.h"

#include <yt/yt/build/config.h>

#include <library/cpp/yt/system/thread_id.h>

#include <util/generic/algorithm.h>

#include <signal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr std::initializer_list<int> CrashSignals{
    SIGSEGV,
    SIGILL,
    SIGFPE,
    SIGABRT,
#ifdef _unix_
    SIGBUS,
#endif
};

// This variable is used for protecting signal handlers for crash signals from
// dumping stuff while another thread is already doing that. Our policy is to let
// the first thread dump stuff and make other threads wait.
std::atomic<TSequentialThreadId> CrashingThreadId = InvalidSequentialThreadId;

////////////////////////////////////////////////////////////////////////////////

TSignalRegistry* TSignalRegistry::Get()
{
    return Singleton<TSignalRegistry>();
}

void TSignalRegistry::SetupSignal(int signal, int flags)
{
#ifdef _unix_
    DispatchMultiSignal(signal, [&] (int signal) {
        // Why would you like to use SIGALRM? It is used in crash handler
        // to prevent program hunging, do not interfere.
        YT_VERIFY(signal != SIGALRM);

        if (!OverrideNonDefaultSignalHandlers_) {
            struct sigaction sa;
            YT_VERIFY(sigaction(signal, nullptr, &sa) == 0);
            if (reinterpret_cast<void*>(sa.sa_sigaction) != SIG_DFL) {
                return;
            }
        }

        {
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = flags | SA_SIGINFO | SA_ONSTACK;
            sa.sa_sigaction = &Handle;
            YT_VERIFY(sigaction(signal, &sa, nullptr) == 0);
        }

        Signals_[signal].SetUp = true;
    });
#else
    DispatchMultiSignal(signal, [&] (int signal) {
        _crt_signal_t oldact = ::signal(signal, static_cast<_crt_signal_t>(&Handle));
        YT_VERIFY(oldact != SIG_ERR);
        if (!OverrideNonDefaultSignalHandlers_ && oldact != SIG_DFL) {
            YT_VERIFY(::signal(signal, oldact) != SIG_ERR);
            return;
        }
        Signals_[signal].SetUp = true;
    });
#endif
}

void TSignalRegistry::PushCallback(int signal, TSignalRegistry::TSignalHandler callback)
{
    DispatchMultiSignal(signal, [&] (int signal) {
        if (!Signals_[signal].SetUp) {
            SetupSignal(signal);
        }
        Signals_[signal].Callbacks.emplace_back(callback);
    });
}

#ifdef _unix_
void TSignalRegistry::PushCallback(int signal, std::function<void(int)> callback)
{
    PushCallback(signal, [callback = std::move(callback)] (int signal, siginfo_t* /*siginfo*/, void* /*ucontext*/) {
        callback(signal);
    });
}
#endif

void TSignalRegistry::PushCallback(int signal, std::function<void(void)> callback)
{
#ifdef _unix_
    PushCallback(signal, [callback = std::move(callback)] (int /*signal*/, siginfo_t* /*siginfo*/, void* /*ucontext*/) {
        callback();
    });
#else
    PushCallback(signal, [callback = std::move(callback)] (int /*signal*/) {
        callback();
    });
#endif
}

void TSignalRegistry::PushDefaultSignalHandler(int signal)
{
    PushCallback(signal, [] (int signal) {
    #ifdef _unix_
        {
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sigemptyset(&sa.sa_mask);
            sa.sa_handler = SIG_DFL;
            YT_VERIFY(sigaction(signal, &sa, nullptr) == 0);
        }

        YT_VERIFY(raise(signal) == 0);
    #else
        YT_VERIFY(::signal(signal, SIG_DFL) != SIG_ERR);
        YT_VERIFY(::raise(signal) == 0);
    #endif
    });
}

#ifdef _unix_
void TSignalRegistry::Handle(int signal, siginfo_t* siginfo, void* ucontext)
#else
void TSignalRegistry::Handle(int signal)
#endif
{
    auto* self = Get();

    if (self->EnableCrashSignalProtection_ &&
        Find(CrashSignals, signal) != CrashSignals.end())
    {
        // For crash signals we try pretty hard to prevent simultaneous execution of
        // several crash handlers.
        auto currentThreadId = GetSequentialThreadId();
        auto expectedCrashingThreadId = InvalidSequentialThreadId;
        if (!CrashingThreadId.compare_exchange_strong(expectedCrashingThreadId, currentThreadId)) {
            // We've already entered the signal handler. What should we do?
            if (currentThreadId == expectedCrashingThreadId) {
                // It looks the current thread is reentering the signal handler.
                // Something must be going wrong (maybe we are reentering by another
                // type of signal?). Simply return from here and hope that the default signal handler
                // (which is going to be executed after us by TSignalRegistry) will succeed in killing us.
                // Otherwise, we will probably end up  running out of stack entering
                // CrashSignalHandler over and over again. Not a bad thing, after all.
                return;
            } else {
                // Another thread is dumping stuff. Let's wait until that thread
                // finishes the job and kills the process.
                while (true) {
                    sleep(1);
                }
            }
        }

        // This is the first time we enter the signal handler.
        // Let the rest of the handlers do their interesting stuff.
    }

    for (const auto& callback : self->Signals_[signal].Callbacks) {
    #ifdef _unix_
        callback(signal, siginfo, ucontext);
    #else
        callback(signal);
    #endif
    }
}

template <class TCallback>
void TSignalRegistry::DispatchMultiSignal(int multiSignal, const TCallback& callback)
{
    std::vector<int> signals;
    if (multiSignal == AllCrashSignals) {
        signals = CrashSignals;
    } else {
        signals = {multiSignal};
    }

    for (int signal : signals) {
        callback(signal);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
