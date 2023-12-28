#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Shorthand for all crash signals (SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS).
//! May be used instead of signal in all public methods of signal registry.
constexpr int AllCrashSignals = -1;

//! Singleton class which provides convenient interface for signal handler registration.
class TSignalRegistry
{
public:
    //! Enables a mechanism which protects multiple crash signal handlers from simultaneous
    //! execution.
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableCrashSignalProtection, true);

    //! Prevents from overriding user custom signal handlers.
    DEFINE_BYVAL_RW_PROPERTY(bool, OverrideNonDefaultSignalHandlers, true);

public:
#ifdef _unix_
    using TSignalHandler = std::function<void(int, siginfo_t*, void*)>;
#else
    using TSignalHandler = std::function<void(int)>;
#endif

    static TSignalRegistry* Get();

    //! Sets up our handler that invokes registered callbacks in order.
    //! Flags has same meaning as sa_flags in sigaction(2). Use this method if you need certain flags.
    //! By default any signal touched by PushCallback(...) will be set up with default flags.
    void SetupSignal(int signal, int flags = 0);

    //! Adds a simple callback which should be called for signal. Different signatures are supported for convenience.
    void PushCallback(int signal, std::function<void(void)> callback);
#ifdef _unix_
    void PushCallback(int signal, std::function<void(int)> callback);
#endif
    void PushCallback(int signal, TSignalHandler callback);

    //! Adds the default signal handler which is called after invoking our custom handlers.
    //! NB: this handler restores default signal handler as a side-effect. Use it only
    //! when default handler terminates the program.
    void PushDefaultSignalHandler(int signal);

private:
    static constexpr int SignalRange = 64;

    struct TSignalSetup
    {
        std::vector<TSignalHandler> Callbacks;
        bool SetUp = false;
    };
    std::array<TSignalSetup, SignalRange> Signals_;

#ifdef _unix_
    static void Handle(int signal, siginfo_t* siginfo, void* ucontext);
#else
    static void Handle(int signal);
#endif

    //! Invoke something for `multisignal` which may either be some real signal or signal set like `AllCrashSignals`.
    template <class TCallback>
    void DispatchMultiSignal(int multiSignal, const TCallback& callback);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
