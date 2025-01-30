#pragma once

#include <signal.h>

namespace NSignals {

using sighandler_t = void (*)(int);

template <int signum, sighandler_t handler>
struct TSignalHandler {
    sighandler_t SavedHandler = nullptr;

    TSignalHandler()
        : SavedHandler(signal(signum, handler))
    {}

    ~TSignalHandler() {
        signal(signum, SavedHandler);
    }
};

template <int signum>
struct TSignalIgnore {
    sighandler_t SavedHandler = nullptr;

    TSignalIgnore()
        : SavedHandler(signal(signum, SIG_IGN))
    {}

    ~TSignalIgnore() {
        signal(signum, SavedHandler);
    }
};

}
