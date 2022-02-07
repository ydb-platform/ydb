#pragma once

#include "misc/atomic_box.h"

#include <util/system/event.h>

enum EShutdownState {
    SS_RUNNING,
    SS_SHUTDOWN_COMMAND,
    SS_SHUTDOWN_COMPLETE,
};

struct TAtomicShutdownState {
    TAtomicBox<EShutdownState> State;
    TSystemEvent ShutdownComplete;

    void ShutdownCommand();
    void CompleteShutdown();
    bool IsRunning();

    ~TAtomicShutdownState();
};
