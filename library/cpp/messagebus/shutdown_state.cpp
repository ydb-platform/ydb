#include "shutdown_state.h"

#include <util/system/yassert.h>

void TAtomicShutdownState::ShutdownCommand() {
    Y_ABORT_UNLESS(State.CompareAndSet(SS_RUNNING, SS_SHUTDOWN_COMMAND));
}

void TAtomicShutdownState::CompleteShutdown() {
    Y_ABORT_UNLESS(State.CompareAndSet(SS_SHUTDOWN_COMMAND, SS_SHUTDOWN_COMPLETE));
    ShutdownComplete.Signal();
}

bool TAtomicShutdownState::IsRunning() {
    return State.Get() == SS_RUNNING;
}

TAtomicShutdownState::~TAtomicShutdownState() {
    Y_ABORT_UNLESS(SS_SHUTDOWN_COMPLETE == State.Get());
}
