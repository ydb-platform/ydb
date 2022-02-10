#include "shutdown_state.h" 
 
#include <util/system/yassert.h>

void TAtomicShutdownState::ShutdownCommand() {
    Y_VERIFY(State.CompareAndSet(SS_RUNNING, SS_SHUTDOWN_COMMAND));
}

void TAtomicShutdownState::CompleteShutdown() {
    Y_VERIFY(State.CompareAndSet(SS_SHUTDOWN_COMMAND, SS_SHUTDOWN_COMPLETE));
    ShutdownComplete.Signal();
}

bool TAtomicShutdownState::IsRunning() {
    return State.Get() == SS_RUNNING;
}

TAtomicShutdownState::~TAtomicShutdownState() {
    Y_VERIFY(SS_SHUTDOWN_COMPLETE == State.Get());
}
