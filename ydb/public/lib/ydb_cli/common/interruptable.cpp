#include "interruptable.h"

namespace NYdb::NConsoleClient {

TAtomic TInterruptableCommand::Interrupted = false;

void TInterruptableCommand::OnTerminate(int) {
    AtomicSet(Interrupted, true);
}

void TInterruptableCommand::SetInterruptHandlers() {
    signal(SIGINT, &TInterruptableCommand::OnTerminate);
    signal(SIGTERM, &TInterruptableCommand::OnTerminate);
}

bool TInterruptableCommand::IsInterrupted() {
    return AtomicGet(Interrupted);
}

} // namespace NYdb::NConsoleClient
