#include "interruptible.h"

namespace NYdb::NConsoleClient {

TAtomic TInterruptibleCommand::Interrupted = false;

void TInterruptibleCommand::OnTerminate(int) {
    if (Interrupted) {
        std::exit(0);
    }
    AtomicSet(Interrupted, true);
}

void TInterruptibleCommand::SetInterruptHandlers() {
    signal(SIGINT, &TInterruptibleCommand::OnTerminate);
    signal(SIGTERM, &TInterruptibleCommand::OnTerminate);
}

bool TInterruptibleCommand::IsInterrupted() {
    return AtomicGet(Interrupted);
}

} // namespace NYdb::NConsoleClient
