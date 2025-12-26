#include "interruptable.h"

namespace NYdb::NConsoleClient {

TAtomic TInterruptableCommand::Interrupted = false;

void TInterruptableCommand::OnTerminate(int) {
    AtomicSet(Interrupted, true);
}

void TInterruptableCommand::SetInterruptHandlers() {
    SignalHandlers[SIGINT] = std::signal(SIGINT, &TInterruptableCommand::OnTerminate);
    SignalHandlers[SIGTERM] = std::signal(SIGTERM, &TInterruptableCommand::OnTerminate);
}

void TInterruptableCommand::ResetInterrupted() {
    AtomicSet(Interrupted, false);

    for (const auto& [signal, handler] : SignalHandlers) {
        std::signal(signal, handler);
    }

    SignalHandlers.clear();
}

bool TInterruptableCommand::IsInterrupted() {
    return AtomicGet(Interrupted);
}

} // namespace NYdb::NConsoleClient
