#include "interruptable.h"

namespace NYdb::NConsoleClient {

std::atomic<bool> TInterruptableCommand::Interrupted = false;

void TInterruptableCommand::OnTerminate(int) {
    Interrupted.store(true);
}

void TInterruptableCommand::SetInterruptHandlers() {
    SignalHandlers[SIGINT] = std::signal(SIGINT, &TInterruptableCommand::OnTerminate);
    SignalHandlers[SIGTERM] = std::signal(SIGTERM, &TInterruptableCommand::OnTerminate);
}

void TInterruptableCommand::ResetInterrupted() {
    for (const auto& [signal, handler] : SignalHandlers) {
        std::signal(signal, handler);
    }

    SignalHandlers.clear();
    Interrupted.store(false);
}

bool TInterruptableCommand::IsInterrupted() {
    return Interrupted.load();
}

} // namespace NYdb::NConsoleClient
