#include "interruptible.h"

namespace NYdb {
namespace NConsoleClient {

TAtomic TInterruptibleCommand::Interrupted = false;

void TInterruptibleCommand::OnTerminate(int) {
    AtomicSet(Interrupted, true);
}

void TInterruptibleCommand::SetInterruptHandlers() {
    signal(SIGINT, &TInterruptibleCommand::OnTerminate);
    signal(SIGTERM, &TInterruptibleCommand::OnTerminate);
}

bool TInterruptibleCommand::IsInterrupted() {
    return AtomicGet(Interrupted);
}

}
}
