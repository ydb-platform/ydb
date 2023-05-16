#include "state.h"

namespace NKikimr::NKqp {

void TKqpShutdownState::Update(ui32 pendingSessions) {
    AtomicSet(PendingSessions_, pendingSessions);

    if (!Initialized()) {
        AtomicSet(Initialized_, 1);
    }

    if (!pendingSessions) {
        SetCompleted();
    }
}

} // namespace NKikimr::NKqp
