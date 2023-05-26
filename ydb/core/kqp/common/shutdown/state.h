#pragma once
#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NKqp {

class TKqpShutdownController;

class TKqpShutdownState: public TThrRefBase {
    friend class TKqpShutdownController;

public:
    void Update(ui32 pendingSessions);
private:
    bool ShutdownComplete() const {
        return AtomicGet(ShutdownComplete_) == 1;
    }

    ui32 GetPendingSessions() const {
        return AtomicGet(PendingSessions_);
    }

    bool Initialized() const {
        return AtomicGet(Initialized_) == 1;
    }

    void SetCompleted() {
        AtomicSet(ShutdownComplete_, 1);
    }

    TAtomic PendingSessions_ = 0;
    TAtomic Initialized_ = 0;
    TAtomic ShutdownComplete_ = 0;
};

} // namespace NKikimr::NKqp
