#pragma once
#include "defs.h"
#include "immediate_control_board_control.h"

namespace NKikimr {

class TControlWrapper {
    TIntrusivePtr<TControl> Control;
    friend class TControlBoard;

public:
    TControlWrapper(TAtomicBase defaultValue = 0)
        : Control(new TControl(defaultValue, Min<TAtomicBase>(), Max<TAtomicBase>()))
    {}

    TControlWrapper(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound)
        : Control(new TControl(defaultValue, lowerBound, upperBound))
    {}

    operator i64() const {
        return Control->Get();
    }

    i64 operator=(i64 value) {
        Control->Set(value);
        return value;
    }

    bool IsTheSame(TControlWrapper another) {
        return Control == another.Control;
    }

    /**
     * Resets an existing control to different values.
     *
     * WARNING: this method is not thread safe and may only be used during initialization.
     */
    void Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound) {
        Control->Reset(defaultValue, lowerBound, upperBound);
    }
};

// WARNING: not thread safe
class TMemorizableControlWrapper {
    static constexpr i32 RequestCountWithRelevantValue = 1024;
    static constexpr TDuration TimeDurationWithRelevantValue = TDuration::Seconds(15);
    TControlWrapper Control;
    TInstant CheckingRelevantDeadline;
    i32 CheckingCounter = 0;
    i64 CurrentValue = 0;

public:
    TMemorizableControlWrapper(const TControlWrapper &control)
        : Control(control)
        , CurrentValue(Control)
    {
    }

    void ResetControl(const TControlWrapper &control) {
        Control = control;
        CurrentValue = control;
    }

    i64 Update(TInstant now) {
        CheckingCounter--;
        if (now > CheckingRelevantDeadline || CheckingCounter <= 0) {
            CurrentValue = Control;
            CheckingRelevantDeadline = now + TimeDurationWithRelevantValue;
            CheckingCounter = RequestCountWithRelevantValue;
        }
        return CurrentValue;
    }

    operator i64() const {
        return CurrentValue;
    }
};

}
