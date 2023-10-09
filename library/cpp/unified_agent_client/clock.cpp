#include "clock.h"

namespace NUnifiedAgent {
    void TClock::Configure() {
        Y_ABORT_UNLESS(!Configured_);

        Configured_ = true;
    }

    void TClock::SetBase(TInstant value) {
        Y_ABORT_UNLESS(Configured_);

        Base_.store(value.GetValue());
    }

    void TClock::ResetBase() {
        Base_.store(0);
    }

    void TClock::ResetBaseWithShift() {
        Y_ABORT_UNLESS(Configured_);

        Shift_.store(static_cast<i64>(Base_.exchange(0)) - static_cast<i64>(::Now().GetValue()));
    }

    void TClock::SetShift(TDuration value) {
        Y_ABORT_UNLESS(Configured_);

        Shift_.fetch_add(value.GetValue());
    }

    void TClock::ResetShift() {
        Shift_.store(0);
    }

    TInstant TClock::Get() {
        auto base = Base_.load();
        if (base == 0) {
            base = ::Now().GetValue();
        }
        base += Shift_.load();
        return TInstant::FromValue(base);
    }

    bool TClock::Configured_{false};
    std::atomic<ui64> TClock::Base_{0};
    std::atomic<i64> TClock::Shift_{0};
}
