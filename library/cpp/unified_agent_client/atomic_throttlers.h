#pragma once

#include <atomic>
#include <chrono>

namespace NUnifiedAgent {

class AtomicOneSecondThrottler {
   public:
    // The current time in seconds by AtomicOneSecondThrottler version.
    uint64_t static inline Now() {
        return std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
                   .count()
               // Add one second, because the steady clock starts after the
               // system reboot, and at the first moment, it's zero, and the
               // last operation is also zero.
               + 1;
    }
    // Check if we may do the operation.
    inline bool May() noexcept {
        return LastOp_.load(std::memory_order_acquire) < Now();
    }
    //  Check if we can do it right now. Book the limits.
    inline bool Do() noexcept {
        auto now = Now();
        auto last_op_expected = LastOp_.load(std::memory_order_acquire);
        // Early check that we can do operation.
        if (last_op_expected >= now) {
            // It means that somebody has already taken our second, or maybe
            // this thread is stuck in the past.
            return false;
        }
        // Place our second in the LastOp_,
        // if false, that means somebody replaced last_op_expected value.
        return LastOp_.compare_exchange_strong(last_op_expected, now,
                                               std::memory_order_acq_rel);
    }

   private:
    // Last operation time
    std::atomic<uint64_t> LastOp_{0};
    static_assert(decltype(LastOp_)::is_always_lock_free,
                  "AtomicOneSecondThrottler::LastOp_ must be always lock-free");
};

}  // namespace NUnifiedAgent

