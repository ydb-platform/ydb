#pragma once

#include <util/datetime/base.h>

#include <atomic>

namespace NUnifiedAgent {
    class TClock {
    public:
        static void Configure();

        static inline bool Configured() {
            return Configured_;
        }

        static inline TInstant Now() {
            return Configured_ ? Get() : TInstant::Now();
        }

        static void SetBase(TInstant value);

        static void ResetBase();

        static void ResetBaseWithShift();

        static void SetShift(TDuration value);

        static void ResetShift();

        static TInstant Get();

    private:
        static bool Configured_;
        static std::atomic<ui64> Base_;
        static std::atomic<i64> Shift_;
    };
}
