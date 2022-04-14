#pragma once

#include <util/datetime/base.h>
#include <util/random/normal.h>
#include <util/system/event.h>

#include <atomic>

namespace NTvmAuth {
    // https://habr.com/ru/post/227225/
    class TExponentialBackoff {
    public:
        struct TSettings {
            TDuration Min;
            TDuration Max;
            double Factor = 1.001;
            double Jitter = 0;

            bool operator==(const TSettings& o) const {
                return Min == o.Min &&
                       Max == o.Max &&
                       Factor == o.Factor &&
                       Jitter == o.Jitter;
            }
        };

        TExponentialBackoff(const TSettings& settings, bool isEnabled = true)
            : CurrentValue_(settings.Min)
            , IsEnabled_(isEnabled)
        {
            UpdateSettings(settings);
        }

        void UpdateSettings(const TSettings& settings) {
            Y_ENSURE(settings.Factor > 1, "factor=" << settings.Factor << ". Should be > 1");
            Y_ENSURE(settings.Jitter >= 0 && settings.Jitter < 1, "jitter should be in range [0, 1)");

            Min_ = settings.Min;
            Max_ = settings.Max;
            Factor_ = settings.Factor;
            Jitter_ = settings.Jitter;
        }

        TDuration Increase() {
            CurrentValue_ = std::min(CurrentValue_ * Factor_, Max_);

            double rnd = StdNormalRandom<double>();
            const bool isNegative = rnd < 0;
            rnd = std::abs(rnd);

            const TDuration diff = rnd * Jitter_ * CurrentValue_;
            if (isNegative) {
                CurrentValue_ -= diff;
            } else {
                CurrentValue_ += diff;
            }

            return CurrentValue_;
        }

        TDuration Decrease() {
            CurrentValue_ = std::max(CurrentValue_ / Factor_, Min_);
            return CurrentValue_;
        }

        void Sleep(TDuration add = TDuration()) {
            if (IsEnabled_.load(std::memory_order_relaxed)) {
                Ev_.WaitT(CurrentValue_ + add);
            }
        }

        void Interrupt() {
            Ev_.Signal();
        }

        TDuration GetCurrentValue() const {
            return CurrentValue_;
        }

        void SetEnabled(bool val) {
            IsEnabled_.store(val);
        }

    private:
        TDuration Min_;
        TDuration Max_;
        double Factor_;
        double Jitter_;
        TDuration CurrentValue_;
        std::atomic_bool IsEnabled_;

        TAutoEvent Ev_;
    };
}
