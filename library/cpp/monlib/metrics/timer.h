#pragma once

#include "metric.h"

#include <util/generic/typetraits.h>

#include <chrono>


namespace NMonitoring {

    /**
     * A timing scope to record elapsed time since creation.
     */
    template <typename TMetric,
              typename Resolution = std::chrono::milliseconds,
              typename Clock = std::chrono::high_resolution_clock>
    class TMetricTimerScope {
    public:
        explicit TMetricTimerScope(TMetric* metric)
                : Metric_(metric)
                , StartTime_(Clock::now())
        {
            Y_ENSURE(Metric_);
        }

        TMetricTimerScope(TMetricTimerScope&) = delete;
        TMetricTimerScope& operator=(const TMetricTimerScope&) = delete;

        TMetricTimerScope(TMetricTimerScope&& other) {
            *this = std::move(other);
        }

        TMetricTimerScope& operator=(TMetricTimerScope&& other) {
            Metric_ = other.Metric_;
            other.Metric_ = nullptr;
            StartTime_ = std::move(other.StartTime_);

            return *this;
        }

        void Record() {
            Y_DEBUG_ABORT_UNLESS(Metric_);
            if (Metric_ == nullptr) {
                return;
            }

            auto duration = std::chrono::duration_cast<Resolution>(Clock::now() - StartTime_).count();
            if constexpr (std::is_same<TMetric, TGauge>::value) {
                Metric_->Set(duration);
            } else if constexpr (std::is_same<TMetric, TIntGauge>::value) {
                Metric_->Set(duration);
            } else if constexpr (std::is_same<TMetric, TCounter>::value) {
                Metric_->Add(duration);
            } else if constexpr (std::is_same<TMetric, TRate>::value) {
                Metric_->Add(duration);
            } else if constexpr (std::is_same<TMetric, THistogram>::value) {
                Metric_->Record(duration);
            } else {
                static_assert(TDependentFalse<TMetric>, "Not supported metric type");
            }

            Metric_ = nullptr;
        }

        ~TMetricTimerScope() {
            if (Metric_ == nullptr) {
                return;
            }

            Record();
        }

    private:
        TMetric* Metric_{nullptr};
        typename Clock::time_point StartTime_;
    };

    /**
     * @brief A class that is supposed to use to measure execution time of an asynchronuous operation.
     *
     * In order to be able to capture an object into a lambda which is then passed to TFuture::Subscribe/Apply,
     * the object must be copy constructible (limitation of the std::function class). So, we cannot use the TMetricTimerScope
     * with the abovementioned functions without storing it in a shared pointer or somewhere else. This class works around this
     * issue with wrapping the timer with a auto_ptr-like hack  Also, Record is const so that one doesn't need to make every lambda mutable
     * just to record time measurement.
     */
    template <typename TMetric,
              typename Resolution = std::chrono::milliseconds,
              typename Clock = std::chrono::high_resolution_clock>
    class TFutureFriendlyTimer {
    public:
        explicit TFutureFriendlyTimer(TMetric* metric)
            : Impl_{metric}
        {
        }

        TFutureFriendlyTimer(const TFutureFriendlyTimer& other)
            : Impl_{std::move(other.Impl_)}
        {
        }

        TFutureFriendlyTimer& operator=(const TFutureFriendlyTimer& other) {
            Impl_ = std::move(other.Impl_);
        }

        TFutureFriendlyTimer(TFutureFriendlyTimer&&) = default;
        TFutureFriendlyTimer& operator=(TFutureFriendlyTimer&& other) = default;

        void Record() const {
            Impl_.Record();
        }

    private:
        mutable TMetricTimerScope<TMetric, Resolution, Clock> Impl_;
    };

    template <typename TMetric>
    TMetricTimerScope<TMetric> ScopeTimer(TMetric* metric) {
        return TMetricTimerScope<TMetric>{metric};
    }

    template <typename TMetric>
    TFutureFriendlyTimer<TMetric> FutureTimer(TMetric* metric) {
        return TFutureFriendlyTimer<TMetric>{metric};
    }
}
