#include "ewma.h"
#include "metric.h"

#include <atomic>
#include <cmath>

namespace NMonitoring {
namespace {
    constexpr auto DEFAULT_INTERVAL = TDuration::Seconds(5);

    const double ALPHA1 = 1. - std::exp(-double(DEFAULT_INTERVAL.Seconds())/60./1.);
    const double ALPHA5 = 1. - std::exp(-double(DEFAULT_INTERVAL.Seconds())/60./5.);
    const double ALPHA15 = 1. - std::exp(-double(DEFAULT_INTERVAL.Seconds())/60./15.);

    class TExpMovingAverage final: public IExpMovingAverage {
    public:
        explicit TExpMovingAverage(IGauge* metric, double alpha, TDuration interval)
            : Metric_{metric}
            , Alpha_{alpha}
            , Interval_{interval.Seconds()}
        {
            Y_ABORT_UNLESS(metric != nullptr, "Passing nullptr metric is not allowed");
        }

        ~TExpMovingAverage() override = default;

        // This method NOT thread safe
        void Tick() override {
            const auto current = Uncounted_.fetch_and(0);
            const double instantRate = double(current) / Interval_;

            if (Y_UNLIKELY(!IsInitialized())) {
                Metric_->Set(instantRate);
                Init_ = true;
            } else {
                const double currentRate = Metric_->Get();
                Metric_->Set(Alpha_ * (instantRate - currentRate) + currentRate);
            }

        }

        void Update(i64 value) override {
            Uncounted_ += value;
        }

        double Rate() const override {
            return Metric_->Get();
        }

        void Reset() override {
            Init_ = false;
            Uncounted_ = 0;
        }

    private:
        bool IsInitialized() const {
            return Init_;
        }

    private:
        std::atomic<i64> Uncounted_{0};
        std::atomic<bool> Init_{false};

        IGauge* Metric_{nullptr};
        double Alpha_;
        ui64 Interval_;
    };

    struct TFakeEwma: IExpMovingAverage {
        void Tick() override {}
        void Update(i64) override {}
        double Rate() const override { return 0; }
        void Reset() override {}
    };

} // namespace

    TEwmaMeter::TEwmaMeter()
        : Ewma_{MakeHolder<TFakeEwma>()}
    {
    }

    TEwmaMeter::TEwmaMeter(IExpMovingAveragePtr&& ewma)
        : Ewma_{std::move(ewma)}
    {
    }

    TEwmaMeter::TEwmaMeter(TEwmaMeter&& other) {
        if (&other == this) {
            return;
        }

        *this = std::move(other);
    }

    TEwmaMeter& TEwmaMeter::operator=(TEwmaMeter&& other) {
        Ewma_ = std::move(other.Ewma_);
        LastTick_.store(other.LastTick_);
        return *this;
    }

    void TEwmaMeter::TickIfNeeded() {
        constexpr ui64 INTERVAL_SECONDS = DEFAULT_INTERVAL.Seconds();

        const auto now = TInstant::Now().Seconds();
        ui64 old = LastTick_.load();
        const auto secondsSinceLastTick = now - old;

        if (secondsSinceLastTick > INTERVAL_SECONDS) {
            // round to the interval grid
            const ui64 newLast = now - (secondsSinceLastTick % INTERVAL_SECONDS);
            if (LastTick_.compare_exchange_strong(old, newLast)) {
                for (size_t i = 0; i < secondsSinceLastTick / INTERVAL_SECONDS; ++i) {
                    Ewma_->Tick();
                }
            }
        }
    }

    void TEwmaMeter::Mark() {
        TickIfNeeded();
        Ewma_->Update(1);
    }

    void TEwmaMeter::Mark(i64 value) {
        TickIfNeeded();
        Ewma_->Update(value);
    }

    double TEwmaMeter::Get() {
        TickIfNeeded();
        return Ewma_->Rate();
    }

    IExpMovingAveragePtr OneMinuteEwma(IGauge* metric) {
        return MakeHolder<TExpMovingAverage>(metric, ALPHA1, DEFAULT_INTERVAL);
    }

    IExpMovingAveragePtr FiveMinuteEwma(IGauge* metric) {
        return MakeHolder<TExpMovingAverage>(metric, ALPHA5, DEFAULT_INTERVAL);
    }

    IExpMovingAveragePtr FiveteenMinuteEwma(IGauge* metric) {
        return MakeHolder<TExpMovingAverage>(metric, ALPHA15, DEFAULT_INTERVAL);
    }

    IExpMovingAveragePtr CreateEwma(IGauge* metric, double alpha, TDuration interval) {
        return MakeHolder<TExpMovingAverage>(metric, alpha, interval);
    }
} // namespace NMonitoring
