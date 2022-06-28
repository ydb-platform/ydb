#pragma once

#include "metric.h"
#include "metric_registry.h"

namespace NMonitoring {
    class TFakeMetricRegistry: public IMetricRegistry {
    public:
        TFakeMetricRegistry() noexcept
            : CommonLabels_{0}
        {
        }

        explicit TFakeMetricRegistry(TLabels commonLabels) noexcept
            : CommonLabels_{std::move(commonLabels)}
        {
        }

        IGauge* Gauge(ILabelsPtr labels) override;
        ILazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) override;
        IIntGauge* IntGauge(ILabelsPtr labels) override;
        ILazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) override;
        ICounter* Counter(ILabelsPtr labels) override;
        ILazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) override;
        IRate* Rate(ILabelsPtr labels) override;
        ILazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) override;

        IHistogram* HistogramCounter(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;

        IHistogram* HistogramRate(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;

        IHistogram* HistogramCounter(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> collector) override;

        IHistogram* HistogramRate(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> collector) override;
        void Accept(TInstant time, IMetricConsumer* consumer) const override;
        void Append(TInstant time, IMetricConsumer* consumer) const override;

        const TLabels& CommonLabels() const noexcept override;
        void RemoveMetric(const ILabels& labels) noexcept override;

    private:
        TRWMutex Lock_;
        THashMap<ILabelsPtr, IMetricPtr> Metrics_;

        template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
        TMetric* Metric(TLabelsType&& labels, Args&&... args);

        const TLabels CommonLabels_;
    };

    template <typename TBase>
    struct TFakeAcceptor: TBase {
        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            Y_UNUSED(time, consumer);
        }
    };

    struct TFakeIntGauge final: public TFakeAcceptor<IIntGauge> {
        i64 Add(i64 n) noexcept override {
            Y_UNUSED(n);
            return 0;
        }

        void Set(i64 n) noexcept override {
            Y_UNUSED(n);
        }

        i64 Get() const noexcept override {
            return 0;
        }
    };

    struct TFakeLazyIntGauge final: public TFakeAcceptor<ILazyIntGauge> {
        i64 Get() const noexcept override {
            return 0;
        }
    };

    struct TFakeRate final: public TFakeAcceptor<IRate> {
        ui64 Add(ui64 n) noexcept override {
            Y_UNUSED(n);
            return 0;
        }

        ui64 Get() const noexcept override {
            return 0;
        }

        void Reset() noexcept override {
        }
    };

    struct TFakeLazyRate final: public TFakeAcceptor<ILazyRate> {
        ui64 Get() const noexcept override {
            return 0;
        }
    };

    struct TFakeGauge final: public TFakeAcceptor<IGauge> {
        double Add(double n) noexcept override {
            Y_UNUSED(n);
            return 0;
        }

        void Set(double n) noexcept override {
            Y_UNUSED(n);
        }

        double Get() const noexcept override {
            return 0;
        }
    };

    struct TFakeLazyGauge final: public TFakeAcceptor<ILazyGauge> {
        double Get() const noexcept override {
            return 0;
        }
    };

    struct TFakeHistogram final: public IHistogram {
        TFakeHistogram(bool isRate = false)
            : IHistogram{isRate}
        {
        }

        void Record(double value) override {
            Y_UNUSED(value);
        }

        void Record(double value, ui32 count) override {
            Y_UNUSED(value, count);
        }

        IHistogramSnapshotPtr TakeSnapshot() const override {
            return nullptr;
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            Y_UNUSED(time, consumer);
        }

        void Reset() override {
        }
    };

    struct TFakeCounter final: public TFakeAcceptor<ICounter> {
        ui64 Add(ui64 n) noexcept override {
            Y_UNUSED(n);
            return 0;
        }

        ui64 Get() const noexcept override {
            return 0;
        }

        void Reset() noexcept override {
        }
    };

    struct TFakeLazyCounter final: public TFakeAcceptor<ILazyCounter> {
        ui64 Get() const noexcept override {
            return 0;
        }
    };
} // namespace NMonitoring
