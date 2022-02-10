#pragma once

#include "labels.h"
#include "metric.h"

#include <util/system/rwlock.h>

#include <library/cpp/threading/light_rw_lock/lightrwlock.h>


namespace NMonitoring {
    class IMetricFactory {
    public:
        virtual ~IMetricFactory() = default;

        virtual IGauge* Gauge(ILabelsPtr labels) = 0;
        virtual ILazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) = 0;
        virtual IIntGauge* IntGauge(ILabelsPtr labels) = 0;
        virtual ILazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) = 0;
        virtual ICounter* Counter(ILabelsPtr labels) = 0;
        virtual ILazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) = 0;

        virtual IRate* Rate(ILabelsPtr labels) = 0;
        virtual ILazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) = 0;

        virtual IHistogram* HistogramCounter(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) = 0;

        virtual IHistogram* HistogramRate(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) = 0;
    };

    class IMetricSupplier {
    public:
        virtual ~IMetricSupplier() = default;

        virtual void Accept(TInstant time, IMetricConsumer* consumer) const = 0;
        virtual void Append(TInstant time, IMetricConsumer* consumer) const = 0;
    };

    class IMetricRegistry: public IMetricSupplier, public IMetricFactory {
    public:
        virtual const TLabels& CommonLabels() const noexcept = 0;
        virtual void RemoveMetric(const ILabels& labels) noexcept = 0;
    };


    ///////////////////////////////////////////////////////////////////////////////
    // TMetricRegistry
    ///////////////////////////////////////////////////////////////////////////////
    class TMetricRegistry: public IMetricRegistry {
    public:
        TMetricRegistry();
        ~TMetricRegistry();

        explicit TMetricRegistry(const TLabels& commonLabels);

        /**
         * Get a global metrics registry instance.
         */
        static TMetricRegistry* Instance();

        TGauge* Gauge(TLabels labels);
        TLazyGauge* LazyGauge(TLabels labels, std::function<double()> supplier);
        TIntGauge* IntGauge(TLabels labels);
        TLazyIntGauge* LazyIntGauge(TLabels labels, std::function<i64()> supplier);
        TCounter* Counter(TLabels labels);
        TLazyCounter* LazyCounter(TLabels labels, std::function<ui64()> supplier);
        TRate* Rate(TLabels labels);
        TLazyRate* LazyRate(TLabels labels, std::function<ui64()> supplier);

        THistogram* HistogramCounter(
                TLabels labels,
                IHistogramCollectorPtr collector);

        THistogram* HistogramRate(
                TLabels labels,
                IHistogramCollectorPtr collector);

        /**
         * Set all registered metrics to zero
         */
        void Reset();
        /**
         * Remove all registered metrics from registry
         */
        void Clear();

        void Accept(TInstant time, IMetricConsumer* consumer) const override;
        void Append(TInstant time, IMetricConsumer* consumer) const override;

        const TLabels& CommonLabels() const noexcept override {
            return CommonLabels_;
        }

        void RemoveMetric(const ILabels& labels) noexcept override;

    private:
        TGauge* Gauge(ILabelsPtr labels) override;
        TLazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) override;
        TIntGauge* IntGauge(ILabelsPtr labels) override;
        TLazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) override;
        TCounter* Counter(ILabelsPtr labels) override;
        TLazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) override;
        TRate* Rate(ILabelsPtr labels) override;
        TLazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) override;

        THistogram* HistogramCounter(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;

        THistogram* HistogramRate(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;

    private:
        TRWMutex Lock_;
        THashMap<ILabelsPtr, IMetricPtr> Metrics_;

        template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
        TMetric* Metric(TLabelsType&& labels, Args&&... args);

        TLabels CommonLabels_;
    };

    void WriteLabels(IMetricConsumer* consumer, const ILabels& labels);
}
