#pragma once

#include "labels.h"
#include "metric.h"

#include <util/system/rwlock.h>

#include <library/cpp/threading/light_rw_lock/lightrwlock.h>


namespace NMonitoring {

    struct TMetricOpts {
        bool MemOnly = false;
    };

    class IMetricFactory {
    public:
        virtual ~IMetricFactory() = default;

        virtual IGauge* Gauge(ILabelsPtr labels) = 0;
        virtual IGauge* GaugeWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) {
                Y_UNUSED(opts);
                return Gauge(std::move(labels));
        }

        virtual ILazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) = 0;
        virtual ILazyGauge* LazyGaugeWithOpts(ILabelsPtr labels, std::function<double()> supplier, TMetricOpts opts = {}) {
                Y_UNUSED(opts);
                return LazyGauge(std::move(labels), std::move(supplier));
        }

        virtual IIntGauge* IntGauge(ILabelsPtr labels) = 0;
        virtual IIntGauge* IntGaugeWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return IntGauge(std::move(labels));
        }
        virtual ILazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) = 0;
        virtual ILazyIntGauge* LazyIntGaugeWithOpts(ILabelsPtr labels, std::function<i64()> supplier, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return LazyIntGauge(std::move(labels), std::move(supplier));
        }

        virtual ICounter* Counter(ILabelsPtr labels) = 0;
        virtual ICounter* CounterWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return Counter(std::move(labels));
        }

        virtual ILazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) = 0;
        virtual ILazyCounter* LazyCounterWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return LazyCounter(std::move(labels), std::move(supplier));
        }

        virtual IRate* Rate(ILabelsPtr labels) = 0;
        virtual IRate* RateWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return Rate(std::move(labels));
        }

        virtual ILazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) = 0;
        virtual ILazyRate* LazyRateWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts = {}) {
            Y_UNUSED(opts);
            return LazyRate(std::move(labels), std::move(supplier));
        }

        virtual IHistogram* HistogramCounter(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) = 0;
        virtual IHistogram* HistogramCounterWithOpts(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector,
                TMetricOpts opts = {})
        {
            Y_UNUSED(opts);
            return HistogramCounter(std::move(labels), std::move(collector));
        }

        virtual IHistogram* HistogramRate(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) = 0;
        virtual IHistogram* HistogramRateWithOpts(
            ILabelsPtr labels,
            IHistogramCollectorPtr collector,
            TMetricOpts opts = {})
        {
                Y_UNUSED(opts);
                return HistogramRate(std::move(labels), std::move(collector));
        }

        virtual IHistogram* HistogramCounter(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector) = 0;
        virtual IHistogram* HistogramCounterWithOpts(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {})
        {
                Y_UNUSED(opts);
                return HistogramCounter(std::move(labels), std::move(makeHistogramCollector));
        }

        virtual IHistogram* HistogramRate(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector) = 0;
        virtual IHistogram* HistogramRateWithOpts(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {})
        {
                Y_UNUSED(opts);
                return HistogramRate(std::move(labels), std::move(makeHistogramCollector));
        }
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
        TMetricRegistry(TMetricRegistry&& other);

        ~TMetricRegistry();

        explicit TMetricRegistry(const TLabels& commonLabels);

        /**
         * Not thread-safe. There should be no concurrent operations in the registry.
         */
        TMetricRegistry& operator=(TMetricRegistry&& other);

        /**
         * Get a global metrics registry instance.
         */
        static TMetricRegistry* Instance();
        static std::shared_ptr<TMetricRegistry> SharedInstance();

        TGauge* Gauge(TLabels labels);
        TGauge* GaugeWithOpts(TLabels labels, TMetricOpts opts = {});
        TLazyGauge* LazyGauge(TLabels labels, std::function<double()> supplier);
        TLazyGauge* LazyGaugeWithOpts(TLabels labels, std::function<double()> supplier, TMetricOpts opts = {});
        TIntGauge* IntGauge(TLabels labels);
        TIntGauge* IntGaugeWithOpts(TLabels labels, TMetricOpts opts = {});
        TLazyIntGauge* LazyIntGauge(TLabels labels, std::function<i64()> supplier);
        TLazyIntGauge* LazyIntGaugeWithOpts(TLabels labels, std::function<i64()> supplier, TMetricOpts opts = {});
        TCounter* Counter(TLabels labels);
        TCounter* CounterWithOpts(TLabels labels, TMetricOpts opts = {});
        TLazyCounter* LazyCounter(TLabels labels, std::function<ui64()> supplier);
        TLazyCounter* LazyCounterWithOpts(TLabels labels, std::function<ui64()> supplier, TMetricOpts opts = {});
        TRate* Rate(TLabels labels);
        TRate* RateWithOpts(TLabels labels, TMetricOpts opts = {});
        TLazyRate* LazyRate(TLabels labels, std::function<ui64()> supplier);
        TLazyRate* LazyRateWithOpts(TLabels labels, std::function<ui64()> supplier, TMetricOpts opts = {});

        THistogram* HistogramCounter(
                TLabels labels,
                IHistogramCollectorPtr collector);
        THistogram* HistogramCounterWithOpts(
                TLabels labels,
                IHistogramCollectorPtr collector,
                TMetricOpts opts = {});

        THistogram* HistogramRate(
                TLabels labels,
                IHistogramCollectorPtr collector);
        THistogram* HistogramRateWithOpts(
                TLabels labels,
                IHistogramCollectorPtr collector,
                TMetricOpts opts = {});

        THistogram* HistogramCounter(
                TLabels labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector);
        THistogram* HistogramCounterWithOpts(
                TLabels labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {});

        THistogram* HistogramRate(
                TLabels labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector);
        THistogram* HistogramRateWithOpts(
                TLabels labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {});

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
        void Took(TInstant time, IMetricConsumer* consumer) const;

        const TLabels& CommonLabels() const noexcept override {
            return CommonLabels_;
        }

        void RemoveMetric(const ILabels& labels) noexcept override;

    private:
        TGauge* Gauge(ILabelsPtr labels) override;
        TGauge* GaugeWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) override;
        TLazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) override;
        TLazyGauge* LazyGaugeWithOpts(ILabelsPtr labels, std::function<double()> supplier, TMetricOpts opts = {}) override;
        TIntGauge* IntGauge(ILabelsPtr labels) override;
        TIntGauge* IntGaugeWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) override;
        TLazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) override;
        TLazyIntGauge* LazyIntGaugeWithOpts(ILabelsPtr labels, std::function<i64()> supplier, TMetricOpts opts = {}) override;
        TCounter* Counter(ILabelsPtr labels) override;
        TCounter* CounterWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) override;
        TLazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) override;
        TLazyCounter* LazyCounterWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts = {}) override;
        TRate* Rate(ILabelsPtr labels) override;
        TRate* RateWithOpts(ILabelsPtr labels, TMetricOpts opts = {}) override;
        TLazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) override;
        TLazyRate* LazyRateWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts = {}) override;

        THistogram* HistogramCounter(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;
        THistogram* HistogramCounterWithOpts(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector,
                TMetricOpts opts = {}) override;

        THistogram* HistogramRate(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector) override;
        THistogram* HistogramRateWithOpts(
                ILabelsPtr labels,
                IHistogramCollectorPtr collector,
                TMetricOpts opts = {}) override;

        THistogram* HistogramCounter(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector) override;
        THistogram* HistogramCounterWithOpts(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {}) override;

        THistogram* HistogramRate(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector) override;
        THistogram* HistogramRateWithOpts(
                ILabelsPtr labels,
                std::function<IHistogramCollectorPtr()> makeHistogramCollector,
                TMetricOpts opts = {}) override;

        struct TMetricValue {
            IMetricPtr Metric;
            TMetricOpts Opts;
        };

    protected:
        template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
        TMetric* Metric(TLabelsType&& labels, TMetricOpts&& opts, Args&&... args) {
            {
                TReadGuard g{*Lock_};

                auto it = Metrics_.find(labels);
                if (it != Metrics_.end()) {
                    Y_ENSURE(it->second.Metric->Type() == type, "cannot create metric " << labels
                            << " with type " << MetricTypeToStr(type)
                            << ", because registry already has same metric with type " << MetricTypeToStr(it->second.Metric->Type()));
                    Y_ENSURE(it->second.Opts.MemOnly == opts.MemOnly,"cannot create metric " << labels
                            << " with memOnly=" << opts.MemOnly
                            << ", because registry already has same metric with memOnly=" << it->second.Opts.MemOnly);
                    return static_cast<TMetric*>(it->second.Metric.Get());
                }
            }

            {
                IMetricPtr metric = MakeIntrusive<TMetric>(std::forward<Args>(args)...);

                TWriteGuard g{*Lock_};
                // decltype(Metrics_)::iterator breaks build on windows
                THashMap<ILabelsPtr, TMetricValue>::iterator it;
                TMetricValue metricValue = {metric, opts};
                if constexpr (!std::is_convertible_v<TLabelsType, ILabelsPtr>) {
                    it = Metrics_.emplace(new TLabels{std::forward<TLabelsType>(labels)}, std::move(metricValue)).first;
                } else {
                    it = Metrics_.emplace(std::forward<TLabelsType>(labels), std::move(metricValue)).first;
                }

                return static_cast<TMetric*>(it->second.Metric.Get());
            }
        }

    private:
        THolder<TRWMutex> Lock_ = MakeHolder<TRWMutex>();
        THashMap<ILabelsPtr, TMetricValue> Metrics_;

        TLabels CommonLabels_;
    };

    void WriteLabels(IMetricConsumer* consumer, const ILabels& labels);
}
