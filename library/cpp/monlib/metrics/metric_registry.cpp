#include "metric_registry.h"

#include <memory>

namespace NMonitoring {
    namespace {
        void ConsumeLabels(IMetricConsumer* consumer, const ILabels& labels) {
            for (auto&& label: labels) {
                consumer->OnLabel(label.Name(), label.Value());
            }
        }

        template <typename TLabelsConsumer>
        void ConsumeMetric(TInstant time, IMetricConsumer* consumer, IMetric* metric, TLabelsConsumer&& labelsConsumer, TMetricOpts opts = {}) {
            consumer->OnMetricBegin(metric->Type());

            // (1) add labels
            consumer->OnLabelsBegin();
            labelsConsumer();
            consumer->OnLabelsEnd();
            // (2) add time and value
            metric->Accept(time, consumer);
            // (3) add flag
            consumer->OnMemOnly(opts.MemOnly);
            consumer->OnMetricEnd();
        }
    }

    void WriteLabels(IMetricConsumer* consumer, const ILabels& labels) {
        consumer->OnLabelsBegin();
        ConsumeLabels(consumer, labels);
        consumer->OnLabelsEnd();
    }

    TMetricRegistry::TMetricRegistry() = default;
    TMetricRegistry::TMetricRegistry(TMetricRegistry&& other) = default;

    TMetricRegistry::~TMetricRegistry() = default;

    TMetricRegistry::TMetricRegistry(const TLabels& commonLabels)
        : TMetricRegistry{}
    {
        CommonLabels_ = commonLabels;
    }

    TMetricRegistry& TMetricRegistry::operator=(TMetricRegistry&& other) = default;

    TMetricRegistry* TMetricRegistry::Instance() {
        //return SharedInstance().get();
        return Singleton<TMetricRegistry>();
    }

    std::shared_ptr<TMetricRegistry> TMetricRegistry::SharedInstance() {
        static auto instance(std::make_shared<TMetricRegistry>());
        return instance;
    }

    TGauge* TMetricRegistry::Gauge(TLabels labels) {
        return GaugeWithOpts(std::move(labels));
    }
    TGauge* TMetricRegistry::GaugeWithOpts(TLabels labels, TMetricOpts opts) {
        return Metric<TGauge, EMetricType::GAUGE>(std::move(labels), std::move(opts));
    }

    TGauge* TMetricRegistry::Gauge(ILabelsPtr labels) {
        return GaugeWithOpts(std::move(labels));
    }
    TGauge* TMetricRegistry::GaugeWithOpts(ILabelsPtr labels, TMetricOpts opts) {
        return Metric<TGauge, EMetricType::GAUGE>(std::move(labels), std::move(opts));
    }

    TLazyGauge* TMetricRegistry::LazyGauge(TLabels labels, std::function<double()> supplier) {
        return LazyGaugeWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyGauge* TMetricRegistry::LazyGaugeWithOpts(TLabels labels, std::function<double()> supplier, TMetricOpts opts) {
        return Metric<TLazyGauge, EMetricType::GAUGE>(std::move(labels),std::move(opts), std::move(supplier));
    }

    TLazyGauge* TMetricRegistry::LazyGauge(ILabelsPtr labels, std::function<double()> supplier) {
        return LazyGaugeWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyGauge* TMetricRegistry::LazyGaugeWithOpts(ILabelsPtr labels, std::function<double()> supplier, TMetricOpts opts) {
        return Metric<TLazyGauge, EMetricType::GAUGE>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TIntGauge* TMetricRegistry::IntGauge(TLabels labels) {
        return IntGaugeWithOpts(std::move(labels));
    }
    TIntGauge* TMetricRegistry::IntGaugeWithOpts(TLabels labels, TMetricOpts opts) {
        return Metric<TIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(opts));
    }

    TIntGauge* TMetricRegistry::IntGauge(ILabelsPtr labels) {
        return IntGaugeWithOpts(std::move(labels));
    }
    TIntGauge* TMetricRegistry::IntGaugeWithOpts(ILabelsPtr labels, TMetricOpts opts) {
        return Metric<TIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(opts));
    }

    TLazyIntGauge* TMetricRegistry::LazyIntGauge(TLabels labels, std::function<i64()> supplier) {
        return LazyIntGaugeWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyIntGauge* TMetricRegistry::LazyIntGaugeWithOpts(TLabels labels, std::function<i64()> supplier, TMetricOpts opts) {
        return Metric<TLazyIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TLazyIntGauge* TMetricRegistry::LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) {
        return LazyIntGaugeWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyIntGauge* TMetricRegistry::LazyIntGaugeWithOpts(ILabelsPtr labels, std::function<i64()> supplier, TMetricOpts opts) {
        return Metric<TLazyIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TCounter* TMetricRegistry::Counter(TLabels labels) {
        return CounterWithOpts(std::move(labels));
    }
    TCounter* TMetricRegistry::CounterWithOpts(TLabels labels, TMetricOpts opts) {
        return Metric<TCounter, EMetricType::COUNTER>(std::move(labels), std::move(opts));
    }

    TCounter* TMetricRegistry::Counter(ILabelsPtr labels) {
        return CounterWithOpts(std::move(labels));
    }
    TCounter* TMetricRegistry::CounterWithOpts(ILabelsPtr labels, TMetricOpts opts) {
        return Metric<TCounter, EMetricType::COUNTER>(std::move(labels), std::move(opts));
    }

    TLazyCounter* TMetricRegistry::LazyCounter(TLabels labels, std::function<ui64()> supplier) {
        return LazyCounterWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyCounter* TMetricRegistry::LazyCounterWithOpts(TLabels labels, std::function<ui64()> supplier, TMetricOpts opts) {
        return Metric<TLazyCounter, EMetricType::COUNTER>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TLazyCounter* TMetricRegistry::LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) {
        return LazyCounterWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyCounter* TMetricRegistry::LazyCounterWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts) {
        return Metric<TLazyCounter, EMetricType::COUNTER>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TRate* TMetricRegistry::Rate(TLabels labels) {
        return RateWithOpts(std::move(labels));
    }
    TRate* TMetricRegistry::RateWithOpts(TLabels labels, TMetricOpts opts) {
        return Metric<TRate, EMetricType::RATE>(std::move(labels), std::move(opts));
    }

    TRate* TMetricRegistry::Rate(ILabelsPtr labels) {
        return RateWithOpts(std::move(labels));
    }
    TRate* TMetricRegistry::RateWithOpts(ILabelsPtr labels, TMetricOpts opts) {
        return Metric<TRate, EMetricType::RATE>(std::move(labels), std::move(opts));
    }

    TLazyRate* TMetricRegistry::LazyRate(TLabels labels, std::function<ui64()> supplier) {
        return LazyRateWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyRate* TMetricRegistry::LazyRateWithOpts(TLabels labels, std::function<ui64()> supplier, TMetricOpts opts) {
        return Metric<TLazyRate, EMetricType::RATE>(std::move(labels), std::move(opts), std::move(supplier));
    }

    TLazyRate* TMetricRegistry::LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) {
        return LazyRateWithOpts(std::move(labels), std::move(supplier));
    }
    TLazyRate* TMetricRegistry::LazyRateWithOpts(ILabelsPtr labels, std::function<ui64()> supplier, TMetricOpts opts) {
        return Metric<TLazyRate, EMetricType::RATE>(std::move(labels), std::move(opts), std::move(supplier));
    }

    THistogram* TMetricRegistry::HistogramCounter(TLabels labels, IHistogramCollectorPtr collector) {
        return HistogramCounterWithOpts(std::move(labels), std::move(collector));
    }
    THistogram* TMetricRegistry::HistogramCounterWithOpts(TLabels labels, IHistogramCollectorPtr collector, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(opts), std::move(collector), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        return HistogramCounterWithOpts(std::move(labels), std::move(collector));
    }
    THistogram* TMetricRegistry::HistogramCounterWithOpts(ILabelsPtr labels, IHistogramCollectorPtr collector, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(opts), std::move(collector), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(TLabels labels, std::function<IHistogramCollectorPtr()> supplier) {
        return HistogramCounterWithOpts(std::move(labels), std::move(supplier));
    }
    THistogram* TMetricRegistry::HistogramCounterWithOpts(TLabels labels, std::function<IHistogramCollectorPtr()> supplier, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(opts), std::move(supplier), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier) {
        return HistogramCounterWithOpts(std::move(labels), std::move(supplier));
    }
    THistogram* TMetricRegistry::HistogramCounterWithOpts(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(opts), std::move(supplier), false);
    }

    THistogram* TMetricRegistry::HistogramRate(TLabels labels, IHistogramCollectorPtr collector) {
        return HistogramRateWithOpts(std::move(labels), std::move(collector));
    }
    THistogram* TMetricRegistry::HistogramRateWithOpts(TLabels labels, IHistogramCollectorPtr collector, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(opts), std::move(collector), true);
    }

    THistogram* TMetricRegistry::HistogramRate(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        return HistogramRateWithOpts(std::move(labels), std::move(collector));
    }
    THistogram* TMetricRegistry::HistogramRateWithOpts(ILabelsPtr labels, IHistogramCollectorPtr collector, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(opts), std::move(collector), true);
    }

    THistogram* TMetricRegistry::HistogramRate(TLabels labels, std::function<IHistogramCollectorPtr()> supplier) {
        return HistogramRateWithOpts(std::move(labels), std::move(supplier));
    }
    THistogram* TMetricRegistry::HistogramRateWithOpts(TLabels labels, std::function<IHistogramCollectorPtr()> supplier, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(opts), std::move(supplier), true);
    }

    THistogram* TMetricRegistry::HistogramRate(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier) {
        return HistogramRateWithOpts(std::move(labels), std::move(supplier));
    }
    THistogram* TMetricRegistry::HistogramRateWithOpts(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier, TMetricOpts opts) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(opts), std::move(supplier), true);
    }

    void TMetricRegistry::Reset() {
        TWriteGuard g{*Lock_};
        for (auto& [label, metricValue] : Metrics_) {
            metricValue.Metric->Reset();
        }
    }

    void TMetricRegistry::Clear() {
        TWriteGuard g{*Lock_};
        Metrics_.clear();
    }

    void TMetricRegistry::RemoveMetric(const ILabels& labels) noexcept {
        TWriteGuard g{*Lock_};
        Metrics_.erase(labels);
    }

    void TMetricRegistry::Accept(TInstant time, IMetricConsumer* consumer) const {
        consumer->OnStreamBegin();

        if (!CommonLabels_.Empty()) {
            consumer->OnLabelsBegin();
            ConsumeLabels(consumer, CommonLabels_);
            consumer->OnLabelsEnd();
        }

        TVector<std::pair<ILabelsPtr, TMetricValue>> tmpMetrics;

        {
            TReadGuard g{*Lock_};
            tmpMetrics.reserve(Metrics_.size());
            for (const auto& it: Metrics_) {
                tmpMetrics.push_back(it);
            }
        }

        for (const auto& it: tmpMetrics) {
            ILabels* labels = it.first.Get();
            IMetric* metric = it.second.Metric.Get();
            TMetricOpts opts = it.second.Opts;
            ConsumeMetric(
                time,
                consumer,
                metric,
                [&]() {
                    ConsumeLabels(consumer, *labels);
                },
                opts
            );
        }

        consumer->OnStreamEnd();
    }

    void TMetricRegistry::Append(TInstant time, IMetricConsumer* consumer) const {
        TReadGuard g{*Lock_};

        for (const auto& it: Metrics_) {
            ILabels* labels = it.first.Get();
            IMetric* metric = it.second.Metric.Get();
            TMetricOpts opts = it.second.Opts;
            ConsumeMetric(
                time,
                consumer,
                metric,
                [&]() {
                    ConsumeLabels(consumer, CommonLabels_);
                    ConsumeLabels(consumer, *labels);
                },
                opts
            );
        }
    }

    void TMetricRegistry::Took(TInstant time, IMetricConsumer* consumer) const {
        consumer->OnStreamBegin();
        TReadGuard g{*Lock_};

        for (const auto& it: Metrics_) {
            ILabels* labels = it.first.Get();
            IMetric* metric = it.second.Metric.Get();
            TMetricOpts opts = it.second.Opts;
            ConsumeMetric(
                time,
                consumer,
                metric,
                [&]() {
                    ConsumeLabels(consumer, CommonLabels_);
                    ConsumeLabels(consumer, *labels);
                },
                opts
            );
            metric->Reset();
        }
        consumer->OnStreamEnd();
    }
}
