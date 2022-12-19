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
        void ConsumeMetric(TInstant time, IMetricConsumer* consumer, IMetric* metric, TLabelsConsumer&& labelsConsumer) {
            consumer->OnMetricBegin(metric->Type());

            // (1) add labels
            consumer->OnLabelsBegin();
            labelsConsumer();
            consumer->OnLabelsEnd();

            // (2) add time and value
            metric->Accept(time, consumer);
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
        return Metric<TGauge, EMetricType::GAUGE>(std::move(labels));
    }

    TGauge* TMetricRegistry::Gauge(ILabelsPtr labels) {
        return Metric<TGauge, EMetricType::GAUGE>(std::move(labels));
    }

    TLazyGauge* TMetricRegistry::LazyGauge(TLabels labels, std::function<double()> supplier) {
        return Metric<TLazyGauge, EMetricType::GAUGE>(std::move(labels), std::move(supplier));
    }

    TLazyGauge* TMetricRegistry::LazyGauge(ILabelsPtr labels, std::function<double()> supplier) {
        return Metric<TLazyGauge, EMetricType::GAUGE>(std::move(labels), std::move(supplier));
    }

    TIntGauge* TMetricRegistry::IntGauge(TLabels labels) {
        return Metric<TIntGauge, EMetricType::IGAUGE>(std::move(labels));
    }

    TIntGauge* TMetricRegistry::IntGauge(ILabelsPtr labels) {
        return Metric<TIntGauge, EMetricType::IGAUGE>(std::move(labels));
    }

    TLazyIntGauge* TMetricRegistry::LazyIntGauge(TLabels labels, std::function<i64()> supplier) {
        return Metric<TLazyIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(supplier));
    }

    TLazyIntGauge* TMetricRegistry::LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) {
        return Metric<TLazyIntGauge, EMetricType::IGAUGE>(std::move(labels), std::move(supplier));
    }

    TCounter* TMetricRegistry::Counter(TLabels labels) {
        return Metric<TCounter, EMetricType::COUNTER>(std::move(labels));
    }

    TCounter* TMetricRegistry::Counter(ILabelsPtr labels) {
        return Metric<TCounter, EMetricType::COUNTER>(std::move(labels));
    }

    TLazyCounter* TMetricRegistry::LazyCounter(TLabels labels, std::function<ui64()> supplier) {
        return Metric<TLazyCounter, EMetricType::COUNTER>(std::move(labels), std::move(supplier));
    }

    TLazyCounter* TMetricRegistry::LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) {
        return Metric<TLazyCounter, EMetricType::COUNTER>(std::move(labels), std::move(supplier));
    }

    TRate* TMetricRegistry::Rate(TLabels labels) {
        return Metric<TRate, EMetricType::RATE>(std::move(labels));
    }

    TRate* TMetricRegistry::Rate(ILabelsPtr labels) {
        return Metric<TRate, EMetricType::RATE>(std::move(labels));
    }

    TLazyRate* TMetricRegistry::LazyRate(TLabels labels, std::function<ui64()> supplier) {
        return Metric<TLazyRate, EMetricType::RATE>(std::move(labels), std::move(supplier));
    }

    TLazyRate* TMetricRegistry::LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) {
        return Metric<TLazyRate, EMetricType::RATE>(std::move(labels), std::move(supplier));
    }

    THistogram* TMetricRegistry::HistogramCounter(TLabels labels, IHistogramCollectorPtr collector) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(collector), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(collector), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(TLabels labels, std::function<IHistogramCollectorPtr()> supplier) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(supplier), false);
    }

    THistogram* TMetricRegistry::HistogramCounter(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier) {
        return Metric<THistogram, EMetricType::HIST>(std::move(labels), std::move(supplier), false);
    }

    THistogram* TMetricRegistry::HistogramRate(TLabels labels, IHistogramCollectorPtr collector) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(collector), true);
    }

    THistogram* TMetricRegistry::HistogramRate(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(collector), true);
    }

    THistogram* TMetricRegistry::HistogramRate(TLabels labels, std::function<IHistogramCollectorPtr()> supplier) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(supplier), true);
    }

    THistogram* TMetricRegistry::HistogramRate(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> supplier) {
        return Metric<THistogram, EMetricType::HIST_RATE>(std::move(labels), std::move(supplier), true);
    }

    void TMetricRegistry::Reset() {
        TWriteGuard g{*Lock_};
        for (auto& [label, metric] : Metrics_) {
            switch (metric->Type()) {
            case EMetricType::GAUGE:
                static_cast<TGauge*>(metric.Get())->Set(.0);
                break;
            case EMetricType::IGAUGE:
                static_cast<TIntGauge*>(metric.Get())->Set(0);
                break;
            case EMetricType::COUNTER:
                static_cast<TCounter*>(metric.Get())->Reset();
                break;
            case EMetricType::RATE:
                static_cast<TRate*>(metric.Get())->Reset();
                break;
            case EMetricType::HIST:
            case EMetricType::HIST_RATE:
                static_cast<THistogram*>(metric.Get())->Reset();
                break;
            case EMetricType::UNKNOWN:
            case EMetricType::DSUMMARY:
            case EMetricType::LOGHIST:
                break;
            }
        }
    }

    void TMetricRegistry::Clear() {
        TWriteGuard g{*Lock_};
        Metrics_.clear();
    }

    template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
    TMetric* TMetricRegistry::Metric(TLabelsType&& labels, Args&&... args) {
        {
            TReadGuard g{*Lock_};

            auto it = Metrics_.find(labels);
            if (it != Metrics_.end()) {
                Y_ENSURE(it->second->Type() == type, "cannot create metric " << labels
                        << " with type " << MetricTypeToStr(type)
                        << ", because registry already has same metric with type " << MetricTypeToStr(it->second->Type()));
                return static_cast<TMetric*>(it->second.Get());
            }
        }

        {
            IMetricPtr metric = MakeIntrusive<TMetric>(std::forward<Args>(args)...);

            TWriteGuard g{*Lock_};
            // decltype(Metrics_)::iterator breaks build on windows
            THashMap<ILabelsPtr, IMetricPtr>::iterator it;
            if constexpr (!std::is_convertible_v<TLabelsType, ILabelsPtr>) {
                it = Metrics_.emplace(new TLabels{std::forward<TLabelsType>(labels)}, std::move(metric)).first;
            } else {
                it = Metrics_.emplace(std::forward<TLabelsType>(labels), std::move(metric)).first;
            }

            return static_cast<TMetric*>(it->second.Get());
        }
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

        TVector<std::pair<ILabelsPtr, IMetricPtr>> tmpMetrics;

        {
            TReadGuard g{*Lock_};
            tmpMetrics.reserve(Metrics_.size());
            for (const auto& it: Metrics_) {
                tmpMetrics.push_back(it);
            }
        }

        for (const auto& it: tmpMetrics) {
            ILabels* labels = it.first.Get();
            IMetric* metric = it.second.Get();
            ConsumeMetric(time, consumer, metric, [&]() {
                ConsumeLabels(consumer, *labels);
            });
        }

        consumer->OnStreamEnd();
    }

    void TMetricRegistry::Append(TInstant time, IMetricConsumer* consumer) const {
        TReadGuard g{*Lock_};

        for (const auto& it: Metrics_) {
            ILabels* labels = it.first.Get();
            IMetric* metric = it.second.Get();
            ConsumeMetric(time, consumer, metric, [&]() {
                ConsumeLabels(consumer, CommonLabels_);
                ConsumeLabels(consumer, *labels);
            });
        }
    }
}
