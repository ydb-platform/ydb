#include "fake.h"

namespace NMonitoring {

    IGauge* TFakeMetricRegistry::Gauge(ILabelsPtr labels) {
        return Metric<TFakeGauge, EMetricType::GAUGE>(std::move(labels));
    }

    ILazyGauge* TFakeMetricRegistry::LazyGauge(ILabelsPtr labels, std::function<double()> supplier) {
        Y_UNUSED(supplier);
        return Metric<TFakeLazyGauge, EMetricType::GAUGE>(std::move(labels));
    }

    IIntGauge* TFakeMetricRegistry::IntGauge(ILabelsPtr labels) {
        return Metric<TFakeIntGauge, EMetricType::IGAUGE>(std::move(labels));
    }

    ILazyIntGauge* TFakeMetricRegistry::LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) {
        Y_UNUSED(supplier);
        return Metric<TFakeLazyIntGauge, EMetricType::IGAUGE>(std::move(labels));
    }

    ICounter* TFakeMetricRegistry::Counter(ILabelsPtr labels) {
        return Metric<TFakeCounter, EMetricType::COUNTER>(std::move(labels));
    }

    ILazyCounter* TFakeMetricRegistry::LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) {
        Y_UNUSED(supplier);
        return Metric<TFakeLazyCounter, EMetricType::COUNTER>(std::move(labels));
    }

    IRate* TFakeMetricRegistry::Rate(ILabelsPtr labels) {
        return Metric<TFakeRate, EMetricType::RATE>(std::move(labels));
    }

    ILazyRate* TFakeMetricRegistry::LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) {
        Y_UNUSED(supplier);
        return Metric<TFakeLazyRate, EMetricType::RATE>(std::move(labels));
    }

    IHistogram* TFakeMetricRegistry::HistogramCounter(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        Y_UNUSED(collector);
        return Metric<TFakeHistogram, EMetricType::HIST>(std::move(labels), false);
    }

    IHistogram* TFakeMetricRegistry::HistogramCounter(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> collector) {
        Y_UNUSED(collector);
        return Metric<TFakeHistogram, EMetricType::HIST>(std::move(labels), false);
    }

    void TFakeMetricRegistry::RemoveMetric(const ILabels& labels) noexcept {
        TWriteGuard g{Lock_};
        Metrics_.erase(labels);
    }

    void TFakeMetricRegistry::Accept(TInstant time, IMetricConsumer* consumer) const {
        Y_UNUSED(time);
        consumer->OnStreamBegin();
        consumer->OnStreamEnd();
    }

    IHistogram* TFakeMetricRegistry::HistogramRate(ILabelsPtr labels, IHistogramCollectorPtr collector) {
        Y_UNUSED(collector);
        return Metric<TFakeHistogram, EMetricType::HIST_RATE>(std::move(labels), true);
    }

    IHistogram* TFakeMetricRegistry::HistogramRate(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> collector) {
        Y_UNUSED(collector);
        return Metric<TFakeHistogram, EMetricType::HIST_RATE>(std::move(labels), true);
    }

    void TFakeMetricRegistry::Append(TInstant time, IMetricConsumer* consumer) const {
        Y_UNUSED(time, consumer);
    }

    const TLabels& TFakeMetricRegistry::CommonLabels() const noexcept {
        return CommonLabels_;
    }

    template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
    TMetric* TFakeMetricRegistry::Metric(TLabelsType&& labels, Args&&... args) {
        {
            TReadGuard g{Lock_};

            auto it = Metrics_.find(labels);
            if (it != Metrics_.end()) {
                Y_ENSURE(it->second->Type() == type, "cannot create metric " << labels
                        << " with type " << MetricTypeToStr(type)
                        << ", because registry already has same metric with type " << MetricTypeToStr(it->second->Type()));
                return static_cast<TMetric*>(it->second.Get());
            }
        }

        {
            TWriteGuard g{Lock_};

            IMetricPtr metric = MakeIntrusive<TMetric>(std::forward<Args>(args)...);

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
} // namespace NMonitoring
