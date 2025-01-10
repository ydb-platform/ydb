#pragma once

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NMonitoring {

struct TCounterLocation {
    TIntrusivePtr<TDynamicCounters> Root;
    TString Value;
};

class TRateCounterAdapter : public IRate {
public:
    TRateCounterAdapter(TCounterLocation location)
        : Counter(location.Root->GetCounter(location.Value, true))
    {
    }

    ui64 Add(ui64 n) noexcept override {
        return Counter->Add(n);
    }

    ui64 Get() const noexcept override {
        return Counter->Val();
    }

    void Reset() noexcept override {
        Counter->Set(0);
    }

    void Accept(TInstant time, IMetricConsumer* consumer) const override {
        consumer->OnUint64(time, Get());
    }

    TDynamicCounters::TCounterPtr Counter;
};

class TRateHistogramAdapter : public IHistogram {
public:
    TRateHistogramAdapter(TCounterLocation location, IHistogramCollectorPtr collector)
        : IHistogram(true)
        , Histogram(location.Root->GetHistogram(location.Value, std::move(collector)))
    {
    }

    void Record(double value) noexcept override {
        Histogram->Collect(value, 1);
    }

    void Record(double value, ui32 count) noexcept override {
        Histogram->Collect(value, count);
    }

    IHistogramSnapshotPtr TakeSnapshot() const override {
        return Histogram->Snapshot();
    }

    void Reset() noexcept override {
        Histogram->Reset();
    }

    void Accept(TInstant time, IMetricConsumer* consumer) const override {
        consumer->OnHistogram(time, TakeSnapshot());
    }

    THistogramPtr Histogram;
};

class TMetricFactoryForDynamicCounters : public IMetricFactory {
public:
    TMetricFactoryForDynamicCounters(TIntrusivePtr<TDynamicCounters> root)
        : Root(std::move(root))
    {
    }

    TCounterLocation GetRoot(ILabelsPtr labels) const {
        TIntrusivePtr<TDynamicCounters> current = Root;
        TString value;
        for (const auto& label : *labels) {
            if (label.Name() == "direction") {
                continue;
            }
            if (label.Name() == "peer") {
                continue;
            }
            if (label.Name() == "sensor") {
                value = label.Value();
                continue;
            }
            current = current->GetSubgroup(TString(label.Name()), TString(label.Value()));
        }
        return {
            .Root = current,
            .Value = value
        };
    }

    IGauge* Gauge(ILabelsPtr labels) override {
        Y_UNUSED(labels);
        Y_ABORT("Not implemented");
    }

    ILazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) override {
        Y_UNUSED(labels);
        Y_UNUSED(supplier);
        Y_ABORT("Not implemented");
    }

    IIntGauge* IntGauge(ILabelsPtr labels) override {
        Y_UNUSED(labels);
        Y_ABORT("Not implemented");
    }

    ILazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) override {
        Y_UNUSED(labels);
        Y_UNUSED(supplier);
        Y_ABORT("Not implemented");
    }

    ICounter* Counter(ILabelsPtr labels) override {
        Y_UNUSED(labels);
        Y_ABORT("Not implemented");
    }

    ILazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) override {
        Y_UNUSED(labels);
        Y_UNUSED(supplier);
        Y_ABORT("Not implemented");
    }

    IRate* Rate(ILabelsPtr labels) override {
        return Metric<TRateCounterAdapter, EMetricType::RATE>(labels);
    }

    ILazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) override {
        Y_UNUSED(labels);
        Y_UNUSED(supplier);
        Y_ABORT("Not implemented");
    }

    IHistogram* HistogramCounter(ILabelsPtr labels, IHistogramCollectorPtr collector) override {
        Y_UNUSED(labels);
        Y_UNUSED(collector);
        Y_ABORT("Not implemented");
    }

    IHistogram* HistogramRate(ILabelsPtr labels, IHistogramCollectorPtr collector) override {
        return Metric<TRateHistogramAdapter, EMetricType::HIST_RATE>(labels, std::move(collector));
    }

    IHistogram* HistogramCounter(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> makeHistogramCollector) override {
        Y_UNUSED(labels);
        Y_UNUSED(makeHistogramCollector);
        Y_ABORT("Not implemented");
    }

    IHistogram* HistogramRate(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> makeHistogramCollector) override {
        Y_UNUSED(labels);
        Y_UNUSED(makeHistogramCollector);
        Y_ABORT("Not implemented");
    }

    // copy-paste from metrics_registry.cpp, can't reuse it because of private members
    template <typename TMetric, EMetricType type, typename TLabelsType, typename... Args>
    TMetric* Metric(TLabelsType&& labels, Args&&... args) {
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
            IMetricPtr metric = MakeIntrusive<TMetric>(GetRoot(labels), std::forward<Args>(args)...);

            TWriteGuard g{Lock_};
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

    TIntrusivePtr<TDynamicCounters> Root;
    TRWMutex Lock_;
    THashMap<ILabelsPtr, IMetricPtr> Metrics_;
};


}
