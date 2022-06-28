#pragma once

#include "metric_registry.h"

namespace NMonitoring {

/**
 * This registry is wrapping given delegate registry to add common labels
 * to all created metrics through this sub registry.
 */
class TMetricSubRegistry final: public IMetricRegistry {
public:
    /**
     * Do not keep ownership of the given delegate.
     */
    TMetricSubRegistry(TLabels commonLabels, IMetricRegistry* delegate) noexcept
        : CommonLabels_{std::move(commonLabels)}
        , DelegatePtr_{delegate}
    {
    }

    /**
     * Keeps ownership of the given delegate.
     */
    TMetricSubRegistry(TLabels commonLabels, std::shared_ptr<IMetricRegistry> delegate) noexcept
        : CommonLabels_{std::move(commonLabels)}
        , Delegate_{std::move(delegate)}
        , DelegatePtr_{Delegate_.get()}
    {
    }

    IGauge* Gauge(ILabelsPtr labels) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->Gauge(std::move(labels));
    }

    ILazyGauge* LazyGauge(ILabelsPtr labels, std::function<double()> supplier) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->LazyGauge(std::move(labels), std::move(supplier));
    }

    IIntGauge* IntGauge(ILabelsPtr labels) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->IntGauge(std::move(labels));
    }

    ILazyIntGauge* LazyIntGauge(ILabelsPtr labels, std::function<i64()> supplier) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->LazyIntGauge(std::move(labels), std::move(supplier));
    }

    ICounter* Counter(ILabelsPtr labels) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->Counter(std::move(labels));
    }

    ILazyCounter* LazyCounter(ILabelsPtr labels, std::function<ui64()> supplier) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->LazyCounter(std::move(labels), std::move(supplier));
    }

    IRate* Rate(ILabelsPtr labels) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->Rate(std::move(labels));
    }

    ILazyRate* LazyRate(ILabelsPtr labels, std::function<ui64()> supplier) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->LazyRate(std::move(labels), std::move(supplier));
    }

    IHistogram* HistogramCounter(ILabelsPtr labels, IHistogramCollectorPtr collector) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->HistogramCounter(std::move(labels), std::move(collector));
    }

    IHistogram* HistogramRate(ILabelsPtr labels, IHistogramCollectorPtr collector) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->HistogramRate(std::move(labels), std::move(collector));
    }

    IHistogram* HistogramCounter(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> collector) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->HistogramCounter(std::move(labels), std::move(collector));
    }

    IHistogram* HistogramRate(ILabelsPtr labels, std::function<IHistogramCollectorPtr()> collector) override {
        AddCommonLabels(labels.Get());
        return DelegatePtr_->HistogramRate(std::move(labels), std::move(collector));
    }

    void Accept(TInstant time, IMetricConsumer* consumer) const override {
        DelegatePtr_->Accept(time, consumer);
    }

    void Append(TInstant time, IMetricConsumer* consumer) const override {
        DelegatePtr_->Append(time, consumer);
    }

    const TLabels& CommonLabels() const noexcept override {
        return CommonLabels_;
    }

    void RemoveMetric(const ILabels& labels) noexcept override {
        TLabelsImpl<TStringBuf> toRemove;
        for (auto& l: labels) {
            toRemove.Add(l);
        }
        AddCommonLabels(&toRemove);
        DelegatePtr_->RemoveMetric(toRemove);
    }

private:
    void AddCommonLabels(ILabels* labels) const {
        for (auto& label: CommonLabels_) {
            labels->Add(label);
        }
    }

private:
    const TLabels CommonLabels_;
    std::shared_ptr<IMetricRegistry> Delegate_;
    IMetricRegistry* DelegatePtr_;
};

} // namespace NMonitoring
