#pragma once

#include "metric_consumer.h"

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NMonitoring {
    ///////////////////////////////////////////////////////////////////////////////
    // IMetric
    ///////////////////////////////////////////////////////////////////////////////
    class IMetric : public TThrRefBase {
    public:
        virtual ~IMetric() = default;

        virtual EMetricType Type() const noexcept = 0;
        virtual void Accept(TInstant time, IMetricConsumer* consumer) const = 0;
    };

    using IMetricPtr = TIntrusivePtr<IMetric>;

    class IGauge: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return  EMetricType::GAUGE;
        }

        virtual double Add(double n) noexcept = 0;
        virtual void Set(double n) noexcept = 0;
        virtual double Get() const noexcept = 0;
        virtual void Reset() noexcept {
            Set(0);
        }
    };

    class ILazyGauge: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return  EMetricType::GAUGE;
        }
        virtual double Get() const noexcept = 0;
    };

    class IIntGauge: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::IGAUGE;
        }

        virtual i64 Add(i64 n) noexcept = 0;
        virtual i64 Inc() noexcept {
            return Add(1);
        }

        virtual i64 Dec() noexcept {
            return Add(-1);
        }

        virtual void Set(i64 value) noexcept = 0;
        virtual i64 Get() const noexcept = 0;
        virtual void Reset() noexcept {
            Set(0);
        }
    };

    class ILazyIntGauge: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::IGAUGE;
        }

        virtual i64 Get() const noexcept = 0;
    };

    class ICounter: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::COUNTER;
        }

        virtual ui64 Inc() noexcept {
            return Add(1);
        }

        virtual ui64 Add(ui64 n) noexcept = 0;
        virtual ui64 Get() const noexcept = 0;
        virtual void Reset() noexcept = 0;
    };

    class ILazyCounter: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::COUNTER;
        }

        virtual ui64 Get() const noexcept = 0;
    };

    class IRate: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::RATE;
        }

        virtual ui64 Inc() noexcept {
            return Add(1);
        }

        virtual ui64 Add(ui64 n) noexcept = 0;
        virtual ui64 Get() const noexcept = 0;
        virtual void Reset() noexcept = 0;
    };

    class ILazyRate: public IMetric {
    public:
        EMetricType Type() const noexcept final {
            return EMetricType::RATE;
        }

        virtual ui64 Get() const noexcept = 0;
    };

    class IHistogram: public IMetric {
    public:
        explicit IHistogram(bool isRate)
            : IsRate_{isRate}
        {
        }

        EMetricType Type() const noexcept final {
            return IsRate_ ? EMetricType::HIST_RATE : EMetricType::HIST;
        }

        virtual void Record(double value) = 0;
        virtual void Record(double value, ui32 count) = 0;
        virtual IHistogramSnapshotPtr TakeSnapshot() const = 0;
        virtual void Reset() = 0;

    protected:
        const bool IsRate_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TGauge
    ///////////////////////////////////////////////////////////////////////////////
    class TGauge final: public IGauge {
    public:
        explicit TGauge(double value = 0.0) {
            Set(value);
        }

        double Add(double n) noexcept override {
            double newValue;
            double oldValue = Get();

            do {
                newValue = oldValue + n;
            } while (!Value_.compare_exchange_weak(oldValue, newValue, std::memory_order_release, std::memory_order_consume));

            return newValue;
        }

        void Set(double n) noexcept override {
            Value_.store(n, std::memory_order_relaxed);
        }

        double Get() const noexcept override {
            return Value_.load(std::memory_order_relaxed);
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnDouble(time, Get());
        }

    private:
        std::atomic<double> Value_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TLazyGauge
    ///////////////////////////////////////////////////////////////////////////////
    class TLazyGauge final: public ILazyGauge {
    public:
        explicit TLazyGauge(std::function<double()> supplier)
            : Supplier_(std::move(supplier))
        {
        }

        double Get() const noexcept override {
            return Supplier_();
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnDouble(time, Get());
        }

    private:
        std::function<double()> Supplier_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TIntGauge
    ///////////////////////////////////////////////////////////////////////////////
    class TIntGauge final: public IIntGauge {
    public:
        explicit TIntGauge(i64 value = 0) {
            Set(value);
        }

        i64 Add(i64 n) noexcept override {
            return Value_.fetch_add(n, std::memory_order_relaxed) + n;
        }

        void Set(i64 value) noexcept override {
            Value_.store(value, std::memory_order_relaxed);
        }

        i64 Get() const noexcept override {
            return Value_.load(std::memory_order_relaxed);
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnInt64(time, Get());
        }

    private:
        std::atomic_int64_t Value_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TLazyIntGauge
    ///////////////////////////////////////////////////////////////////////////////
    class TLazyIntGauge final: public ILazyIntGauge {
    public:
        explicit TLazyIntGauge(std::function<i64()> supplier)
            : Supplier_(std::move(supplier))
        {
        }

        i64 Get() const noexcept override {
            return Supplier_();
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnInt64(time, Get());
        }

    private:
        std::function<i64()> Supplier_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TCounter
    ///////////////////////////////////////////////////////////////////////////////
    class TCounter final: public ICounter {
    public:
        explicit TCounter(ui64 value = 0) {
            Value_.store(value, std::memory_order_relaxed);
        }

        ui64 Add(ui64 n) noexcept override {
            return Value_.fetch_add(n, std::memory_order_relaxed) + n;
        }

        ui64 Get() const noexcept override {
            return Value_.load(std::memory_order_relaxed);
        }

        void Reset() noexcept override {
            Value_.store(0, std::memory_order_relaxed);
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnUint64(time, Get());
        }

    private:
        std::atomic_uint64_t Value_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TLazyCounter
    ///////////////////////////////////////////////////////////////////////////////
    class TLazyCounter final: public ILazyCounter {
    public:
        explicit TLazyCounter(std::function<ui64()> supplier)
            : Supplier_(std::move(supplier))
        {
        }

        ui64 Get() const noexcept override {
            return Supplier_();
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnUint64(time, Get());
        }

    private:
        std::function<ui64()> Supplier_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TRate
    ///////////////////////////////////////////////////////////////////////////////
    class TRate final: public IRate {
    public:
        explicit TRate(ui64 value = 0) {
            Value_.store(value, std::memory_order_relaxed);
        }

        ui64 Add(ui64 n) noexcept override {
            return Value_.fetch_add(n, std::memory_order_relaxed) + n;
        }

        ui64 Get() const noexcept override {
            return Value_.load(std::memory_order_relaxed);
        }

        void Reset() noexcept override {
            Value_.store(0, std::memory_order_relaxed);
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnUint64(time, Get());
        }

    private:
        std::atomic_uint64_t Value_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // TLazyRate
    ///////////////////////////////////////////////////////////////////////////////
    class TLazyRate final: public ILazyRate {
    public:
        explicit TLazyRate(std::function<ui64()> supplier)
            : Supplier_(std::move(supplier))
        {
        }

        ui64 Get() const noexcept override {
            return Supplier_();
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnUint64(time, Get());
        }

    private:
        std::function<ui64()> Supplier_;
    };

    ///////////////////////////////////////////////////////////////////////////////
    // THistogram
    ///////////////////////////////////////////////////////////////////////////////
    class THistogram final: public IHistogram {
    public:
        THistogram(IHistogramCollectorPtr collector, bool isRate)
            : IHistogram(isRate)
            , Collector_(std::move(collector))
        {
        }

        THistogram(std::function<IHistogramCollectorPtr()> makeHistogramCollector, bool isRate)
            : IHistogram(isRate)
            , Collector_(makeHistogramCollector())
        {
        }

        void Record(double value) override {
            Collector_->Collect(value);
        }

        void Record(double value, ui32 count) override {
            Collector_->Collect(value, count);
        }

        void Accept(TInstant time, IMetricConsumer* consumer) const override {
            consumer->OnHistogram(time, TakeSnapshot());
        }

        IHistogramSnapshotPtr TakeSnapshot() const override {
            return Collector_->Snapshot();
        }

        void Reset() override {
            Collector_->Reset();
        }

    private:
        IHistogramCollectorPtr Collector_;
    };
}
