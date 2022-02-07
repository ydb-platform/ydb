#pragma once

#include <library/cpp/monlib/metrics/metric_value.h>


namespace NMonitoring {

    class TTypedPoint {
    public:
        TTypedPoint()
            : Time_(TInstant::Zero())
            , ValueType_(EMetricValueType::UNKNOWN)
        {
        }

        template <typename T>
        TTypedPoint(TInstant time, T value)
            : Time_(time)
            , ValueType_(TValueType<T>::Type)
            , Value_(value)
        {
            Ref();
        }

        ~TTypedPoint() {
            UnRef();
        }

        TTypedPoint(const TTypedPoint& rhs)
            : Time_(rhs.Time_)
            , ValueType_(rhs.ValueType_)
            , Value_(rhs.Value_)
        {
            Ref();
        }

        TTypedPoint& operator=(const TTypedPoint& rhs) {
            UnRef();

            Time_ = rhs.Time_;
            ValueType_ = rhs.ValueType_;
            Value_ = rhs.Value_;

            Ref();
            return *this;
        }

        TTypedPoint(TTypedPoint&& rhs) noexcept
            : Time_(rhs.Time_)
            , ValueType_(rhs.ValueType_)
            , Value_(rhs.Value_)
        {
            rhs.ValueType_ = EMetricValueType::UNKNOWN;
            rhs.Value_ = {};
        }

        TTypedPoint& operator=(TTypedPoint&& rhs) noexcept {
            UnRef();

            Time_ = rhs.Time_;
            ValueType_ = rhs.ValueType_;
            Value_ = rhs.Value_;

            rhs.ValueType_ = EMetricValueType::UNKNOWN;
            rhs.Value_ = {};

            return *this;
        }

        TInstant GetTime() const noexcept {
            return Time_;
        }

        void SetTime(TInstant time) noexcept {
            Time_ = time;
        }

        TMetricValue GetValue() const noexcept {
            return Value_;
        }

        EMetricValueType GetValueType() const noexcept {
            return ValueType_;
        }

        template <typename T>
        void SetValue(T value) noexcept {
            ValueType_ = TValueType<T>::Type;
            Value_ = TMetricValue{value};
        }

        bool HasValue() {
            return ValueType_ != EMetricValueType::UNKNOWN;
        }

    private:
        void Ref() {
            if (ValueType_ == EMetricValueType::HISTOGRAM) {
                Value_.AsHistogram()->Ref();
            } else if (ValueType_ == EMetricValueType::SUMMARY) {
                Value_.AsSummaryDouble()->Ref();
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                Value_.AsLogHistogram()->Ref();
            }
        }

        void UnRef() {
            if (ValueType_ == EMetricValueType::HISTOGRAM) {
                Value_.AsHistogram()->UnRef();
            } else if (ValueType_ == EMetricValueType::SUMMARY) {
                Value_.AsSummaryDouble()->UnRef();
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                Value_.AsLogHistogram()->UnRef();
            }
        }

    private:
        TInstant Time_;
        EMetricValueType ValueType_;
        TMetricValue Value_;
    };

}
