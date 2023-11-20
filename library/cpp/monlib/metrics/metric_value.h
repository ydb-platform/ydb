#pragma once

#include "histogram_collector.h"
#include "metric_value_type.h"
#include "summary_collector.h"
#include "log_histogram_snapshot.h"

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/cast.h>
#include <util/generic/ymath.h>

namespace NMonitoring {
    namespace NPrivate {
        template <typename T>
        T FromFloatSafe(double d) {
            static_assert(std::is_integral<T>::value, "this function only converts floats to integers");
            Y_ENSURE(::IsValidFloat(d) && d >= Min<T>() && d <= MaxFloor<T>(), "Cannot convert " << d << " to an integer value");
            return static_cast<T>(d);
        }

        inline auto POINT_KEY_FN = [](auto& p) {
            return p.GetTime();
        };
    } // namespace NPrivate

    template <typename T, typename Enable = void>
    struct TValueType;

    template <>
    struct TValueType<double> {
        static constexpr auto Type = EMetricValueType::DOUBLE;
    };

    template <>
    struct TValueType<i64> {
        static constexpr auto Type = EMetricValueType::INT64;
    };

    template <>
    struct TValueType<ui64> {
        static constexpr auto Type = EMetricValueType::UINT64;
    };

    template <>
    struct TValueType<TLogHistogramSnapshot*> {
        static constexpr auto Type = EMetricValueType::LOGHISTOGRAM;
    };

    template <typename T>
    struct TValueType<T*, typename std::enable_if_t<std::is_base_of<IHistogramSnapshot, T>::value>> {
        static constexpr auto Type = EMetricValueType::HISTOGRAM;
    };

    template <typename T>
    struct TValueType<T*, typename std::enable_if_t<std::is_base_of<ISummaryDoubleSnapshot, T>::value>> {
        static constexpr auto Type = EMetricValueType::SUMMARY;
    };

    ///////////////////////////////////////////////////////////////////////////
    // TMetricValue
    ///////////////////////////////////////////////////////////////////////////
    // TMetricValue represents a generic value. It does not contain type
    // information about a value. This is done to minimize object footprint.
    // To read an actual value from the object the type must be checked
    // first or provided to AsXxxx(type) member-functions.
    // This class does not hold an ownership of an IHistogramSnapshot or
    // SummarySnapshot, so this must be done somewhere outside.
    class TMetricValue {
    public:
        TMetricValue() noexcept {
            Value_.Uint64 = 0;
        }

        explicit TMetricValue(double value) noexcept {
            Value_.Double = value;
        }

        explicit TMetricValue(i64 value) noexcept {
            Value_.Int64 = value;
        }

        explicit TMetricValue(ui64 value) noexcept {
            Value_.Uint64 = value;
        }

        explicit TMetricValue(IHistogramSnapshot* histogram) noexcept {
            Value_.Histogram = histogram;
        }

        explicit TMetricValue(ISummaryDoubleSnapshot* summary) noexcept {
            Value_.Summary = summary;
        }

        explicit TMetricValue(TLogHistogramSnapshot* logHist) noexcept {
            Value_.LogHistogram = logHist;
        }

        double AsDouble() const noexcept {
            return Value_.Double;
        }

        // will cast value into double, current value type is determined by
        // the given type argument
        double AsDouble(EMetricValueType type) const {
            switch (type) {
                case EMetricValueType::DOUBLE:
                    return Value_.Double;
                case EMetricValueType::INT64:
                    return static_cast<double>(Value_.Int64);
                case EMetricValueType::UINT64:
                    return static_cast<double>(Value_.Uint64);
                case EMetricValueType::HISTOGRAM:
                    ythrow yexception() << "histogram cannot be casted to Double";
                case EMetricValueType::SUMMARY:
                    ythrow yexception() << "summary cannot be casted to Double";
                case EMetricValueType::LOGHISTOGRAM:
                    ythrow yexception() << "loghistogram cannot be casted to Double";
                case EMetricValueType::UNKNOWN:
                    ythrow yexception() << "unknown value type";
            }
            Y_ABORT(); // for GCC
        }

        ui64 AsUint64() const noexcept {
            return Value_.Uint64;
        }

        // will cast value into uint64, current value's type is determined by
        // the given type argument
        ui64 AsUint64(EMetricValueType type) const {
            switch (type) {
                case EMetricValueType::DOUBLE:
                    return NPrivate::FromFloatSafe<ui64>(Value_.Double);
                case EMetricValueType::INT64:
                    return SafeIntegerCast<ui64>(Value_.Int64);
                case EMetricValueType::UINT64:
                    return Value_.Uint64;
                case EMetricValueType::HISTOGRAM:
                    ythrow yexception() << "histogram cannot be casted to Uint64";
                case EMetricValueType::SUMMARY:
                    ythrow yexception() << "summary cannot be casted to Uint64";
                case EMetricValueType::LOGHISTOGRAM:
                    ythrow yexception() << "loghistogram cannot be casted to Uint64";
                case EMetricValueType::UNKNOWN:
                    ythrow yexception() << "unknown value type";
            }
            Y_ABORT(); // for GCC
        }

        i64 AsInt64() const noexcept {
            return Value_.Int64;
        }

        // will cast value into int64, current value's type is determined by
        // the given type argument
        i64 AsInt64(EMetricValueType type) const {
            switch (type) {
                case EMetricValueType::DOUBLE:
                    return NPrivate::FromFloatSafe<i64>(Value_.Double);
                case EMetricValueType::INT64:
                    return Value_.Int64;
                case EMetricValueType::UINT64:
                    return SafeIntegerCast<i64>(Value_.Uint64);
                case EMetricValueType::HISTOGRAM:
                    ythrow yexception() << "histogram cannot be casted to Int64";
                case EMetricValueType::SUMMARY:
                    ythrow yexception() << "summary cannot be casted to Int64";
                case EMetricValueType::LOGHISTOGRAM:
                    ythrow yexception() << "loghistogram cannot be casted to Int64";
                case EMetricValueType::UNKNOWN:
                    ythrow yexception() << "unknown value type";
            }
            Y_ABORT(); // for GCC
        }

        IHistogramSnapshot* AsHistogram() const noexcept {
            return Value_.Histogram;
        }

        IHistogramSnapshot* AsHistogram(EMetricValueType type) const {
            if (type != EMetricValueType::HISTOGRAM) {
                ythrow yexception() << type << " cannot be casted to Histogram";
            }

            return Value_.Histogram;
        }

        ISummaryDoubleSnapshot* AsSummaryDouble() const noexcept {
            return Value_.Summary;
        }

        ISummaryDoubleSnapshot* AsSummaryDouble(EMetricValueType type) const {
            if (type != EMetricValueType::SUMMARY) {
                ythrow yexception() << type << " cannot be casted to SummaryDouble";
            }

            return Value_.Summary;
        }

        TLogHistogramSnapshot* AsLogHistogram() const noexcept {
            return Value_.LogHistogram;
        }

        TLogHistogramSnapshot* AsLogHistogram(EMetricValueType type) const {
            if (type != EMetricValueType::LOGHISTOGRAM) {
                ythrow yexception() << type << " cannot be casted to LogHistogram";
            }

            return Value_.LogHistogram;
        }

    protected:
        union {
            double Double;
            i64 Int64;
            ui64 Uint64;
            IHistogramSnapshot* Histogram;
            ISummaryDoubleSnapshot* Summary;
            TLogHistogramSnapshot* LogHistogram;
        } Value_;
    };

    ///////////////////////////////////////////////////////////////////////////
    // TMetricValueWithType
    ///////////////////////////////////////////////////////////////////////////
    // Same as TMetricValue, but this type holds an ownership of
    // snapshots and contains value type information.
    class TMetricValueWithType: private TMetricValue, public TMoveOnly {
    public:
        using TBase = TMetricValue;

        template <typename T>
        explicit TMetricValueWithType(T value)
            : TBase(value)
            , ValueType_{TValueType<T>::Type}
        {
            Ref();
        }

        TMetricValueWithType(TMetricValueWithType&& other)
            : TBase(std::move(other))
            , ValueType_{other.ValueType_}
        {
            Ref();
            other.Clear();
        }

        TMetricValueWithType& operator=(TMetricValueWithType&& other) {
            TBase::operator=(other);
            ValueType_ = other.ValueType_;

            Ref();
            other.Clear();

            return *this;
        }

        ~TMetricValueWithType() {
            UnRef();
        }

        void Clear() {
            UnRef();
            ValueType_ = EMetricValueType::UNKNOWN;
        }

        EMetricValueType GetType() const noexcept {
            return ValueType_;
        }

        double AsDouble() const {
            return TBase::AsDouble(ValueType_);
        }

        ui64 AsUint64() const {
            return TBase::AsUint64(ValueType_);
        }

        i64 AsInt64() const {
            return TBase::AsInt64(ValueType_);
        }

        IHistogramSnapshot* AsHistogram() const {
            return TBase::AsHistogram(ValueType_);
        }

        ISummaryDoubleSnapshot* AsSummaryDouble() const {
            return TBase::AsSummaryDouble(ValueType_);
        }

        TLogHistogramSnapshot* AsLogHistogram() const {
            return TBase::AsLogHistogram(ValueType_);
        }

    private:
        void Ref() {
            if (ValueType_ == EMetricValueType::SUMMARY) {
                TBase::AsSummaryDouble()->Ref();
            } else if (ValueType_ == EMetricValueType::HISTOGRAM) {
                TBase::AsHistogram()->Ref();
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                TBase::AsLogHistogram()->Ref();
            }
        }

        void UnRef() {
            if (ValueType_ == EMetricValueType::SUMMARY) {
                TBase::AsSummaryDouble()->UnRef();
            } else if (ValueType_ == EMetricValueType::HISTOGRAM) {
                TBase::AsHistogram()->UnRef();
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                TBase::AsLogHistogram()->UnRef();
            }
        }

    private:
        EMetricValueType ValueType_ = EMetricValueType::UNKNOWN;
    };

    static_assert(sizeof(TMetricValue) == sizeof(ui64),
                  "expected size of TMetricValue is one machine word");

    ///////////////////////////////////////////////////////////////////////////
    // TMetricTimeSeries
    ///////////////////////////////////////////////////////////////////////////
    class TMetricTimeSeries: private TMoveOnly {
    public:
        class TPoint {
        public:
            TPoint()
                : Time_(TInstant::Zero())
            {
            }

            template <typename T>
            TPoint(TInstant time, T value)
                : Time_(time)
                , Value_(value)
            {
            }

            TInstant GetTime() const noexcept {
                return Time_;
            }

            TMetricValue GetValue() const noexcept {
                return Value_;
            }

            void ClearValue() {
                Value_ = {};
            }

        private:
            TInstant Time_;
            TMetricValue Value_;
        };

    public:
        TMetricTimeSeries() = default;

        TMetricTimeSeries(TMetricTimeSeries&& rhs) noexcept
            : ValueType_(rhs.ValueType_)
            , Points_(std::move(rhs.Points_))
        {
            rhs.ValueType_ = EMetricValueType::UNKNOWN;
        }

        TMetricTimeSeries& operator=(TMetricTimeSeries&& rhs) noexcept {
            Clear();

            ValueType_ = rhs.ValueType_;
            rhs.ValueType_ = EMetricValueType::UNKNOWN;

            Points_ = std::move(rhs.Points_);
            return *this;
        }

        ~TMetricTimeSeries() {
            Clear();
        }

        template <typename T>
        void Add(TInstant time, T value) {
            Add(TPoint(time, value), TValueType<T>::Type);
        }

        void Add(TPoint point, EMetricValueType valueType) {
            if (Empty()) {
                ValueType_ = valueType;
            } else {
                CheckTypes(ValueType_, valueType);
            }
            Points_.push_back(point);

            if (ValueType_ == EMetricValueType::SUMMARY) {
                TPoint& p = Points_.back();
                p.GetValue().AsSummaryDouble()->Ref();
            } else if (ValueType_ == EMetricValueType::HISTOGRAM) {
                TPoint& p = Points_.back();
                p.GetValue().AsHistogram()->Ref();
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                TPoint& p = Points_.back();
                p.GetValue().AsLogHistogram()->Ref();
            }
        }

        void CopyFrom(const TMetricTimeSeries& other) {
            if (Empty()) {
                ValueType_ = other.ValueType_;
            } else {
                CheckTypes(GetValueType(), other.GetValueType());
            }

            size_t prevSize = Points_.size();
            Copy(std::begin(other.Points_), std::end(other.Points_),
                 std::back_inserter(Points_));

            if (ValueType_ == EMetricValueType::HISTOGRAM) {
                for (size_t i = prevSize; i < Points_.size(); i++) {
                    TPoint& point = Points_[i];
                    point.GetValue().AsHistogram()->Ref();
                }
            } else if (ValueType_ == EMetricValueType::SUMMARY) {
                for (size_t i = prevSize; i < Points_.size(); ++i) {
                    TPoint& point = Points_[i];
                    point.GetValue().AsSummaryDouble()->Ref();
                }
            } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
                for (size_t i = prevSize; i < Points_.size(); ++i) {
                    TPoint& point = Points_[i];
                    point.GetValue().AsLogHistogram()->Ref();
                }
            }
        }

        template <typename TConsumer>
        void ForEach(TConsumer c) const {
            for (const auto& point : Points_) {
                c(point.GetTime(), ValueType_, point.GetValue());
            }
        }

        bool Empty() const noexcept {
            return Points_.empty();
        }

        size_t Size() const noexcept {
            return Points_.size();
        }

        size_t Capacity() const noexcept {
            return Points_.capacity();
        }

        const TPoint& operator[](size_t index) const noexcept {
            return Points_[index];
        }

        void SortByTs();

        void Clear() noexcept;

        EMetricValueType GetValueType() const noexcept {
            return ValueType_;
        }

    private:
        static void CheckTypes(EMetricValueType t1, EMetricValueType t2) {
            Y_ENSURE(t1 == t2,
                     "Series type mismatch: expected " << t1 <<
                     ", but got " << t2);
        }

    private:
        EMetricValueType ValueType_ = EMetricValueType::UNKNOWN;
        TVector<TPoint> Points_;
    };

    template <EMetricValueType valueType, typename TPoint>
    static inline void SnapshotUnRef(TPoint& point) {
        if constexpr (valueType == EMetricValueType::HISTOGRAM) {
            if (auto* hist = point.GetValue().AsHistogram()) {
                hist->UnRef();
            }
        } else if constexpr (valueType == EMetricValueType::SUMMARY) {
            if (auto* summary = point.GetValue().AsSummaryDouble()) {
                summary->UnRef();
            }
        } else if constexpr (valueType == EMetricValueType::LOGHISTOGRAM) {
            if (auto* logHist = point.GetValue().AsLogHistogram()) {
                logHist->UnRef();
            }
        }
    }

    template <EMetricValueType valueType, typename TPoint>
    static void EraseDuplicates(TVector<TPoint>& points) {
        // we have to manually clean reference to a snapshot from point
        // while removing duplicates
        auto result = points.rbegin();
        for (auto it = result + 1; it != points.rend(); ++it) {
            if (result->GetTime() != it->GetTime() && ++result != it) {
                SnapshotUnRef<valueType>(*result);
                *result = *it;    // (2) copy
                it->ClearValue(); // (3) clean pointer in the source
            }
        }

        // erase tail points
        for (auto it = result + 1; it != points.rend(); ++it) {
            SnapshotUnRef<valueType>(*it);
        }
        points.erase(points.begin(), (result + 1).base());
    }

    template <typename TPoint>
    void SortPointsByTs(EMetricValueType valueType, TVector<TPoint>& points) {
        if (points.size() < 2) {
            return;
        }

        if (valueType != EMetricValueType::HISTOGRAM && valueType != EMetricValueType::SUMMARY
            && valueType != EMetricValueType::LOGHISTOGRAM) {
            // Stable sort + saving only the last point inside a group of duplicates
            StableSortBy(points, NPrivate::POINT_KEY_FN);
            auto it = UniqueBy(points.rbegin(), points.rend(), NPrivate::POINT_KEY_FN);
            points.erase(points.begin(), it.base());
        } else {
            StableSortBy(points, NPrivate::POINT_KEY_FN);
            if (valueType == EMetricValueType::HISTOGRAM) {
                EraseDuplicates<EMetricValueType::HISTOGRAM>(points);
            } else if (valueType == EMetricValueType::LOGHISTOGRAM) {
                EraseDuplicates<EMetricValueType::LOGHISTOGRAM>(points);
            } else {
                EraseDuplicates<EMetricValueType::SUMMARY>(points);
            }
        }
    }
}
