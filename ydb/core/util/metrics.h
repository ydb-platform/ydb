#pragma once
#include <array>
#include <util/datetime/base.h>
#include <util/stream/buffer.h>
#include <cmath>
#include <ydb/core/protos/metrics.pb.h>

namespace std {

template <>
struct make_signed<double> {
    using type = double;
};

}

namespace NKikimr {
namespace NMetrics {

// gauge resource type, supports absolute and delta population
template <typename ValueType>
class TGaugeValue {
public:
    using TType = ValueType;

    TGaugeValue(ValueType value = ValueType())
        : Value(value)
    {}

    std::make_signed_t<ValueType> Set(ValueType value) {
        std::make_signed_t<ValueType> diff =
                static_cast<std::make_signed_t<ValueType>>(value) -
                static_cast<std::make_signed_t<ValueType>>(Value);
        Value = value;
        return diff;
    }

    void Increment(std::make_signed_t<ValueType> value) { Value += value; }
    ValueType GetValue() const { return Value; }
    TGaugeValue operator +(const TGaugeValue& o) const { return TGaugeValue(Value + o.GetValue()); }
    TGaugeValue operator -(const TGaugeValue& o) const { return TGaugeValue(Value - o.GetValue()); }
    constexpr bool IsValueReady() const { return Value != ValueType(); }
    constexpr bool IsValueObsolete(TInstant now = TInstant::Now()) const { Y_UNUSED(now); return false; }

protected:
    ValueType Value;
};

constexpr TTimeBase<TInstant>::TValue DurationPerSecond = 1000000ull;
constexpr TTimeBase<TInstant>::TValue DurationPerMinute = DurationPerSecond * 60;
constexpr TTimeBase<TInstant>::TValue DurationPerHour = DurationPerMinute * 60;
constexpr TTimeBase<TInstant>::TValue DurationPerDay = DurationPerHour * 24;

// simple RRD time series type
template <typename ValueType, TTimeBase<TInstant>::TValue DurationToStore = DurationPerDay, int BucketCount = 24>
class TTimeSeriesValue {
public:
    using TType = ValueType;
    static constexpr int BUCKET_COUNT = BucketCount;
    static constexpr TTimeBase<TInstant>::TValue BUCKET_DURATION = DurationToStore / BucketCount;

    TTimeSeriesValue()
        : Buckets()
        , FirstUpdate()
        , LastUpdate()
    {}

    static int TimeToBucket(TInstant time) {
        return (time.GetValue() % DurationToStore) / BUCKET_DURATION;
    }

    static TTimeBase<TInstant>::TValue GetBucketSizeFrom(TInstant time) {
        return BUCKET_DURATION - (time.GetValue() % BUCKET_DURATION);
    }

    static TTimeBase<TInstant>::TValue GetBucketSizeTo(TInstant time) {
        return time.GetValue() % BUCKET_DURATION;
    }

    void Store(ValueType value, TInstant to) {
        int bucketTo = TimeToBucket(to);
        TInstant from;
        if (LastUpdate == TInstant()) {
            LastUpdate = FirstUpdate = to;
            return;
        } else {
            from = TInstant::MicroSeconds(std::max(LastUpdate.GetValue(), to.GetValue() - (DurationToStore - BUCKET_DURATION)));
        }
        int bucketFrom = TimeToBucket(from);
        if (bucketFrom == bucketTo) {
            Buckets[bucketTo] += value;
        } else {
            if (++bucketFrom == BucketCount)
                bucketFrom = 0;
            while (bucketFrom != bucketTo) {
                Buckets[bucketFrom] = 0;
                if (++bucketFrom == BucketCount)
                    bucketFrom = 0;
            }
            Buckets[bucketTo] = value;
        }
        LastUpdate = to;
        if (FirstUpdate == TInstant()) {
            FirstUpdate = to;
        } else {
            FirstUpdate = TInstant::MicroSeconds(std::max(FirstUpdate.GetValue(), to.GetValue() - GetBucketSizeTo(to) - (DurationToStore - BUCKET_DURATION)));
        }
    }

    TDuration GetStoredDuration() const {
        return TDuration::MicroSeconds(LastUpdate.GetValue() - FirstUpdate.GetValue());
    }

    ValueType GetValueAveragePerDuration(TDuration duration) const {
        if (LastUpdate == TInstant() || LastUpdate == FirstUpdate)
            return ValueType();
        ValueType accumulator = ValueType();
        int bucketTo = TimeToBucket(LastUpdate);
        int bucketFrom = TimeToBucket(FirstUpdate);
        for (int bucket = bucketFrom;; ++bucket, bucket = bucket >= BucketCount ? 0 : bucket) {
            accumulator += Buckets[bucket];
            if (bucket == bucketTo)
                break;
        }
        ValueType multiplicator(duration.GetValue());
        ValueType divider(GetStoredDuration().GetValue());
        return accumulator * multiplicator / divider;
    }

    void Increment(std::make_signed_t<ValueType> value, TInstant to = TInstant::Now()) {
        Store(value, to);
    }

    ValueType GetValue() const {
        return GetValueAveragePerDuration(TDuration::Seconds(1));
    }

    TTimeSeriesValue operator -(const TTimeSeriesValue& o) const {
        TTimeSeriesValue temp(*this);
        temp -= o;
        return temp;
    }

    TTimeSeriesValue operator +(const TTimeSeriesValue& o) const {
        TTimeSeriesValue temp(*this);
        temp += o;
        return temp;
    }

    TTimeSeriesValue& operator -=(const TTimeSeriesValue& o) {
        for (std::size_t i = 0; i < BucketCount; ++i)
            Buckets[i] -= o.Buckets[i];
        return *this;
    }

    TTimeSeriesValue& operator +=(const TTimeSeriesValue& o) {
        for (std::size_t i = 0; i < BucketCount; ++i)
            Buckets[i] += o.Buckets[i];
        return *this;
    }

    bool IsValueReady() const {
        return (LastUpdate - FirstUpdate).GetValue() >= DurationToStore / BucketCount;
    }

protected:
    std::array<ValueType, BucketCount> Buckets;
    TInstant FirstUpdate;
    TInstant LastUpdate;
};

template <typename ValueType>
struct TAverageConverter {
    static ValueType GetAverage(TTimeBase<TInstant>::TValue duration,
                                TTimeBase<TInstant>::TValue accumulatorTime,
                                ValueType accumulatorValue) {
        // we ether lose precision or having overflow...
        //return AccumulatorValue * duration.MilliSeconds() / AccumulatorTime;
        //return AccumulatorValue / AccumulatorTime * duration.GetValue();
        return round(double(duration) / accumulatorTime * accumulatorValue);
    }
};

template <>
struct TAverageConverter<double> {
    static double GetAverage(TTimeBase<TInstant>::TValue duration,
                             TTimeBase<TInstant>::TValue accumulatorTime,
                             double accumulatorValue) {
        return double(duration) / accumulatorTime * accumulatorValue;
    }
};

template <typename ValueType,
          TTimeBase<TInstant>::TValue DurationToStore = DurationPerMinute,
          TTimeBase<TInstant>::TValue DurationToCalculate = DurationToStore>
class TDecayingAverageValue {
    static constexpr TTimeBase<TInstant>::TValue GetTimeValue(TDuration duration) {
        return duration.MicroSeconds();
    }

    static constexpr TDuration GetTimeValue(TTimeBase<TInstant>::TValue duration) {
        return TDuration::MicroSeconds(duration);
    }

public:
    using TType = ValueType;

    TDecayingAverageValue()
        : AverageValue()
        , AccumulatorValue()
        , LastUpdate()
    {}

    void Set(ValueType value, TInstant now = TInstant::Now()) {
        AverageValue = value;
        AccumulatorValue = ValueType();
        LastUpdate = now;
    }

    ValueType GetValueAveragePerDuration(TTimeBase<TInstant>::TValue periodToCalculate, TTimeBase<TInstant>::TValue periodToMeasure) const {
        if (AccumulatorValue == ValueType()) {
            return ValueType();
        }
        return TAverageConverter<ValueType>::GetAverage(periodToCalculate, periodToMeasure, AccumulatorValue);
    }

    static ValueType GetSumOfAverages(ValueType oldAverage, ValueType newAverage) {
        return oldAverage == ValueType() ? newAverage : (oldAverage * 1 + newAverage * 3) / 4;
    }

    void Increment(std::make_signed_t<ValueType> value, TInstant now = TInstant::Now()) {
        if (LastUpdate == TInstant()) {
            LastUpdate = now;
            return;
        }
        AccumulatorValue += value;
        auto time(GetTimeValue(now - LastUpdate));
        if (time >= DurationToStore) {
            AverageValue = GetSumOfAverages(AverageValue, GetValueAveragePerDuration(DurationToCalculate, time));
            AccumulatorValue = ValueType();
            LastUpdate = now;
        }
    }

    ValueType GetValue() const {
        return AverageValue;
    }

    bool IsValueReady() const {
        return AverageValue != ValueType();
    }

    bool IsValueObsolete(TInstant now = TInstant::Now()) const {
        return (LastUpdate + GetTimeValue(DurationToStore) * 2) < now;
    }

protected:
    ValueType AverageValue;
    ValueType AccumulatorValue;
    TInstant LastUpdate;
};

template <typename ValueType, size_t MaxCount = 20>
class TAverageValue {
public:
    using TType = ValueType;

    TAverageValue()
        : AccumulatorValue()
        , AccumulatorCount()
    {}

    void Push(ValueType value) {
        if (AccumulatorCount >= MaxCount) {
            AccumulatorValue = AccumulatorValue / 2;
            AccumulatorCount /= 2;
        }
        AccumulatorValue += value;
        AccumulatorCount++;
    }

    ValueType GetValue() const {
        if (AccumulatorCount == 0) {
            return ValueType();
        }
        return AccumulatorValue / AccumulatorCount;
    }

    bool IsValueReady() const {
        return AccumulatorCount > 0;
    }

    bool IsValueStable() const {
        return AccumulatorCount >= MaxCount / 2;
    }

protected:
    ValueType AccumulatorValue;
    size_t AccumulatorCount;
};

template <typename ValueType, size_t MaxCount = 20>
class TFastRiseAverageValue {
public:
    using TType = ValueType;

    TFastRiseAverageValue()
        : AccumulatorValue()
        , AccumulatorCount()
    {}

    void Push(ValueType value) {
        if (IsValueReady()) {
            ValueType currentValue = GetValue();
            if (value > currentValue) {
                ValueType newValue = (currentValue + value) / 2;
                AccumulatorCount++;
                AccumulatorValue = newValue * AccumulatorCount;
                return;
            }
        }
        if (AccumulatorCount >= MaxCount) {
            AccumulatorValue = AccumulatorValue / 2;
            AccumulatorCount /= 2;
        }
        AccumulatorValue += value;
        AccumulatorCount++;
    }

    ValueType GetValue() const {
        if (AccumulatorCount == 0) {
            return ValueType();
        }
        return AccumulatorValue / AccumulatorCount;
    }

    bool IsValueReady() const {
        return AccumulatorCount > 0;
    }

    bool IsValueStable() const {
        return AccumulatorCount >= 2;
    }

protected:
    ValueType AccumulatorValue;
    size_t AccumulatorCount;
};

template <TTimeBase<TInstant>::TValue DurationToStore = DurationPerDay, size_t BucketCount = 24>
class TMaximumValueUI64 : public NKikimrMetricsProto::TMaximumValueUI64 {
public:
    using TType = ui64;
    using TProto = NKikimrMetricsProto::TMaximumValueUI64;
    static constexpr TDuration BucketDuration = TDuration::MilliSeconds(DurationToStore / BucketCount);

    void SetValue(TType value, TInstant now = TInstant::Now()) {
        if (TProto::ValuesSize() == 0 || now - TInstant::MilliSeconds(TProto::GetLastBucketStartTime()) > BucketDuration) {
            // new bucket
            if (TProto::ValuesSize() == BucketCount) {
                std::rotate(TProto::MutableValues()->begin(), std::next(TProto::MutableValues()->begin()), TProto::MutableValues()->end());
                auto& lastBucketValue(*std::prev(TProto::MutableValues()->end()));
                lastBucketValue = value;
                MaximumValue = *std::max_element(TProto::GetValues().begin(), TProto::GetValues().end());
            } else {
                TProto::MutableValues()->Add(value);
                MaximumValue = std::max(MaximumValue, value);
            }
            TProto::SetLastBucketStartTime(now.MilliSeconds());
        } else {
            auto& lastBucketValue(*std::prev(TProto::MutableValues()->end()));
            lastBucketValue = std::max(lastBucketValue, value);
            MaximumValue = std::max(MaximumValue, value);
        }
    }

    TType GetValue() const {
        return MaximumValue;
    }

    void InitiaizeFrom(const TProto& proto) {
        TProto::CopyFrom(proto);
        if (TProto::ValuesSize() > 0) {
            MaximumValue = *std::max_element(TProto::GetValues().begin(), TProto::GetValues().end());
        }
    }

protected:
    TType MaximumValue = {};
};

class TMaximumValueVariableWindowUI64 : public NKikimrMetricsProto::TMaximumValueUI64 {
public:
    using TType = ui64;
    using TProto = NKikimrMetricsProto::TMaximumValueUI64;

    void SetValue(TType value, TInstant now = TInstant::Now()) {
        if (TProto::GetAllTimeMaximum() > 0 || MaximumValue > 0) { // ignoring initial value
            TProto::SetAllTimeMaximum(std::max(value, TProto::GetAllTimeMaximum()));
        }
        TDuration elapsedCurrentBucket = now - TInstant::MilliSeconds(TProto::GetLastBucketStartTime());
        if (TProto::ValuesSize() == 0 || elapsedCurrentBucket >= BucketDuration) {
            size_t bucketsPassed = 0;
            // new bucket
            if (TProto::ValuesSize() == 0) {
                TProto::SetLastBucketStartTime(now.MilliSeconds());
                bucketsPassed = 1;
            } else {
                bucketsPassed = elapsedCurrentBucket / BucketDuration;
                TProto::SetLastBucketStartTime(TProto::GetLastBucketStartTime() + bucketsPassed * BucketDuration.MilliSeconds());
            }

            if (bucketsPassed >= BucketCount) {
                TProto::MutableValues()->Clear();
                TProto::MutableValues()->Add(value);
                MaximumValue = value;
            } else if (TProto::ValuesSize() + bucketsPassed > BucketCount) {
                auto shift = TProto::ValuesSize() + bucketsPassed - BucketCount;
                for (size_t pass = TProto::ValuesSize(); pass < BucketCount; ++pass) {
                    TProto::MutableValues()->Add(value);
                }
                auto newBegin = TProto::MutableValues()->begin();
                std::advance(newBegin, shift);
                std::rotate(TProto::MutableValues()->begin(), newBegin, TProto::MutableValues()->end());
                auto last = TProto::MutableValues()->end();
                std::advance(last, -shift);
                std::fill(last, TProto::MutableValues()->end(), value);
                MaximumValue = *std::max_element(TProto::GetValues().begin(), TProto::GetValues().end());
            } else {
                for (size_t pass = 0; pass < bucketsPassed; ++pass) {
                    TProto::MutableValues()->Add(value);
                }
                MaximumValue = std::max(MaximumValue, value);
            }
        } else {
            auto& lastBucketValue(*std::prev(TProto::MutableValues()->end()));
            lastBucketValue = std::max(lastBucketValue, value);
            MaximumValue = std::max(MaximumValue, value);
        }
    }

    void AdvanceTime(TInstant now) {
        // Nothing changed, last value is stiil relevant
        TType lastValue = {};
        if (!TProto::GetValues().empty()) {
            lastValue = *std::prev(TProto::MutableValues()->end());
        }
        SetValue(lastValue, now);
    }

    TType GetValue() const {
        return MaximumValue;
    }

    void InitiaizeFrom(const TProto& proto) {
        TProto::CopyFrom(proto);
        if (TProto::ValuesSize() > 0) {
            MaximumValue = *std::max_element(TProto::GetValues().begin(), TProto::GetValues().end());
        }
    }

    void SetWindowSize(TDuration durationToStore, size_t bucketCount = 24) {
        BucketCount = bucketCount;
        BucketDuration = TDuration::MilliSeconds(durationToStore.MilliSeconds() / BucketCount);
    }

protected:
    TType MaximumValue = {};
    size_t BucketCount = 24;
    TDuration BucketDuration = TDuration::Hours(1);
};

} // NMetrics
} // NKikimr
