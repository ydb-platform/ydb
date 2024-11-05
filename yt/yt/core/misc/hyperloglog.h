#pragma once

#include "public.h"
#include "hyperloglog_bias.h"
#include "farm_hash.h"

#include <library/cpp/yt/memory/range.h>
#include <yt/yt_proto/yt/core/misc/proto/hyperloglog.pb.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <int Precision>
class THyperLogLog;

template <int Precision>
void FormatValue(TStringBuilderBase* builder, const THyperLogLog<Precision>& value, TStringBuf format);

template <int Precision>
void ToProto(
    NProto::THyperLogLog* protoHyperLogLog,
    const THyperLogLog<Precision>& hyperloglog);

template <int Precision>
void FromProto(
    THyperLogLog<Precision>* hyperloglog,
    const NProto::THyperLogLog& protoHyperLogLog);

////////////////////////////////////////////////////////////////////////////////

template <int Precision>
class THyperLogLog
{
    static_assert(
        Precision >= 4 && Precision <= 18,
        "Precision is out of range (expected to be within 4..18)");

public:
    static constexpr ui64 RegisterCount = (ui64)1 << Precision;

    THyperLogLog();
    explicit THyperLogLog(TRange<char> data);

    void Add(TFingerprint fingerprint);

    void Merge(const THyperLogLog& that);

    ui64 EstimateCardinality() const;

    TRange<char> Data() const;

    static ui64 EstimateCardinality(const std::vector<ui64>& values);

    bool operator==(const THyperLogLog<Precision>& other) const = default;

private:
    friend void ToProto<Precision>(
        NProto::THyperLogLog* protoHyperLogLog,
        const THyperLogLog<Precision>& hyperloglog);

    friend void FromProto<Precision>(
        THyperLogLog<Precision>* hyperloglog,
        const NProto::THyperLogLog& protoHyperLogLog);

    friend void FormatValue<Precision>(TStringBuilderBase* builder, const THyperLogLog<Precision>& value, TStringBuf format);

    static constexpr ui64 PrecisionMask = RegisterCount - 1;
    static constexpr double Threshold = NDetail::Thresholds[Precision - 4];
    static constexpr int Size = NDetail::Sizes[Precision - 4];
    static constexpr auto RawEstimates = NDetail::RawEstimates[Precision - 4];
    static constexpr auto BiasData = NDetail::BiasData[Precision - 4];
    std::array<ui8, RegisterCount> ZeroCounts_;

    void AddHash(ui64 hash);
    double EstimateBias(double cardinality) const;
};

template <int Precision>
THyperLogLog<Precision>::THyperLogLog()
{
    std::fill(ZeroCounts_.begin(), ZeroCounts_.end(), 0);
}

template <int Precision>
THyperLogLog<Precision>::THyperLogLog(TRange<char> data)
{
    YT_VERIFY(data.size() == RegisterCount);
    std::copy(data.begin(), data.end(), ZeroCounts_.begin());
}

template <int Precision>
TRange<char> THyperLogLog<Precision>::Data() const
{
    return TRange<char>(reinterpret_cast<const char*>(ZeroCounts_.begin()), ZeroCounts_.size());
}

template <int Precision>
void THyperLogLog<Precision>::Add(TFingerprint fingerprint)
{
    fingerprint |= ((ui64)1 << 63);
    auto zeroesPlusOne = __builtin_ctzl(fingerprint >> Precision) + 1;

    auto index = fingerprint & PrecisionMask;
    if (ZeroCounts_[index] < zeroesPlusOne) {
        ZeroCounts_[index] = zeroesPlusOne;
    }
}

template <int Precision>
void THyperLogLog<Precision>::Merge(const THyperLogLog<Precision>& that)
{
    for (size_t i = 0; i < RegisterCount; i++) {
        auto thatCount = that.ZeroCounts_[i];
        if (ZeroCounts_[i] < thatCount) {
            ZeroCounts_[i] = thatCount;
        }
    }
}

template <int Precision>
double THyperLogLog<Precision>::EstimateBias(double cardinality) const
{
    auto upperEstimate = std::lower_bound(RawEstimates, RawEstimates + Size, cardinality);
    int index = upperEstimate - RawEstimates;
    if (*upperEstimate == cardinality) {
        return BiasData[index];
    }

    if (index == 0) {
        return BiasData[0];
    } else if (index >= Size) {
        return BiasData[Size];
    } else {
        double w1 = cardinality - RawEstimates[index - 1];
        double w2 = RawEstimates[index] - cardinality;
        return (BiasData[index - 1] * w1 + BiasData[index] * w2) / (w1 + w2);
    }
}

template <int Precision>
ui64 THyperLogLog<Precision>::EstimateCardinality() const
{
    auto zeroRegisters = 0;
    double sum = 0;
    for (auto count : ZeroCounts_) {
        if (count == 0) {
            zeroRegisters++;
        } else {
            sum += 1.0 / ((ui64)1 << count);
        }
    }
    sum += zeroRegisters;

    double alpha = 0.7213 / (1.0 + 1.079 / RegisterCount);
    double m = RegisterCount;
    double raw = (1.0 / sum) * m * m * alpha;

    if (raw < 5 * m) {
        raw -= EstimateBias(raw);
    }

    double smallCardinality = raw;
    if (zeroRegisters != 0) {
        smallCardinality = m * log(m / zeroRegisters);
    }

    if (smallCardinality <= Threshold) {
        return smallCardinality;
    } else {
        return raw;
    }
}

template <int Precision>
ui64 THyperLogLog<Precision>::EstimateCardinality(const std::vector<ui64>& values)
{
    auto state = THyperLogLog<Precision>();
    for (auto v : values) {
        state.Add(v);
    }
    return state.EstimateCardinality();
}

template <int Precision>
void ToProto(
    NProto::THyperLogLog* protoHyperLogLog,
    const THyperLogLog<Precision>& hyperloglog)
{
    protoHyperLogLog->set_data(hyperloglog.Data().begin(), hyperloglog.Data().size());
}

template <int Precision>
void FromProto(
    THyperLogLog<Precision>* hyperloglog,
    const NProto::THyperLogLog& protoHyperLogLog)
{
    YT_VERIFY(protoHyperLogLog.data().size() == std::size(hyperloglog->ZeroCounts_));
    std::copy(protoHyperLogLog.data().begin(), protoHyperLogLog.data().end(), hyperloglog->ZeroCounts_.begin());
}

////////////////////////////////////////////////////////////////////////////////

template <int Precision>
void FormatValue(TStringBuilderBase* builder, const THyperLogLog<Precision>& value, TStringBuf /*format*/)
{
    builder->AppendFormat("%v", std::span<const char>(
        reinterpret_cast<const char*>(value.ZeroCounts_.begin()), value.ZeroCounts_.size()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
