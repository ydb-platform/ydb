#pragma once

#include "public.h"
#include "hyperloglog_bias.h"
#include "farm_hash.h"

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <int Precision>
class THyperLogLog
{
    static_assert(
        Precision >= 4 && Precision <= 18,
        "Precision is out of range (expected to be within 4..18)");

public:
    THyperLogLog();

    void Add(TFingerprint fingerprint);

    void Merge(const THyperLogLog& that);

    ui64 EstimateCardinality() const;

    static ui64 EstimateCardinality(const std::vector<ui64>& values);

private:
    static constexpr ui64 RegisterCount = (ui64)1 << Precision;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
