#include "hyperloglog.h"

#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <functional>

namespace {
    using TLookup = std::array<double, 256>;

    struct TCorrection {
        TLookup Estimations;
        TLookup Biases;

        double GetBias(double e) const {
            for (size_t idx = 0;; ++idx) {
                const auto estr = Estimations[idx];
                if (estr >= e) {
                    if (idx == 0) {
                        return Biases[0];
                    }
                    const auto estl = Estimations[idx - 1];
                    const auto biasl = Biases[idx - 1];
                    const auto biasr = Biases[idx];
                    const auto de = estr - estl;
                    const auto db = biasr - biasl;
                    const auto scale = e - estl;
                    return biasl + scale * db / de;
                } else if (std::fabs(estr) < 1e-4) {
                    //limiter
                    return Biases[idx - 1];
                }
            }
        }
    };

    double EstimateBias(double e, unsigned precision) {
        static const TCorrection CORRECTIONS[1 + THyperLogLog::PRECISION_MAX - THyperLogLog::PRECISION_MIN] = {
#include "hyperloglog_corrections.inc"
        };
        if (precision < THyperLogLog::PRECISION_MIN || precision > THyperLogLog::PRECISION_MAX) {
            return 0.;
        }

        return CORRECTIONS[precision - THyperLogLog::PRECISION_MIN].GetBias(e);
    }

    double GetThreshold(unsigned precision) {
        static const double THRESHOLD_DATA[1 + THyperLogLog::PRECISION_MAX - THyperLogLog::PRECISION_MIN] = {
            10,     // Precision  4
            20,     // Precision  5
            40,     // Precision  6
            80,     // Precision  7
            220,    // Precision  8
            400,    // Precision  9
            900,    // Precision 10
            1800,   // Precision 11
            3100,   // Precision 12
            6500,   // Precision 13
            11500,  // Precision 14
            20000,  // Precision 15
            50000,  // Precision 16
            120000, // Precision 17
            350000  // Precision 18
        };
        if (precision < THyperLogLog::PRECISION_MIN || precision > THyperLogLog::PRECISION_MAX) {
            return 0.;
        }

        return THRESHOLD_DATA[precision - THyperLogLog::PRECISION_MIN];
    }

    double EmpiricAlpha(size_t m) {
        switch (m) {
            case 16:
                return 0.673;
            case 32:
                return 0.697;
            case 64:
                return 0.709;
            default:
                return 0.7213 / (1.0 + 1.079 / m);
        }
    }

    double RawEstimate(const ui8* counts, size_t size) {
        double sum = {};
        for (size_t i = 0; i < size; ++i) {
            sum += std::pow(2.0, -counts[i]);
        }
        return EmpiricAlpha(size) * size * size / sum;
    }

    double LinearCounting(size_t registers, size_t zeroed) {
        return std::log(double(registers) / zeroed) * registers;
    }
}

THyperLogLogBase::THyperLogLogBase(unsigned precision)
    : Precision(precision) {
    Y_ENSURE(precision >= PRECISION_MIN && precision <= PRECISION_MAX);
}

void THyperLogLogBase::Update(ui64 hash) {
    const unsigned subHashBits = 8 * sizeof(hash) - Precision;
    const auto subHash = hash & MaskLowerBits(subHashBits);
    const auto leadingZeroes = subHash ? (subHashBits - GetValueBitCount(subHash)) : subHashBits;
    const ui8 weight = static_cast<ui8>(leadingZeroes + 1);

    const size_t reg = static_cast<size_t>(hash >> subHashBits);
    RegistersRef[reg] = std::max(RegistersRef[reg], weight);
}

void THyperLogLogBase::Merge(const THyperLogLogBase& rh) {
    Y_ENSURE(Precision == rh.Precision);

    std::transform(RegistersRef.begin(), RegistersRef.end(), rh.RegistersRef.begin(), RegistersRef.begin(), [](ui8 l, ui8 r) { return std::max(l, r); });
}

ui64 THyperLogLogBase::Estimate() const {
    const auto m = RegistersRef.size();
    const auto e = RawEstimate(RegistersRef.data(), m);

    const auto e_ = e <= 5 * m ? (e - EstimateBias(e, Precision)) : e;
    const auto v = std::count(RegistersRef.begin(), RegistersRef.end(), ui8(0));
    const auto h = v != 0 ? LinearCounting(m, v) : e_;
    return h <= GetThreshold(Precision) ? h : e_;
}

void THyperLogLogBase::Save(IOutputStream& out) const {
    out.Write(static_cast<char>(Precision));
    out.Write(RegistersRef.data(), RegistersRef.size() * sizeof(RegistersRef.front()));
}
