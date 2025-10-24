#pragma once

#include <util/generic/utility.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <chrono>
#include <cassert>
#include <algorithm>
#include <cmath>
#include <iomanip>


namespace NKikimr::NKqp {


template <typename Lambda>
ui64 MeasureTimeNanos(Lambda &&lambda) {
    namespace Nsc = std::chrono;
    using TClock = Nsc::high_resolution_clock;

    Nsc::time_point start = TClock::now();
    lambda();
    return Nsc::duration_cast<Nsc::nanoseconds>(TClock::now() - start).count();
}

class TimeFormatter {
public:
    static std::string Format(uint64_t valueNs, uint64_t uncertaintyNs) {
        auto [unit, scale] = SelectUnit(valueNs);

        double scaledValue = valueNs / scale;
        double scaledUncertainty = uncertaintyNs / scale;

        int decimalPlaces = GetDecimalPlaces(scaledUncertainty);

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(decimalPlaces);
        oss << scaledValue << " " << unit << " ± " << scaledUncertainty << " " << unit;

        return oss.str();
    }

    static std::string Format(uint64_t valueNs) {
        auto [unit, scale] = SelectUnit(valueNs);
        double scaledValue = valueNs / scale;

        int decimalPlaces = GetDecimalPlacesForValue(scaledValue);

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(decimalPlaces);
        oss << scaledValue << " " << unit;

        return oss.str();
    }

private:
    static std::pair<std::string, double> SelectUnit(uint64_t nanoseconds) {
        if (nanoseconds >= 1'000'000'000) {
            return {"s", 1e9};
        } else if (nanoseconds >= 1'000'000) {
            return {"ms", 1e6};
        } else if (nanoseconds >= 1'000) {
            return {"μs", 1e3};
        } else {
            return {"ns", 1.0};
        }
    }

    static int GetDecimalPlaces(double scaledUncertainty) {
        if (scaledUncertainty < 0.01) return 2;

        double log_val = std::log10(scaledUncertainty);
        int magnitude = static_cast<int>(std::floor(log_val));

        if (scaledUncertainty >= 100.0) {
            return 0;
        } else if (scaledUncertainty >= 10.0) {
            return 1;
        } else if (scaledUncertainty >= 1.0) {
            return 1;
        } else {
            return std::max(0, -magnitude + 1);
        }
    }

    static int GetDecimalPlacesForValue(double scaledValue) {
        if (scaledValue < 0.01) return 4;

        double absValue = std::abs(scaledValue);

        if (absValue >= 1000.0) {
            return 0;
        } else if (absValue >= 100.0) {
            return 1;
        } else if (absValue >= 10.0) {
            return 2;
        } else if (absValue >= 1.0) {
            return 2;
        } else if (absValue >= 0.1) {
            return 3;
        } else {
            return 4;
        }
    }

};

template <typename TValue>
struct TStatistics {
    TValue N;

    TValue Min, Max;

    double Median;
    double MAD;

    double Mean;
    double Stdev;

    void Dump(IOutputStream &OS) const {
        OS << "Median = " << TimeFormatter::Format(Median, MAD) << " (MAD)\n";
        OS << "Mean = " << TimeFormatter::Format(Mean, Stdev) << " (Std. dev.)\n";
        OS << "N = " << N << " [ " << TimeFormatter::Format(Min) << ", " << TimeFormatter::Format(Max) << " ]\n";
    }
};

// TValue is arithmetic and can be compared with itself, added to itself, and dividable by floats
// TValue is assumed to be cheap to pass by value
template <typename TValue>
class TRunningStatistics {
public:
    TRunningStatistics()
        : MaxHeap_()
        , MinHeap_()
        , N_(0)
        , Total_(0)
        , Min_(std::nullopt)
        , Max_(std::nullopt)
    {
    }

    void AddValue(TValue num) {
        Total_ += num;
        ++ N_;

        if (!Min_) {
            Min_ = num;
        } else {
            Min_ = Min(*Min_, num);
        }

        if (!Max_) {
            Max_ = num;
        } else {
            Max_ = Max(*Max_, num);
        };

        if (MaxHeap_.empty() || num <= MaxHeap_[0]) {
            MaxHeap_.push_back(num);
            std::push_heap(MaxHeap_.begin(), MaxHeap_.end());
        } else {
            MinHeap_.push_back(num);
            std::push_heap(MinHeap_.begin(), MinHeap_.end(), std::greater<TValue>{});
        }

        if (MaxHeap_.size() > MinHeap_.size() + 1) {
            // MinHeap_.push(MaxHeap_.top());
            MinHeap_.push_back(MaxHeap_[0]);
            std::push_heap(MinHeap_.begin(), MinHeap_.end(), std::greater<TValue>{});

            // MaxHeap_.pop();
            std::pop_heap(MaxHeap_.begin(), MaxHeap_.end());
            MaxHeap_.pop_back();
        } else if (MinHeap_.size() > MaxHeap_.size()) {
            // MaxHeap_.push(MinHeap_.top());
            MaxHeap_.push_back(MinHeap_[0]);
            std::push_heap(MaxHeap_.begin(), MaxHeap_.end());

            // MinHeap_.pop();
            std::pop_heap(MinHeap_.begin(), MinHeap_.end(), std::greater<TValue>{});
            MinHeap_.pop_back();
        }
    }

    double GetMean() const {
        assert(N_ != 0);

        return Total_ / static_cast<double>(N_);
    }

    double CalculateStdDev() const {
        assert(N_ != 0);

        if (N_ == 1) {
            return 0;
        }

        auto mean = GetMean();

        double deviationSquaresSum = 0;
        auto addValue = [&](TValue value) {
            double deviation = abs(value - mean);
            deviationSquaresSum += deviation * deviation;
        };

        for (TValue value : MaxHeap_) {
            addValue(value);
        }

        for (TValue value : MinHeap_) {
            addValue(value);
        }

        return std::sqrt(deviationSquaresSum / (N_ - 1));
    }

    double GetMedian() const {
        assert(!MaxHeap_.empty() || !MinHeap_.empty());

        if (MaxHeap_.size() > MinHeap_.size()) {
            return MaxHeap_[0];
        } else {
            return (MaxHeap_[0] + MinHeap_[0]) / 2.0;
        }
    }

    double CalculateMAD() const {
        assert(!MaxHeap_.empty() || !MinHeap_.empty());

        std::vector<double> deviation;
        double median = GetMedian();

        for (TValue value : MaxHeap_) {
            deviation.push_back(std::abs(value - median));
        }

        for (TValue value : MinHeap_) {
            deviation.push_back(std::abs(value - median));
        }

        std::sort(deviation.begin(), deviation.end());
        return (deviation[deviation.size() / 2] + deviation[(deviation.size() - 1) / 2]) / 2.0;

    }

    TValue GetMin() const {
        assert(Min_);
        return *Min_;
    }

    TValue GetMax() const {
        assert(Max_);
        return *Max_;
    }

    TValue GetTotal() const {
        return Total_;
    }

    ui32 GetN() const {
        return N_;
    }

    TStatistics<TValue> GetStatistics() const {
        return {
            .N = GetN(),
            .Min = GetMin(),
            .Max = GetMax(),
            .Median = GetMedian(),
            .MAD = CalculateMAD(),
            .Mean = GetMean(),
            .Stdev = CalculateStdDev(),
        };
    }

private:
    std::vector<TValue> MaxHeap_;
    std::vector<TValue> MinHeap_;

    // std::priority_queue<TValue, std::vector<TValue>> MaxHeap_;
    // std::priority_queue<TValue, std::vector<TValue>, std::greater<TValue>> MinHeap_;

    ui32 N_;
    TValue Total_;

    std::optional<TValue> Min_;
    std::optional<TValue> Max_;
};

struct TRepeatedTestConfig {
    ui32 MinRepeats;
    ui32 MaxRepeats;
    ui64 Timeout;
};

template <typename TLambda>
std::optional<TStatistics<ui64>> RepeatedTest(TRepeatedTestConfig config, TLambda &&lambda) {
    assert(config.MinRepeats >= 1);

    TRunningStatistics<ui64> stats;
    do {
        bool hasTimedOut = false;
        ui64 ellapsedTime = MeasureTimeNanos([&]() { hasTimedOut = lambda(); });

        if (!hasTimedOut) {
            return std::nullopt;
        }

        stats.AddValue(ellapsedTime);
    } while (stats.GetN() < config.MaxRepeats &&
             (stats.GetN() < config.MinRepeats ||
              stats.GetTotal() + stats.GetMedian() <= config.Timeout));

    return stats.GetStatistics();
}


struct TBenchmarkConfig {
    TRepeatedTestConfig Warmup;
    TRepeatedTestConfig Bench;
};

template <typename Lambda>
std::optional<TStatistics<ui64>> Benchmark(TBenchmarkConfig config, Lambda &&lambda) {
    if (config.Warmup.MaxRepeats != 0) {
        auto warmupStats = RepeatedTest(config.Warmup, lambda);
        if (!warmupStats) {
            return std::nullopt;
        }
    }

    return RepeatedTest(config.Bench, lambda);
}

} // end namespace NKikimr::NKqp
