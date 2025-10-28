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
    ui64 N;

    TValue Min, Max;

    double Median;
    double MAD;

    double Mean;
    double Stdev;

    double Q1;
    double Q3;

    double IQR;
    std::vector<TValue> Outliers;
};

void DumpTimeStatistics(TStatistics<ui64> stats, IOutputStream &OS) {
    OS << "Median = " << TimeFormatter::Format(stats.Median, stats.MAD) << " (MAD)\n";

    OS << "N = " << stats.N << "\n";
    OS << "Min, Max = [ " << TimeFormatter::Format(stats.Min) << ", " << TimeFormatter::Format(stats.Max) << " ]\n";

    OS << "Mean = " << TimeFormatter::Format(stats.Mean, stats.Stdev) << " (Std. dev.)\n";
}

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

        IterateElements([&](TValue value) {
            deviation.push_back(std::abs(value - median));
        });

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
        auto elements = CollectElements();
        std::sort(elements.begin(), elements.end());

        double q1 = CalculateQuartile(elements, 0.25);
        double q3 = CalculateQuartile(elements, 0.75);
        double iqr = q3 - q1;

        double median = GetMedian();

        const double multiplier = 1.5;
        double lowerFence = q1 - multiplier * iqr;
        double upperFence = q3 + multiplier * iqr;

        std::vector<TValue> outliers;
        for (TValue value : elements) {
            if (value < lowerFence || value > upperFence) {
                outliers.push_back(value);
            }
        }

        return {
            .N = GetN(),
            .Min = GetMin(),
            .Max = GetMax(),
            .Median = median,
            .MAD = CalculateMAD(),
            .Mean = GetMean(),
            .Stdev = CalculateStdDev(),
            .Q1 = q1,
            .Q3 = q3,
            .IQR = iqr,
            .Outliers = outliers
        };
    }

    std::vector<TValue> CollectElements() const {
        std::vector<TValue> values;
        IterateElements([&](TValue value) {
            values.push_back(value);
        });

        return values;
    }

    TRunningStatistics<i64> operator-(TRunningStatistics rhsStats) const {
        TRunningStatistics<i64> stats;
        IterateElements([&](TValue lhs) {
            rhsStats.IterateElements([&](TValue rhs) {
                stats.AddValue((i64) lhs - (i64) rhs);
            });
        });

        return stats;
    }

    TRunningStatistics<double> operator/(TRunningStatistics rhsStats) const {
        TRunningStatistics<double> stats;
        IterateElements([&](TValue lhs) {
            rhsStats.IterateElements([&](TValue rhs) {
                stats.AddValue((double) lhs / (double) rhs);
            });
        });

        return stats;
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

private:
    template <typename Lambda>
    void IterateElements(Lambda &&lambda) const {
        for (TValue value : MaxHeap_) {
            lambda(value);
        }

        for (TValue value : MinHeap_) {
            lambda(value);
        }
    }

    static double CalculateQuartile(const std::vector<TValue>& data, double percentile) { // TODO: rename
        assert(percentile >= 0.0 && percentile <= 1.0);
        assert(std::is_sorted(data.begin(), data.end()));

        double position = percentile * (data.size() - 1);
        int lowerIndex = floor(position);
        int upperIndex = ceil(position);

        if (lowerIndex == upperIndex) {
            return data[lowerIndex];
        }

        double weight = position - lowerIndex;
        return data[lowerIndex] * (1 - weight) + data[upperIndex] * weight;
    }
};

template <typename TValue>
static std::string joinVector(const std::vector<TValue> &data) {
    if (data.empty()) {
        return "";
    }

    std::string str;
    str += std::to_string(data[0]);

    for (ui32 i = 1; i < data.size(); ++ i) {
        str += ";" + std::to_string(data[i]);
    }

    return str;
}


void DumpBoxPlotCSVHeader(IOutputStream &OS) {
    OS << "N,median,Q1,Q3,IQR,MAD,outliers\n";
}

template <typename TValue>
void DumpBoxPlotToCSV(IOutputStream &OS, ui32 i, TStatistics<TValue> stat) {
    OS << i << "," << stat.Median << "," << stat.Q1  << "," << stat.Q3 << "," << stat.IQR << "," << stat.MAD << "," << joinVector(stat.Outliers) << "\n";
}


struct TRepeatedTestConfig {
    ui32 MinRepeats;
    ui32 MaxRepeats;
    ui64 Timeout;
};

template <typename TLambda>
std::optional<TRunningStatistics<ui64>> RepeatedTest(TRepeatedTestConfig config, ui64 singleRunTimeout, double thresholdMAD, TLambda &&lambda) {
    assert(config.MinRepeats >= 1);

    TRunningStatistics<ui64> stats;
    do {
        bool hasTimedOut = false;
        ui64 ellapsedTime = MeasureTimeNanos([&]() { hasTimedOut = !lambda(); });

        if (hasTimedOut || ellapsedTime > singleRunTimeout) {
            return std::nullopt;
        }

        stats.AddValue(ellapsedTime);
    } while ((stats.GetN() < config.MaxRepeats) &&
             (stats.GetN() < config.MinRepeats ||
              (stats.GetTotal() + stats.GetMedian() <= config.Timeout &&
               stats.CalculateMAD() / stats.GetMedian() > thresholdMAD)));

    return stats;
}


struct TBenchmarkConfig {
    TRepeatedTestConfig Warmup;
    TRepeatedTestConfig Bench;
    ui64 SingleRunTimeout;
    double MADThreshold;
};

template <typename Lambda>
std::optional<TRunningStatistics<ui64>> Benchmark(TBenchmarkConfig config, Lambda &&lambda) {
    if (config.Warmup.MaxRepeats != 0) {
        auto warmupStats = RepeatedTest(config.Warmup, config.SingleRunTimeout, config.MADThreshold, lambda);
        if (!warmupStats) {
            return std::nullopt;
        }
    }

    return RepeatedTest(config.Bench, config.SingleRunTimeout, config.MADThreshold, lambda);
}

} // end namespace NKikimr::NKqp
