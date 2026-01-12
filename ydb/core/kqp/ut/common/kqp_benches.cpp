#include "kqp_benches.h"

#include <util/generic/array_size.h>
#include <util/system/yassert.h>

namespace NKikimr::NKqp {

std::string TComputedStatistics::GetCSVHeader() {
    return "median,MAD,Q1,Q3,IQR,mean,stdev,n,min,max";
}

void TComputedStatistics::ToCSV(IOutputStream& os) const {
    os << Median << "," << MAD
        << "," << Q1 << "," << Q3 << "," << IQR
        << "," << Mean << "," << Stdev
        << "," << N << "," << Min << "," << Max;
}

double CalculatePercentile(std::vector<double>& data, double percentile) {
    if (data.empty()) {
        throw std::invalid_argument("At least one element needed to get percentiles");
    }

    double position = percentile * (data.size() - 1);

    // Get range of positions that are considered to be at this percentile
    ui64 lower = std::floor(position);
    ui64 upper = std::ceil(position);
    double weight = position - lower;

    std::nth_element(data.begin(), data.begin() + lower, data.end());
    double lowerValue = data[lower];

    // If position is precise, just get that element:
    if (lower == upper) {
        return lowerValue;
    }

    // If it's a range, then interpolate linearly
    // e.g. median of even number of elements is average of the central two
    std::nth_element(data.begin(), data.begin() + upper, data.end());
    double upperValue = data[upper];

    return lowerValue + weight * (upperValue - lowerValue);
}

double CalculateMedian(std::vector<double>& data) {
    return CalculatePercentile(data, 0.5);
}

double CalculateMAD(const std::vector<double>& data, double median, std::vector<double>& storage) {
    storage.clear();
    storage.reserve(data.size());

    for (double value : data) {
        storage.push_back(abs(value - median));
    }

    return CalculatePercentile(storage, 0.5);
}

double CalculateMean(const std::vector<double>& data) {
    return std::accumulate(data.begin(), data.end(), 0.0) / static_cast<double>(data.size());
}

double CalculateSampleStdDev(const std::vector<double>& data, double mean) {
    if (data.size() == 0) {
        return 0;
    }

    double deviationSquaresSum = 0;
    for (double value : data) {
        double deviation = abs(value - mean);
        deviationSquaresSum += deviation * deviation;
    };

    return std::sqrt(deviationSquaresSum / static_cast<double>(data.size() - 1));
}

void TRunningStatistics::AddValue(double num) {
    Samples_.push_back(num);
    Total_ += num;

    if (!Min_ || !Max_) {
        Min_ = num;
        Max_ = num;
    }

    Min_ = Min(*Min_, num);
    Max_ = Max(*Max_, num);

    if (MaxHeap_.empty() || num <= MaxHeap_.top()) {
        MaxHeap_.push(num);
    } else {
        MinHeap_.push(num);
    }

    if (MaxHeap_.size() > MinHeap_.size() + 1) {
        MinHeap_.push(MaxHeap_.top());
        MaxHeap_.pop();
    } else if (MinHeap_.size() > MaxHeap_.size()) {
        MaxHeap_.push(MinHeap_.top());
        MinHeap_.pop();
    }
}

double TRunningStatistics::GetMedian() const {
    Y_ASSERT(!MaxHeap_.empty() || !MinHeap_.empty());

    if (MaxHeap_.size() > MinHeap_.size()) {
        return MaxHeap_.top();
    } else {
        return (MaxHeap_.top() + MinHeap_.top()) / 2.0;
    }
}

double TRunningStatistics::CalculateMAD() const {
    return ::NKikimr::NKqp::CalculateMAD(Samples_, GetMedian(), MADStorage_);
}

TComputedStatistics TStatistics::ComputeStatistics() const {
    double q1 = CalculatePercentile(Samples_, 0.25);
    double q3 = CalculatePercentile(Samples_, 0.75);
    double iqr = q3 - q1;

    double median = CalculateMedian(Samples_);

    // ComputeStatistics should be called once per statistics combiner at most.
    // Otherwise we would hold on to storage instead of reallocating it each time:
    std::vector<double> storage;
    double mad = ::NKikimr::NKqp::CalculateMAD(Samples_, median, storage);

    double mean = CalculateMean(Samples_);
    double stdev = CalculateSampleStdDev(Samples_, mean);

    return {
        .N = Samples_.size(),
        .Min = Min_,
        .Max = Max_,
        .Median = median,
        .MAD = mad,
        .Q1 = q1,
        .Q3 = q3,
        .IQR = iqr,
        .Mean = mean,
        .Stdev = stdev,
    };
}

void TStatistics::Merge(const TStatistics& other) {
    Samples_.insert(Samples_.end(), other.Samples_.begin(), other.Samples_.end());
    Min_ = std::min(Min_, other.Min_);
    Max_ = std::max(Max_, other.Max_);
}

TStatistics TStatistics::operator-(const TStatistics& statsRHS) const {
    auto op = [](double lhs, double rhs) {
        return lhs - rhs;
    };

    return TStatistics{
        Combine(statsRHS, op),
        Min_ - statsRHS.Max_,
        Max_ - statsRHS.Min_
    };
}

TStatistics TStatistics::operator+(const TStatistics& statsRHS) const {
    auto op = [](double lhs, double rhs) {
        return lhs - rhs;
    };

    return TStatistics{
        Combine(statsRHS, op),
        Min_ + statsRHS.Min_,
        Max_ + statsRHS.Max_
    };
}

TStatistics TStatistics::operator*(const TStatistics& statsRHS) const {
    auto op = [](double lhs, double rhs) {
        return lhs - rhs;
    };

    double candidates[] = {
        Min_ * statsRHS.Min_,
        Min_ * statsRHS.Max_,
        Max_ * statsRHS.Min_,
        Max_ * statsRHS.Max_,
    };

    return TStatistics{
        Combine(statsRHS, op),
        *std::min_element(candidates, candidates + Y_ARRAY_SIZE(candidates)),
        *std::max_element(candidates, candidates + Y_ARRAY_SIZE(candidates))
    };
}

TStatistics TStatistics::operator/(const TStatistics& statsRHS) const {
    auto op = [](double lhs, double rhs) {
        return lhs / rhs;
    };

    double candidates[] = {
        Min_ / statsRHS.Min_,
        Min_ / statsRHS.Max_,
        Max_ / statsRHS.Min_,
        Max_ / statsRHS.Max_,
    };

    return TStatistics{
        Combine(statsRHS, op),
        *std::min_element(candidates, candidates + Y_ARRAY_SIZE(candidates)),
        *std::max_element(candidates, candidates + Y_ARRAY_SIZE(candidates))
    };
}

static std::pair<std::string, double> SelectUnit(ui64 nanoseconds) {
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
    if (scaledUncertainty < 0.01) {
        return 2;
    }

    double log = std::log10(scaledUncertainty);
    int magnitude = static_cast<int>(std::floor(log));

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
    if (scaledValue < 0.01) {
        return 4;
    }

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

std::string TimeFormatter::Format(ui64 valueNs, ui64 uncertaintyNs) {
    auto [unit, scale] = SelectUnit(valueNs);

    double scaledValue = valueNs / scale;
    double scaledUncertainty = uncertaintyNs / scale;

    int decimalPlaces = GetDecimalPlaces(scaledUncertainty);

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(decimalPlaces);
    oss << scaledValue << " " << unit << " ± " << scaledUncertainty << " " << unit;

    return oss.str();
}

std::string TimeFormatter::Format(ui64 valueNs) {
    auto [unit, scale] = SelectUnit(valueNs);
    double scaledValue = valueNs / scale;

    int decimalPlaces = GetDecimalPlacesForValue(scaledValue);

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(decimalPlaces);
    oss << scaledValue << " " << unit;

    return oss.str();
}

void DumpTimeStatistics(const TComputedStatistics& stats, IOutputStream& os) {
    os << "Median = " << TimeFormatter::Format(stats.Median, stats.MAD) << " (MAD)\n";

    os << "Q1 = " << TimeFormatter::Format(stats.Q1)
       << ", Q3 = " << TimeFormatter::Format(stats.Q3)
       << ", IQR = " << TimeFormatter::Format(stats.IQR) << "\n";

    os << "N = " << stats.N << "\n";
    os << "Min, Max = [ "
       << TimeFormatter::Format(stats.Min)
       << ", " << TimeFormatter::Format(stats.Max)
       << " ]\n";

    os << "Mean = " << TimeFormatter::Format(stats.Mean, stats.Stdev) << " (Std. dev.)\n";
}

} // namespace NKikimr::NKqp
