#pragma once

#include <limits>
#include <numeric>
#include <random>
#include <util/generic/array_size.h>
#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <chrono>
#include <cassert>
#include <algorithm>
#include <queue>
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


struct TComputedStatistics {
    ui64 N;
    double Min, Max;

    double Median;
    double MAD;

    double Q1;
    double Q3;
    double IQR;

    double Mean;
    double Stdev;

    static std::string GetCSVHeader();
    void ToCSV(IOutputStream &os) const;
};


double CalculatePercentile(const std::vector<double>& data, double percentile);
double CalculateMedian(std::vector<double>& data);
double CalculateMAD(const std::vector<double>& data, double median, std::vector<double>& storage);
double CalculateMean(const std::vector<double>& data);
double CalculateSampleStdDev(const std::vector<double>& data, double mean);


class TStatistics {
public:
    TStatistics(std::vector<double> samples, double min, double max)
        : Samples_(std::move(samples))
        , Min_(min)
        , Max_(max)
    {
    }

    TComputedStatistics ComputeStatistics() const;

    ui64 GetN() const {
        return Samples_.size();
    }

    double GetMin() const {
        return Min_;
    }

    double GetMax() const {
        return Max_;
    }

    void Merge(const TStatistics& other);

    // Operations on two random values, very expensive.
    // Each operation produces cartesian product of all possibilities if
    // it's small enough or fallbacks to Monte Carlo methods if not.
    TStatistics operator-(const TStatistics &rhs) const;
    TStatistics operator+(const TStatistics &rhs) const;
    TStatistics operator*(const TStatistics &rhs) const;
    TStatistics operator/(const TStatistics &rhs) const;

    // Only gives precisely correct min & max if you map with monotonic function
    template <typename TMapLambda>
    TStatistics Map(TMapLambda map) {
        std::vector<double> samples;
        double min = std::numeric_limits<double>::max();
        double max = std::numeric_limits<double>::min();

        for (double value : Samples_) {
            double newValue = map(value);
            samples.push_back(newValue);

            min = std::min(newValue, min);
            max = std::max(newValue, max);
        }

        double candidates[] = { min, max, map(Min_), map(Max_) };
        return {
            std::move(samples),
            *std::min_element(candidates, candidates + Y_ARRAY_SIZE(candidates)),
            *std::max_element(candidates, candidates + Y_ARRAY_SIZE(candidates))
        };
    }

    template <typename TFilterLambda>
    TStatistics Filter(TFilterLambda &&filter) const {
        std::vector<double> samples;

        // That's an upper bound since this function can only remove elements.
        // This resize can be memory inefficient, but is likely faster.
        samples.reserve(Samples_.size());

        double min = std::numeric_limits<double>::max();
        double max = std::numeric_limits<double>::min();

        for (double value : Samples_) {
            if (!filter(value)) {
                continue;
            }

            samples.push_back(value);

            min = std::min(value, min);
            max = std::max(value, max);
        }

        double globalMin = filter(Min_) ? Min_ : min;
        double globalMax = filter(Max_) ? Max_ : max;

        return {std::move(samples), globalMin, globalMax};
    }


private:
    mutable std::vector<double> Samples_;
    double Min_;
    double Max_;

    // This lets us operate on two random values, it computes distribution of values after arbitrary operation
    template <typename TCombiner>
    std::vector<double>
        Combine(const TStatistics& rhs, TCombiner&& combine, ui64 maxSamples = 1e8) const {

        std::vector<double> combined;

        ui64 cartesianProductSize = Samples_.size() * rhs.Samples_.size();

        if (cartesianProductSize < maxSamples) {
            combined.reserve(cartesianProductSize);

            for (ui64 i = 0; i < Samples_.size(); ++ i) {
                for (ui64 j = 0; j < rhs.Samples_.size(); ++ j) {
                    combined.push_back(combine(Samples_[i], rhs.Samples_[j]));
                }
            }

            return combined;
        }

        // Fallback to Monte Carlo if the sample size is too big:
        std::random_device randomDevice;
        std::mt19937 rng(/*seed=*/randomDevice());

        std::uniform_int_distribution<> distributionLHSIdx(0, Samples_.size() - 1);
        std::uniform_int_distribution<> distributionRHSIdx(0, rhs.Samples_.size() - 1);

        for (ui64 repeatIdx = 0; repeatIdx < maxSamples; ++ repeatIdx) {
            ui64 i = distributionLHSIdx(rng);
            ui64 j = distributionRHSIdx(rng);

            combined.push_back(combine(Samples_[i], rhs.Samples_[j]));
        }

        return combined;
    }

};


class TRunningStatistics {
public:
    TRunningStatistics()
        : MinHeap_()
        , MaxHeap_()
        , Samples_()
        , MADStorage_()
        , Total_(0)
        , Min_(std::nullopt)
        , Max_(std::nullopt)
    {
    }

    void AddValue(double num);

    double GetMedian() const;
    double CalculateMAD() const;

    double GetTotal() const {
        return Total_;
    }

    ui32 GetN() const {
        return Samples_.size();
    }

    double GetMin() const {
        assert(Min_);
        return *Min_;
    }

    double GetMax() const {
        assert(Max_);
        return *Max_;
    }

    TStatistics GetStatistics() {
        assert(GetN() > 0);
        return {Samples_, *Min_, *Max_};
    }

private:
    std::priority_queue<double, std::vector<double>, std::greater<double>> MinHeap_;
    std::priority_queue<double, std::vector<double>> MaxHeap_;

    std::vector<double> Samples_;

    // This vector is used to not allocate memory every time
    // we are requested to compute MAD and need to calculate diviations
    mutable std::vector<double> MADStorage_;

    double Total_;

    std::optional<double> Min_;
    std::optional<double> Max_;
};



class TimeFormatter {
public:
    static std::string Format(uint64_t valueNs, ui64 uncertaintyNs);
    static std::string Format(uint64_t valueNs);
};

void DumpTimeStatistics(const TComputedStatistics &stats, IOutputStream &OS);


struct TRepeatedTestConfig {
    ui64 MinRepeats;
    ui64 MaxRepeats;
    ui64 Timeout;
};

template <typename TLambda>
std::optional<TStatistics> RepeatedTest(TRepeatedTestConfig config, ui64 singleRunTimeout, double thresholdMAD, TLambda &&lambda) {
    assert(config.MinRepeats >= 1);

    TRunningStatistics stats;
    do {
        bool hasTimedOut = false;
        ui64 ellapsedTime = MeasureTimeNanos([&]() { hasTimedOut = !lambda(); });

        if (hasTimedOut) {
            return std::nullopt;
        }

        if (ellapsedTime > singleRunTimeout) {
            break;
        }

        stats.AddValue(ellapsedTime);
    } while ((stats.GetN() < config.MaxRepeats) &&
             (stats.GetN() < config.MinRepeats ||
              (stats.GetTotal() + stats.GetMedian() < config.Timeout &&
               stats.CalculateMAD() / stats.GetMedian() > thresholdMAD)));

    return std::move(stats).GetStatistics();
}


struct TBenchmarkConfig {
    TRepeatedTestConfig Warmup;
    TRepeatedTestConfig Bench;
    ui64 SingleRunTimeout;
    double MADThreshold;
};

template <typename Lambda>
std::optional<TStatistics> Benchmark(TBenchmarkConfig config, Lambda &&lambda) {
    if (config.Warmup.MaxRepeats != 0) {
        auto warmupStats = RepeatedTest(config.Warmup, config.SingleRunTimeout, config.MADThreshold, lambda);
        if (!warmupStats) {
            return std::nullopt;
        }
    }

    return RepeatedTest(config.Bench, config.SingleRunTimeout, config.MADThreshold, lambda);
}

} // end namespace NKikimr::NKqp
