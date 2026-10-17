#include "benchmark.h"
#include "benchmark_program.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/algorithm.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <utility>

namespace NYql::NPureBench {
namespace {

void DropSlowestSamples(TVector<TDuration>& times, size_t fractionDenominator) {
    Sort(times);
    times.erase(times.end() - times.size() / fractionDenominator, times.end());
}

TMaybe<TDuration> GeometricMean(const TVector<TDuration>& times) {
    if (times.empty()) {
        return Nothing();
    }
    if (AnyOf(times, [](auto t) { return t == TDuration::Zero(); })) {
        return TDuration::Zero();
    }
    const TDuration scale = times.back();
    const double logSum = std::transform_reduce(times.cbegin(), times.cend(), 0.0, std::plus{},
                                                [scale](auto t) { return std::log(t / scale); });
    const double geometricMeanRatio = std::exp(logSum / times.size());
    // TDuration::Max() rounds up to 2^64 when converted to double.
    return geometricMeanRatio == 1.0 ? scale : scale * geometricMeanRatio;
}

TMaybe<double> CV(const TVector<TDuration>& times) {
    if (times.empty() || times.back() == TDuration::Zero()) {
        return Nothing();
    }
    const TDuration scale = times.back();
    const double meanRatio = std::transform_reduce(times.cbegin(), times.cend(), 0.0, std::plus{},
                                                   [scale](auto t) { return t / scale; }) /
                             times.size();
    const double squaredRelativeDeviations = std::transform_reduce(times.cbegin(), times.cend(), 0.0, std::plus{},
                                                                   [scale, meanRatio](auto t) { return std::pow(t / scale - meanRatio, 2); });
    return std::sqrt(squaredRelativeDeviations / times.size()) / meanRatio * 100.0;
}

template <typename TRun>
TMeasurements MeasureRuns(ui32 minRuns, TDuration minDuration, const TRun& run) {
    TMeasurements result;
    const auto started = TMonotonic::Now();
    while (result.Times.size() < minRuns || TMonotonic::Now() - started < minDuration) {
        const auto elapsed = run();
        if (!elapsed) {
            break;
        }
        result.Times.push_back(*elapsed);
    }
    result.Elapsed = TMonotonic::Now() - started;
    result.Iterations = result.Times.size();
    result.TrimmedStatistics = CalculateTrimmedStatistics(result.Times);
    return result;
}

} // namespace

TMeasurementStatistics CalculateTrimmedStatistics(TVector<TDuration> times) {
    constexpr size_t SlowestFractionDenominator = 3;
    DropSlowestSamples(times, SlowestFractionDenominator);
    return {.GeometricMean = GeometricMean(times), .CoefficientOfVariationPercent = CV(times)};
}

TMeasurements Measure(ui32 minRuns, TDuration minDuration, const std::function<TDuration()>& run) {
    return MeasureRuns(minRuns, minDuration, [&]() -> TMaybe<TDuration> { return run(); });
}

TBenchmarkResult RunBenchmarks(IBenchmarkProgram& program, const TBenchmarkOptions& options) {
    TBenchmarkResult result;
    result.Benchmark = Measure(options.MinMeasuredRuns, options.MinMeasurementDuration,
                               [&] { return program.RunMeasureQuery(EOutputMode::Discard).Statistics.Elapsed; });
    auto calibration = MeasureRuns(options.MinCalibrationRuns, options.MinCalibrationDuration, [&]() -> TMaybe<TDuration> {
        const auto calibrationResult = program.RunCalibrationQuery();
        if (const auto* statistics = std::get_if<TQueryRunStatistics>(&calibrationResult)) {
            return statistics->Elapsed;
        }
        return Nothing();
    });
    if (!calibration.Times.empty()) {
        result.Calibration = std::move(calibration);
    }
    const auto calibrationMean = result.Calibration ? result.Calibration->TrimmedStatistics.GeometricMean : TMaybe<TDuration>(TDuration::Zero());
    if (result.Benchmark.TrimmedStatistics.GeometricMean && calibrationMean) {
        result.Score = *result.Benchmark.TrimmedStatistics.GeometricMean / TDuration::MilliSeconds(1) - *calibrationMean / TDuration::MilliSeconds(1);
    }
    return result;
}

} // namespace NYql::NPureBench
