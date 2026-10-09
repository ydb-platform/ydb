#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <functional>

namespace NYql::NPureBench {

class IBenchmarkProgram;

struct TBenchmarkOptions {
    ui32 MinMeasuredRuns = 10;
    ui32 MinCalibrationRuns = 3;
    TDuration MinMeasurementDuration = TDuration::Seconds(1);
    TDuration MinCalibrationDuration = TDuration::Seconds(1);
};

struct TMeasurementStatistics {
    TMaybe<TDuration> GeometricMean;
    TMaybe<double> CoefficientOfVariationPercent;
};

struct TMeasurements {
    TVector<TDuration> Times;
    ui64 Iterations = 0;
    TDuration Elapsed;
    TMeasurementStatistics TrimmedStatistics;
};

struct TBenchmarkResult {
    TMeasurements Benchmark;
    TMaybe<TMeasurements> Calibration;
    TMaybe<double> Score;
};

// Drops the slowest third. No statistics for empty samples; no CV if all remaining durations are zero.
TMeasurementStatistics CalculateTrimmedStatistics(TVector<TDuration> times);
TMeasurements Measure(ui32 minRuns, TDuration minDuration, const std::function<TDuration()>& run);
TBenchmarkResult RunBenchmarks(IBenchmarkProgram& program, const TBenchmarkOptions& options);

} // namespace NYql::NPureBench
