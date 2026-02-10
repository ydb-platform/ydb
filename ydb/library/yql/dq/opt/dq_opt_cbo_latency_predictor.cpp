#include "dq_opt_cbo_latency_predictor.h"
#include <cmath>
#include <optional>
#include <vector>
#include <set>
#include <iomanip>

namespace NYql::NDq {

    // ==================== Helper Functions ====================

    double ComputeStdDev(const std::vector<double>& values) {
        if (values.empty()) {
            return 0.0;
        }

        double mean = 0.0;
        for (double val : values) {
            mean += val;
        }
        mean /= values.size();

        double variance = 0.0;
        for (double val : values) {
            variance += (val - mean) * (val - mean);
        }
        variance /= values.size();

        return std::sqrt(variance);
    }

    std::pair<double, double> ComputeMeanAndStdDev(const std::vector<double>& values) {
        if (values.empty()) {
            return {0.0, 0.0};
        }

        double mean = 0.0;
        for (double val : values) {
            mean += val;
        }
        mean /= values.size();

        double variance = 0.0;
        for (double val : values) {
            variance += (val - mean) * (val - mean);
        }
        variance /= values.size();

        return {mean, std::sqrt(variance)};
    }

    std::optional<double> ComputeAssortativity(
        const std::vector<double>& sourceDegrees,
        const std::vector<double>& targetDegrees
    ) {
        if (sourceDegrees.size() != targetDegrees.size() || sourceDegrees.empty()) {
            return std::nullopt;
        }

        double sumSource = 0.0, sumTarget = 0.0;
        double sumSourceTarget = 0.0;
        double sumSourceSq = 0.0, sumTargetSq = 0.0;
        size_t numEdges = sourceDegrees.size();

        for (size_t idx = 0; idx < numEdges; ++idx) {
            double source = sourceDegrees[idx];
            double target = targetDegrees[idx];
            sumSource += source;
            sumTarget += target;
            sumSourceTarget += source * target;
            sumSourceSq += source * source;
            sumTargetSq += target * target;
        }

        double numEdgesD = static_cast<double>(numEdges);
        double varSource = numEdgesD * sumSourceSq - sumSource * sumSource;
        double varTarget = numEdgesD * sumTargetSq - sumTarget * sumTarget;

        if (varSource < 1e-12 || varTarget < 1e-12) {
            return 0.0;
        }

        return (numEdgesD * sumSourceTarget - sumSource * sumTarget) / std::sqrt(varSource * varTarget);
    }

    // ==================== Safe Operators ====================

    const double EPSILON = 1e-9;

    template <typename T>
    T Signum(T value) {
        return (T(0) < value) - (value < T(0));
    }

    double SafeDiv(double a, double b) {
        if (b == 0.0) {
            return a / EPSILON;
        }

        return a / (Signum(b) * std::max(std::abs(b), EPSILON));
    }

    double SafeSqrt(double a) {
        return std::sqrt(std::abs(a));
    }

    double SafeLog(double a) {
        return std::log(std::max(std::abs(a), EPSILON));
    }

    double SafePow(double base, double exponent) {
        return std::pow(std::abs(base), exponent);
    }

    double SafeExpm1(double a) {
        // 40 is a bit shy of 44 (for safety) which is the maximum
        // whole number for which the result will fit in ui64
        if (a > 40.0) {
            return std::exp(40.0);
        }
        if (a < 0) {
            return 0;
        }
        return std::exp(a) - 1;
    }

    std::pair<std::string, double> SelectUnit(ui64 nanoseconds) {
        if (nanoseconds >= 1'000'000'000) {
            return {"s", 1e9};
        } else if (nanoseconds >= 1'000'000) {
            return {"ms", 1e6};
        } else if (nanoseconds >= 1'000) {
            return {"mcs", 1e3};
        } else {
            return {"ns", 1.0};
        }
    }

    int GetDecimalPlaces(double scaledValue) {
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

    std::string FormatTime(ui64 valueNs) {
        auto [unit, scale] = SelectUnit(valueNs);
        double scaledValue = valueNs / scale;

        int decimalPlaces = GetDecimalPlaces(scaledValue);

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(decimalPlaces);
        oss << scaledValue << " " << unit;

        return oss.str();
    }
} // end namespace NYql::NDq
