#pragma once


namespace NMonitoring {

enum class EMetricValueType {
    UNKNOWN,
    DOUBLE,
    INT64,
    UINT64,
    HISTOGRAM,
    SUMMARY,
    LOGHISTOGRAM,
};

} // namespace NMonitoring
