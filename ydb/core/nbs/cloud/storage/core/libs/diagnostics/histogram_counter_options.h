#pragma once

#include <util/generic/flags.h>

namespace NYdb::NBS {

enum class EHistogramCounterOption
{
    ReportSingleCounter = (1 << 0),
    ReportMultipleCounters = (1 << 1),
    // Report time histograms with milliseconds buckets. Percentiles are still
    // reported in microseconds.
    UseMsUnitsForTimeHistogram = (1 << 2),
};

Y_DECLARE_FLAGS(EHistogramCounterOptions, EHistogramCounterOption);
Y_DECLARE_OPERATORS_FOR_FLAGS(EHistogramCounterOptions);

}   // namespace NYdb::NBS
