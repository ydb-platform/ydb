#include "kqp_compute_events_stats.h"
#include <util/string/builder.h>

namespace NKikimr::NKqp {


TString FormatDurationAsMilliseconds(TDuration duration) {
    return TStringBuilder() << duration.MicroSeconds() / 1000.0 << "ms";
}

TString TPerStepStatistics::DebugString() const {
    return TStringBuilder() << "StepName: " << StepName << ";"
                            << "IntegralExecutionDuration:" << FormatDurationAsMilliseconds(IntegralExecutionDuration) << ";"
                            << "IntegralWaitDuration:" << FormatDurationAsMilliseconds(IntegralWaitDuration) << ";"
                            << "DeltaRawBytesRead:" << DeltaRawBytesRead << ";";
}

}