#include "kqp_compute_events_stats.h"
#include <util/string/builder.h>

namespace NKikimr::NKqp {


TString FormatDurationAsMilliseconds(TDuration duration) {
    return TStringBuilder() << duration.MicroSeconds() / 1000.0 << "ms";
}

TString TPerStepCounters::DebugString() const {
    return TStringBuilder() << "StepName: " << StepName << ";"
                            << "ExecutionDuration:" << FormatDurationAsMilliseconds(IntegralExecutionDuration) << ";"
                            << "WaitDuration:" << FormatDurationAsMilliseconds(IntegralWaitDuration) << ";"
                            << "RawBytesRead:" << IntegralRawBytesRead << ";";
}

}