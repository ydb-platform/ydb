#include "kqp_compute_events_stats.h"
#include <util/string/builder.h>

namespace NKikimr::NKqp {


inline TString FormatDurationAsMilliseconds(TDuration duration) {
    return TStringBuilder() << duration.MicroSeconds()/1000.0 << "ms";
}

TString TPerStepScanCountersSnapshot::DebugString() const {
    return TStringBuilder() << "ExecutionDuration:" << FormatDurationAsMilliseconds(ExecutionDuration) << ";"
                            << "WaitDuration:" << FormatDurationAsMilliseconds(WaitDuration) << ";"
                            << "RawBytesRead:" << RawBytesRead << ";";
}

}