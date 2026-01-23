#pragma once

#include <util/datetime/base.h>
namespace NKikimr::NKqp {
TString FormatDurationAsMilliseconds(TDuration duration);
struct TCurrentPerStepScanCounters {
    // integral counter only grows during scan, delta counter gets zeroed on another chunk
    TDuration IntegralExecutionDuration;
    TDuration IntegralWaitDuration;
    ui64 IntegralRawBytesRead = 0;
    TString DebugString() const;
};

struct TCurrentNamedPerStepScanCounters {
    TString StepName;
    TCurrentPerStepScanCounters Counters;
};

}   // namespace NKikimr::NKqp
