#pragma once

#include <util/datetime/base.h>
namespace NKikimr::NKqp {
TString FormatDurationAsMilliseconds(TDuration duration);

struct TPerStepScanStatistics {
    TString StepName;
    // integral counter only grows during scan, delta counter gets zeroed on another chunk
    TDuration IntegralExecutionDuration;
    TDuration IntegralWaitDuration;
    ui64 DeltaRawBytesRead = 0;
    TString DebugString() const;
};

using TScanStatistics = TMap<ui32, TPerStepScanStatistics>;

}   // namespace NKikimr::NKqp
