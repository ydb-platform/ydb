#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
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

struct TScanStatistics{
    TMap<ui32, TPerStepScanStatistics> PerStepStats;
    TMap<TString, ui64> ArbitraryDeltaCounters;
};

}   // namespace NKikimr::NKqp
