#pragma once

#include <util/datetime/base.h>
namespace NKikimr::NKqp {
TString FormatDurationAsMilliseconds(TDuration duration);
struct TPerStepScanCountersSnapshot {
    TDuration ExecutionDuration;
    TDuration WaitDuration;
    ui64 RawBytesRead = 0;
    TString DebugString() const;
};

}