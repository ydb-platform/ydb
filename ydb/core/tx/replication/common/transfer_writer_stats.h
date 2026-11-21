#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NKikimr::NReplication {

struct TTransferWriteStats {
    TDuration ProcessingCpu;
    TDuration ProcessingTime;
    TDuration WriteDuration;
    ui64 WriteBytes;
    ui64 WriteRows;
    ui64 ProcessingErrors = 0;
    ui64 WriteErrors = 0;
};

} // namespace NKikimr::NReplication
