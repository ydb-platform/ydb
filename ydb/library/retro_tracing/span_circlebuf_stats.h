#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NRetro {

struct TSpanCircleBufStats {
    ui64 Writes = 0;
    ui64 SuccessfulWrites = 0;
    ui64 SpansRejectedDueToOversize = 0;
    ui64 FailedLocks = 0;
    ui64 Reads = 0;
    ui64 Overflows = 0;

    TString ToString() const;
};

} // namespace NRetro
