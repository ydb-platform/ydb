#pragma once
#include "defs.h"

#include <ydb/library/actors/util/datetime.h>

namespace NKikimr {
namespace NPDisk {

struct TCpuTimer: private TNonCopyable {
    ui64 Timestamp;

    explicit TCpuTimer(ui64 ts = GetCycleCountFast()) {
        Timestamp = ts;
    }

    ui64 Elapsed() {
        ui64 ts = GetCycleCountFast();
        DoSwap(Timestamp, ts);
        return Timestamp - ts;
    }
};

} // NPDisk
} // NKikimr
