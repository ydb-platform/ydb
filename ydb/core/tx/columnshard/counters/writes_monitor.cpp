#include "writes_monitor.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

TAtomicCounter TWritesMonitor::WritesInFlight = 0;
TAtomicCounter TWritesMonitor::WritesSizeInFlight = 0;

void TWritesMonitor::OnStartWrite(const ui64 dataSize) {
    ++WritesInFlightLocal;
    WritesSizeInFlightLocal += dataSize;
    WritesInFlight.Inc();
    WritesSizeInFlight.Add(dataSize);
    UpdateTabletCounters();
}

void TWritesMonitor::OnFinishWrite(const ui64 dataSize, const ui32 writesCount /*= 1*/, const bool onDestroy /*= false*/) {
    AFL_VERIFY(writesCount <= WritesInFlightLocal);
    AFL_VERIFY(dataSize <= WritesSizeInFlightLocal);
    WritesSizeInFlightLocal -= dataSize;
    WritesInFlightLocal -= writesCount;
    AFL_VERIFY(0 <= WritesInFlight.Sub(writesCount));
    AFL_VERIFY(0 <= WritesSizeInFlight.Sub(dataSize));
    if (!onDestroy) {
        UpdateTabletCounters();
    }
}

TString TWritesMonitor::DebugString() const {
    return TStringBuilder() << "{object=write_monitor;count_local=" << WritesInFlightLocal << ";size_local=" << WritesSizeInFlightLocal << ";"
                            << "count_node=" << WritesInFlight.Val() << ";size_node=" << WritesSizeInFlight.Val() << "}";
}

}   // namespace NKikimr::NColumnShard
