#include "writes_monitor.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

namespace NKikimr::NColumnShard {

bool TWritesMonitor::OnStartWrite(const ui64 dataSize) {
    if (!NOverload::TOverloadManagerServiceOperator::RequestResources(1, dataSize)) {
        return false;
    }

    ++WritesInFlightLocal;
    WritesSizeInFlightLocal += dataSize;

    UpdateTabletCounters();

    return true;
}

void TWritesMonitor::OnFinishWrite(const ui64 dataSize, const ui32 writesCount /*= 1*/, const bool onDestroy /*= false*/) {
    AFL_VERIFY(writesCount <= WritesInFlightLocal);
    AFL_VERIFY(dataSize <= WritesSizeInFlightLocal);
    WritesSizeInFlightLocal -= dataSize;
    WritesInFlightLocal -= writesCount;
    NOverload::TOverloadManagerServiceOperator::ReleaseResources(writesCount, dataSize);
    if (!onDestroy) {
        UpdateTabletCounters();
    }
}

TString TWritesMonitor::DebugString() const {
    return TStringBuilder() << "{object=write_monitor;count_local=" << WritesInFlightLocal << ";size_local=" << WritesSizeInFlightLocal << "}";
}

}   // namespace NKikimr::NColumnShard
