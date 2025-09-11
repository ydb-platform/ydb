#include "writes_monitor.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

NOverload::EResourcesStatus TWritesMonitor::OnStartWrite(const ui64 dataSize) {
    auto status = NOverload::TOverloadManagerServiceOperator::RequestResources(1, dataSize);
    if (status != NOverload::EResourcesStatus::Ok) {
        return status;
    }

    ++WritesInFlightLocal;
    WritesSizeInFlightLocal += dataSize;

    UpdateTabletCounters();

    return status;
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
    return TStringBuilder() << "{object=write_monitor;count_local=" << WritesInFlightLocal 
                            << ";size_local=" << WritesSizeInFlightLocal
                            << ";count_node=" << NOverload::TOverloadManagerServiceOperator::GetShardWritesInFly()
                            << ";size_node=" << NOverload::TOverloadManagerServiceOperator::GetShardWritesSizeInFly() << "}";
}

}   // namespace NKikimr::NColumnShard
