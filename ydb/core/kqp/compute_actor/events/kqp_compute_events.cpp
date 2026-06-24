#include "kqp_compute_events.h"

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NKqp {

NInternalImplementation::TEvScanData* NInternalImplementation::TEvScanData::Load(const TEventSerializedData* data) {
    auto pbEv = THolder<TEvRemoteScanData>(TEvRemoteScanData::Load(data));
    auto ev = MakeHolder<TEvScanData>(pbEv->Record.GetScanId(), pbEv->Record.GetGeneration());

    ev->CpuTime = TDuration::MicroSeconds(pbEv->Record.GetCpuTimeUs());
    ev->WaitTime = TDuration::MilliSeconds(pbEv->Record.GetWaitTimeMs());
    ev->PageFault = pbEv->Record.GetPageFault();
    ev->PageFaults = pbEv->Record.GetPageFaults();
    ev->Finished = pbEv->Record.GetFinished();
    ev->RequestedBytesLimitReached = pbEv->Record.GetRequestedBytesLimitReached();
    ev->LastKey = TOwnedCellVec(TSerializedCellVec(pbEv->Record.GetLastKey()).GetCells());
    ev->LastCursorProto = pbEv->Record.GetLastCursor();
    if (pbEv->Record.HasAvailablePacks()) {
        ev->AvailablePacks = pbEv->Record.GetAvailablePacks();
    }

    auto rows = pbEv->Record.GetRows();
    ev->Rows.reserve(rows.size());
    for (const auto& row : rows) {
        ev->Rows.emplace_back(TSerializedCellVec(row).GetCells());
    }

    if (pbEv->Record.HasArrowBatch()) {
        auto batch = pbEv->Record.GetArrowBatch();
        auto schema = NArrow::DeserializeSchema(batch.GetSchema());
        ev->ArrowBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({ NArrow::DeserializeBatch(batch.GetBatch(), schema) }));
    }

    return ev.Release();
}

TEvKqpCompute::TEvScanDataAck* TEvKqpCompute::TEvScanDataAck::Load(const TEventSerializedData* data) {
    auto pbEv = THolder<TEvRemoteScanDataAck>(TEvRemoteScanDataAck::Load(data));
    ui32 maxChunksCount = Max<ui32>();
    if (pbEv->Record.HasMaxChunksCount()) {
        maxChunksCount = pbEv->Record.GetMaxChunksCount();
    }
    return new TEvScanDataAck(pbEv->Record.GetFreeSpace(), pbEv->Record.GetGeneration(), maxChunksCount);
}

}
