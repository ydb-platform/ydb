#include "tx_internal_scan.h"
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>

namespace NKikimr::NOlap::NReader {

bool TTxInternalScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    TMemoryProfileGuard mpg("TTxInternalScan::Execute");
    auto& request = *InternalScanEvent->Get();
    const TSnapshot snapshot = request.ReadToSnapshot.value_or(NOlap::TSnapshot(Self->LastPlannedStep, Self->LastPlannedTxId));

    TReadDescription read(snapshot, request.GetReverse());
    read.PathId = request.GetPathId();
    read.ReadNothing = !Self->TablesManager.HasTable(read.PathId);
    std::unique_ptr<IScannerConstructor> scannerConstructor(new NPlain::TIndexScannerConstructor(snapshot, request.GetItemsLimit(), request.GetReverse()));
    read.ColumnIds = request.GetColumnIds();
    read.ColumnNames = request.GetColumnNames();
    if (request.RangesFilter) {
        read.PKRangesFilter = std::move(*request.RangesFilter);
    }

    const TVersionedIndex* vIndex = Self->GetIndexOptional() ? &Self->GetIndexOptional()->GetVersionedIndex() : nullptr;
    AFL_VERIFY(vIndex);
    {
        TProgramContainer pContainer;
        pContainer.OverrideProcessingColumns(read.ColumnNames);
        read.SetProgram(std::move(pContainer));
    }

    {
        auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
        if (!newRange) {
            ErrorDescription = newRange.GetErrorMessage();
            ReadMetadataRange = nullptr;
            return true;
        }
        ReadMetadataRange = newRange.DetachResult();
    }
    AFL_VERIFY(ReadMetadataRange);
    return true;
}

void TTxInternalScan::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxInternalScan::Complete");
    auto& request = *InternalScanEvent->Get();
    auto scanComputeActor = InternalScanEvent->Sender;
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("tablet", Self->TabletID());

    if (!ReadMetadataRange) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan failed")("reason", "no metadata")("error", ErrorDescription);

        auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(ScanGen, Self->TabletID());
        ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
            << "Table " << request.GetPathId() << " (shard " << Self->TabletID() << ") scan failed, reason: " << ErrorDescription ? ErrorDescription : "no metadata ranges");
        NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

        ctx.Send(scanComputeActor, ev.Release());
        return;
    }
    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << *ReadMetadataRange << ")";
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    const TConclusion<ui64> requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(ReadMetadataRange, index);
    if (!requestCookie) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan failed")("reason", requestCookie.GetErrorMessage())("trace_details", detailedInfo);
        auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(ScanGen, Self->TabletID());

        ev->Record.SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, TStringBuilder()
            << "Table " << request.GetPathId() << " (shard " << Self->TabletID() << ") scan failed, reason: " << requestCookie.GetErrorMessage());
        NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());
        Self->Stats.GetScanCounters().OnScanFinished(NColumnShard::TScanCounters::EStatusFinish::CannotAddInFlight, TDuration::Zero());
        ctx.Send(scanComputeActor, ev.Release());
        return;
    }
    auto scanActor = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(),
        TComputeShardingPolicy(), ScanId, TxId, ScanGen, *requestCookie, Self->TabletID(), TDuration::Max(), ReadMetadataRange,
        NKikimrDataEvents::FORMAT_ARROW, Self->Stats.GetScanCounters()));

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxInternalScan started")("actor_id", scanActor)("trace_detailed", detailedInfo);
}

}
