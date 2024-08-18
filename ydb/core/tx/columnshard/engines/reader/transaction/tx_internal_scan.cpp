#include "tx_internal_scan.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>

namespace NKikimr::NOlap::NReader {

void TTxInternalScan::SendError(const TString& problem, const TString& details) const {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan failed")("problem", problem)("details", details);
    auto& request = *InternalScanEvent->Get();
    auto scanComputeActor = InternalScanEvent->Sender;

    auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(ScanGen, Self->TabletID());
    ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
    auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
        TStringBuilder() << "Table " << request.GetPathId() << " (shard " << Self->TabletID() << ") scan failed, reason: " << problem << "/"
                         << details);
    NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

    ctx.Send(scanComputeActor, ev.Release());
}

bool TTxInternalScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    return true;
}

void TTxInternalScan::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxInternalScan::Complete");

    auto& request = *InternalScanEvent->Get();
    auto scanComputeActor = InternalScanEvent->Sender;
    const TSnapshot snapshot = request.ReadToSnapshot.value_or(NOlap::TSnapshot(Self->LastPlannedStep, Self->LastPlannedTxId));
    const NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build()("tablet", Self->TabletID())("snapshot", snapshot.DebugString());
    TReadMetadataPtr readMetadataRange;
    {
        TReadDescription read(snapshot, request.GetReverse());
        read.PathId = request.GetPathId();
        read.ReadNothing = !Self->TablesManager.HasTable(read.PathId);
        std::unique_ptr<IScannerConstructor> scannerConstructor(
            new NPlain::TIndexScannerConstructor(snapshot, request.GetItemsLimit(), request.GetReverse()));
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
                return SendError("cannot create read metadata", newRange.GetErrorMessage());
            }
            readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
        }
    }

    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << *readMetadataRange << ")";
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);
    auto scanActor = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(), TComputeShardingPolicy(),
        ScanId, TxId, ScanGen, requestCookie, Self->TabletID(), TDuration::Max(), readMetadataRange, NKikimrDataEvents::FORMAT_ARROW,
        Self->Counters.GetScanCounters()));

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxInternalScan started")("actor_id", scanActor)("trace_detailed", detailedInfo);
}

}   // namespace NKikimr::NOlap::NReader
