#include "tx_internal_scan.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

LWTRACE_USING(YDB_CS_SCAN);

void TTxInternalScan::SendError(const TString& problem, const TString& details, const TActorContext& ctx) const {
    const auto& request = *InternalScanEvent->Get();
    SendScanError(Self->TabletID(), InternalScanEvent->Sender, ScanGen, TStringBuilder() << request.GetPathId(), problem, details, ctx);
}

TSnapshot TTxInternalScan::GetSnapshot() const {
    const auto& request = *InternalScanEvent->Get();
    return Self->TablesManager.ResolveReadSnapshot(request.GetPathId().GetSchemeShardLocalPathId(), request.GetSnapshot());
}

TReadMetadataBase::ESorting TTxInternalScan::GetSorting() const {
    return InternalScanEvent->Get()->GetReverse() ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC;
}

TReadDescription TTxInternalScan::MakeReadDescription(const TSnapshot& snapshot, const TReadMetadataBase::ESorting sorting) const {
    const auto& request = *InternalScanEvent->Get();
    AFL_VERIFY(Self->GetIndexOptional());
    // An internal scan always deduplicates, and always through the trivial reader.
    TReadDescription read(Self->TabletID(), snapshot, sorting, true, true);
    read.SetScanIdentifier(request.TaskIdentifier);
    // the parent write has already subscribed to the lock, so no need to subscribe again
    read.SetLock(request.GetLockId(), std::nullopt, NKikimrDataEvents::OPTIMISTIC,
        request.GetLockId().has_value() ? Self->GetOperationsManager().GetLockOptional(request.GetLockId().value()) : nullptr,
        request.GetReadOnlyConflicts());
    read.ColumnIds = request.GetColumnIds();
    read.SetScanCursor(nullptr);
    if (request.RangesFilter) {
        read.PKRangesFilter = request.RangesFilter;
    }
    TProgramContainer program;
    program.OverrideProcessingColumns(read.ColumnIds);
    read.SetProgram(std::move(program));
    return read;
}

TConclusionStatus TTxInternalScan::InitTableAccessor(TReadDescription& read, const TSnapshot& snapshot) const {
    const auto& request = *InternalScanEvent->Get();
    auto accConclusion = Self->TablesManager.BuildTableMetadataAccessor(
        "internal_request", request.GetPathId().GetInternalPathId(), request.GetPathId().GetSchemeShardLocalPathId(), snapshot);
    if (accConclusion.IsFail()) {
        return TConclusionStatus::Fail(accConclusion.GetErrorMessage());
    }
    read.TableMetadataAccessor = accConclusion.DetachResult();
    return TConclusionStatus::Success();
}

std::unique_ptr<TTxInternalScan::TDiagnosticsEvent> TTxInternalScan::MakeDiagnosticsEvent(const TReadDescription& read) const {
    if (!AppDataVerified().ColumnShardConfig.GetEnableDiagnostics()) {
        return nullptr;
    }
    auto graphOptional = read.GetProgram().GetGraphOptional();
    TString dotGraph = graphOptional ? graphOptional->DebugDOT() : "";
    TString ssaProgram = read.GetProgram().ProtoDebugString();
    auto requestMessage = InternalScanEvent->Get()->ToString();
    auto pkRangesFilter = read.PKRangesFilter->DebugString();
    if (pkRangesFilter.size() > 1024) {
        pkRangesFilter = pkRangesFilter.substr(0, 1024) + "...";
    }
    return std::make_unique<TDiagnosticsEvent>(
        std::move(requestMessage), std::move(dotGraph), std::move(ssaProgram), std::move(pkRangesFilter), false);
}

void TTxInternalScan::StartScanActor(
    const TReadMetadataBase::TConstPtr& readMetadataRange, std::unique_ptr<TDiagnosticsEvent>&& diagnostics, const TActorContext& ctx) const {
    const auto& request = *InternalScanEvent->Get();
    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD_SCAN)) {
        detailedInfo << " read metadata: (" << readMetadataRange->DebugString() << ")";
    }

    const TVersionedIndex* index = Self->HasIndex() ? &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex() : nullptr;
    readMetadataRange->OnBeforeStartReading(*Self);

    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);
    if (diagnostics) {
        diagnostics->RequestId = requestCookie;
        ctx.Send(Self->ScanDiagnosticsActorId, std::move(diagnostics));
    }
    auto orbit = std::make_shared<NLWTrace::TOrbit>();
    LWTRACK(StartScan, *orbit, request.GetPathId().GetInternalPathId().GetRawValue(), Self->TabletID(), request.GetLockId().value_or(0), ScanId);
    auto scanActorId = ctx.Register(new TColumnShardScan(Self->SelfId(), InternalScanEvent->Sender, Self->ScanDiagnosticsActorId,
        Self->GetStoragesManager(), Self->DataAccessorsManager.GetObjectPtrVerified(), Self->ColumnDataManager.GetObjectPtrVerified(),
        TComputeShardingPolicy(), ScanId, request.GetLockId().value_or(0), ScanGen, requestCookie, Self->TabletID(), TDuration::Max(),
        readMetadataRange, NKikimrDataEvents::FORMAT_ARROW, Self->Counters.GetScanCounters(), {}, std::move(orbit)));

    Self->InFlightReadsTracker.AddScanActorId(requestCookie, scanActorId);
    YDB_LOG_DEBUG("",
        {"event", "TTxInternalScan started"},
        {"actorId", scanActorId},
        {"traceDetailed", detailedInfo});
}

bool TTxInternalScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    return true;
}

void TTxInternalScan::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxInternalScan::Complete");
    const auto& request = *InternalScanEvent->Get();
    const TSnapshot snapshot = GetSnapshot();
    YDB_LOG_CREATE_CONTEXT(
        {"tablet", Self->TabletID()},
        {"snapshot", snapshot.DebugString()},
        {"taskId", request.TaskIdentifier});
    const TReadMetadataBase::ESorting sorting = GetSorting();
    const TScannerConstructorContext context(snapshot, 0, sorting);

    TReadDescription read = MakeReadDescription(snapshot, sorting);
    if (auto status = InitTableAccessor(read, snapshot); status.IsFail()) {
        return SendError("cannot build table metadata accessor for request: " + status.GetErrorMessage(),
            AppDataVerified().ColumnShardConfig.GetReaderClassName(), ctx);
    }

    const NTrivial::TIndexScannerConstructor scannerConstructor(context);
    auto metadataConclusion = MakeReadMetadata(Self, Self->Counters.GetScanCounters(), scannerConstructor, read);
    if (metadataConclusion.IsFail()) {
        return SendError("cannot create read metadata", metadataConclusion.GetErrorMessage(), ctx);
    }

    StartScanActor(metadataConclusion.DetachResult(), MakeDiagnosticsEvent(read), ctx);
}

}   // namespace NKikimr::NOlap::NReader
