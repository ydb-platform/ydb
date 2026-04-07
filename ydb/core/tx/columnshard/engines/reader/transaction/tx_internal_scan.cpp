#include "tx_internal_scan.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader {

void TTxInternalScan::SendError(const TString& problem, const TString& details, const TActorContext& ctx) const {
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
    const TSnapshot snapshot = request.GetSnapshot();
    const NActors::TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build()("tablet", Self->TabletID())("snapshot", snapshot.DebugString())("task_id", request.TaskIdentifier);
    TReadMetadataPtr readMetadataRange;
    const TReadMetadataBase::ESorting sorting = [&]() {
        return request.GetReverse() ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC;
    }();

    TScannerConstructorContext context(snapshot, 0, sorting);
    std::unique_ptr<NColumnShard::TEvPrivate::TEvReportScanDiagnostics> scanDiagnosticsEvent;
    {
        TReadDescription read(Self->TabletID(), snapshot, sorting);
        read.SetScanIdentifier(request.TaskIdentifier);
        {
            auto accConclusion = Self->TablesManager.BuildTableMetadataAccessor("internal_request", request.GetPathId().GetInternalPathId(), request.GetPathId().GetSchemeShardLocalPathId());
            if (accConclusion.IsFail()) {
                return SendError("cannot build table metadata accessor for request: " + accConclusion.GetErrorMessage(),
                    AppDataVerified().ColumnShardConfig.GetReaderClassName(), ctx);
            } else {
                read.TableMetadataAccessor = accConclusion.DetachResult();
            }
        }
        // the parent write has already subscribed to the lock, so no need to subscribe again
        auto lockNodeId = std::nullopt;
        read.SetLock(
            request.GetLockId(),
            lockNodeId,
            NKikimrDataEvents::OPTIMISTIC,
            request.GetLockId().has_value() ? Self->GetOperationsManager().GetLockOptional(request.GetLockId().value()) : nullptr,
            request.GetReadOnlyConflicts()
        );
        read.DeduplicationPolicy = EDeduplicationPolicy::PREVENT_DUPLICATES;
        std::unique_ptr<IScannerConstructor> scannerConstructor(new NSimple::TIndexScannerConstructor(context));
        read.ColumnIds = request.GetColumnIds();
        read.SetScanCursor(nullptr);
        if (request.RangesFilter) {
            read.PKRangesFilter = request.RangesFilter;
        }

        const TVersionedIndex* vIndex = Self->GetIndexOptional() ? &Self->GetIndexOptional()->GetVersionedIndex() : nullptr;
        AFL_VERIFY(vIndex);
        {
            TProgramContainer pContainer;
            pContainer.OverrideProcessingColumns(read.ColumnIds);
            read.SetProgram(std::move(pContainer));
        }

        {
            TInstant buildReadMetadataStart = TAppData::TimeProvider->Now();
            auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
            if (!newRange) {
                return SendError("cannot create read metadata", newRange.GetErrorMessage(), ctx);
            }
            Self->Counters.GetScanCounters().OnReadMetadata((TAppData::TimeProvider->Now() - buildReadMetadataStart));
            readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
        }

        if (AppDataVerified().ColumnShardConfig.GetEnableDiagnostics()) {
            auto graphOptional = read.GetProgram().GetGraphOptional();
            TString dotGraph = graphOptional ? graphOptional->DebugDOT() : "";
            TString ssaProgram = read.GetProgram().ProtoDebugString();
            auto requestMessage = request.ToString();
            auto pkRangesFilter = read.PKRangesFilter->DebugString();
            if (pkRangesFilter.size() > 1024) {
                pkRangesFilter = pkRangesFilter.substr(0, 1024) + "...";
            }
            scanDiagnosticsEvent = std::make_unique<NColumnShard::TEvPrivate::TEvReportScanDiagnostics>(std::move(requestMessage), std::move(dotGraph), std::move(ssaProgram), std::move(pkRangesFilter), false);
        }
    }
    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD_SCAN)) {
        detailedInfo << " read metadata: (" << readMetadataRange->DebugString() << ")";
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    readMetadataRange->OnBeforeStartReading(*Self);

    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);
    if (AppDataVerified().ColumnShardConfig.GetEnableDiagnostics()) {
        scanDiagnosticsEvent->RequestId = requestCookie;
        ctx.Send(Self->ScanDiagnosticsActorId, std::move(scanDiagnosticsEvent));
    }
    auto scanActorId = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->ScanDiagnosticsActorId, Self->GetStoragesManager(),
        Self->DataAccessorsManager.GetObjectPtrVerified(), Self->ColumnDataManager.GetObjectPtrVerified(), TComputeShardingPolicy(), ScanId, request.GetLockId().value_or(0), ScanGen, requestCookie,
        Self->TabletID(), TDuration::Max(), readMetadataRange, NKikimrDataEvents::FORMAT_ARROW, Self->Counters.GetScanCounters(), {}));

    Self->InFlightReadsTracker.AddScanActorId(requestCookie, scanActorId);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxInternalScan started")("actor_id", scanActorId)("trace_detailed", detailedInfo);
}

}   // namespace NKikimr::NOlap::NReader
