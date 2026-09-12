#include "tx_scan.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/common/scan_memory_limiter.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader {

LWTRACE_USING(YDB_CS_SCAN);

void TTxScan::SendError(const TString& problem, const TString& details, const TActorContext& ctx) const {
    const auto& request = Ev->Get()->Record;
    SendScanError(Self->TabletID(), Ev->Sender, request.GetGeneration(), request.GetTablePath(), problem, details, ctx);
}

TSnapshot TTxScan::GetSnapshot(const NColumnShard::TSchemeShardLocalPathId& ssPathId) const {
    const auto& request = Ev->Get()->Record;
    TSnapshot snapshot(request.GetSnapshot().GetStep(), request.GetSnapshot().GetTxId());
    if (snapshot.IsZero()) {
        snapshot = Self->GetLastTxSnapshot();
    }
<<<<<<< HEAD
    const NColumnShard::TSchemeShardLocalPathId ssPathId = NColumnShard::TSchemeShardLocalPathId::FromProto(request);
    snapshot = Self->TablesManager.ResolveReadSnapshot(ssPathId, snapshot);
    const bool deduplicationEnabled = AppDataVerified().ColumnShardConfig.GetDeduplicationEnabled();
    const TReadMetadataBase::ESorting sorting = [&]() {
        if (request.HasReverse()) {
            return request.GetReverse() ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC;
        } else if (deduplicationEnabled) {
            return TReadMetadataBase::ESorting::ASC;
        } else {
            return TReadMetadataBase::ESorting::NONE;
        }
    }();
    TScannerConstructorContext context(snapshot, request.HasItemsLimit() ? request.GetItemsLimit() : 0, sorting);
    const auto scanId = request.GetScanId();
    const ui64 txId = request.GetTxId();
    const ui32 scanGen = request.GetGeneration();
    const TString table = request.GetTablePath();
    const auto dataFormat = request.GetDataFormat();
    const TDuration timeout = TDuration::MilliSeconds(request.GetTimeoutMs());
=======
    return Self->TablesManager.ResolveReadSnapshot(ssPathId, snapshot);
}

ERequestSorting TTxScan::GetRequestSorting() const {
    const auto& request = Ev->Get()->Record;
    if (!request.HasReverse()) {
        return ERequestSorting::NONE;
    }
    return request.GetReverse() ? ERequestSorting::DESC : ERequestSorting::ASC;
}

bool TTxScan::GetDeduplicationEnabled(const TSnapshot& snapshot) const {
    const bool defGlobal = AppDataVerified().ColumnShardConfig.GetDeduplicationEnabled();
    if (!Self->HasIndex()) {
        return defGlobal;
    }
    const auto& vIndex = Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    return vIndex.GetSchemaVerified(snapshot)->GetIndexInfo().GetDeduplicationEnabled().value_or(defGlobal);
}

TString TTxScan::GetReaderName() const {
    const auto& request = Ev->Get()->Record;
    const TString defGlobal =
        AppDataVerified().ColumnShardConfig.GetReaderClassName() ? AppDataVerified().ColumnShardConfig.GetReaderClassName() : "TRIVIAL";
    const std::optional<TString> schemaReader =
        Self->HasIndex() ? Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetScanReaderPolicyName()
                         : std::nullopt;
    const TString policy = request.GetCSScanPolicy() ? request.GetCSScanPolicy() : schemaReader.value_or(defGlobal);
    return policy == "EXPORT" ? TString("PLAIN") : policy;
}

NConveyorComposite::TCPULimitsConfig TTxScan::GetCpuLimits() const {
>>>>>>> 1cf2a32d756 (Sort columshards out (#51902))
    NConveyorComposite::TCPULimitsConfig cpuLimits;
    cpuLimits.DeserializeFromProto(Ev->Get()->Record).Validate();
    return cpuLimits;
}

const TVersionedPresetSchemas& TTxScan::GetPresetSchemas() const {
    static TVersionedPresetSchemas defaultSchemas(
        0, Self->GetStoragesManager(), Self->GetTablesManager().GetSchemaObjectsCache().GetObjectPtrVerified());
    return Self->GetIndexOptional() ? Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedSchemas() : defaultSchemas;
}

TReadDescription TTxScan::MakeReadDescription(const TSnapshot& snapshot, const ERequestSorting requestSorting,
    const std::shared_ptr<NLWTrace::TOrbit>& orbit, const EReaderClass readerClass,
    const std::shared_ptr<ITableMetadataAccessor>& tableMetadataAccessor) const {
    const auto& request = Ev->Get()->Record;
    TReadDescription read(Self->TabletID(), snapshot, requestSorting, GetDeduplicationEnabled(snapshot), readerClass, tableMetadataAccessor,
        GetCursorSourcesSorting());
    read.GroupedMemoryLimiterOperator =
        request.GetCSScanPolicy() == "EXPORT" ? EScanGroupedMemoryLimiterOperator::Deduplication : EScanGroupedMemoryLimiterOperator::Scan;
    read.Orbit = orbit;
    read.TxId = request.GetTxId();
    read.ScanId = request.GetScanId();
    read.SetLock(request.HasLockTxId() ? std::make_optional(request.GetLockTxId()) : std::nullopt,
        request.HasLockNodeId() ? std::make_optional(request.GetLockNodeId()) : std::nullopt,
        request.HasLockMode() ? std::make_optional(request.GetLockMode()) : std::nullopt,
        request.HasLockTxId() ? Self->GetOperationsManager().GetLockOptional(request.GetLockTxId()) : nullptr, false);
    read.ColumnIds.assign(request.GetColumnTags().begin(), request.GetColumnTags().end());
    read.StatsMode = request.GetStatsMode();
    return read;
}

TConclusion<std::shared_ptr<ITableMetadataAccessor>> TTxScan::MakeTableAccessor(
    const NColumnShard::TSchemeShardLocalPathId& ssPathId, const TSnapshot& snapshot) const {
    const auto& request = Ev->Get()->Record;
    return Self->TablesManager.BuildTableMetadataAccessor(request.GetTablePath() ? request.GetTablePath() : "undefined", ssPathId, snapshot);
}

// The order the scan being resumed used. Read straight off the request: the field sits at the top of
// the cursor message
std::optional<ESourcesSorting> TTxScan::GetCursorSourcesSorting() const {
    const auto& cursor = Ev->Get()->Record.GetScanCursor();
    if (!cursor.HasSourcesSorting()) {
        return std::nullopt;
    }
    return SourcesSortingFromProto(cursor.GetSourcesSorting());
}

ui64 TTxScan::OnScanStartedForPath(const TReadDescription& read, NLWTrace::TOrbit& orbit) const {
    const auto& request = Ev->Get()->Record;
    ui64 rawPathId = 0;
<<<<<<< HEAD
    TReadMetadataPtr readMetadataRange;
    std::unique_ptr<NColumnShard::TEvPrivate::TEvReportScanDiagnostics> scanDiagnosticsEvent;
    {
        LOG_S_DEBUG("TTxScan prepare txId: " << txId << " scanId: " << scanId << " at tablet " << Self->TabletID());
        TReadDescription read(Self->TabletID(), snapshot, sorting);
        read.SetFakeSort(!request.HasReverse() && deduplicationEnabled);
        read.DeduplicationPolicy = deduplicationEnabled ? EDeduplicationPolicy::PREVENT_DUPLICATES : EDeduplicationPolicy::ALLOW_DUPLICATES;
        read.GroupedMemoryLimiterOperator =
            request.GetCSScanPolicy() == "EXPORT" ? EScanGroupedMemoryLimiterOperator::Deduplication : EScanGroupedMemoryLimiterOperator::Scan;
        read.Orbit = orbit;
        read.TxId = txId;
        read.ScanId = scanId;
        read.SetLock(request.HasLockTxId() ? std::make_optional(request.GetLockTxId()) : std::nullopt,
            request.HasLockNodeId() ? std::make_optional(request.GetLockNodeId()) : std::nullopt,
            request.HasLockMode() ? std::make_optional(request.GetLockMode()) : std::nullopt,
            request.HasLockTxId() ? Self->GetOperationsManager().GetLockOptional(request.GetLockTxId()) : nullptr, false);

        {
            auto accConclusion =
                Self->TablesManager.BuildTableMetadataAccessor(request.GetTablePath() ? request.GetTablePath() : "undefined", ssPathId);
            if (accConclusion.IsFail()) {
                return SendError("cannot build table metadata accessor for request: " + accConclusion.GetErrorMessage(),
                    AppDataVerified().ColumnShardConfig.GetReaderClassName(), ctx);
            } else {
                read.TableMetadataAccessor = accConclusion.DetachResult();
            }
            if (auto pathId = read.TableMetadataAccessor->GetPathId()) {
                auto internalPathId = pathId->GetInternalPathIdOptional().value_or(TInternalPathId::FromRawValue(0));
                rawPathId = internalPathId.GetRawValue();
                Self->Counters.GetColumnTablesCounters()->GetPathIdCounter(internalPathId)->OnReadEvent();
            }
            LWTRACK(StartScan, *orbit, rawPathId, Self->TabletID(), request.GetTxId(), request.GetScanId());
        }

        const TString defaultReader = [&]() {
            const TString defGlobal =
                AppDataVerified().ColumnShardConfig.GetReaderClassName() ? AppDataVerified().ColumnShardConfig.GetReaderClassName() : "TRIVIAL";
            if (Self->HasIndex()) {
                return Self->GetIndexAs<TColumnEngineForLogs>()
                    .GetVersionedIndex()
                    .GetLastSchema()
                    ->GetIndexInfo()
                    .GetScanReaderPolicyName()
                    .value_or(defGlobal);
            } else {
                return defGlobal;
            }
        }();
        std::unique_ptr<IScannerConstructor> scannerConstructor = [&]() {
            const TString scanType = [&]() {
                const TString policy = request.GetCSScanPolicy() ? request.GetCSScanPolicy() : defaultReader;
                if (policy == "EXPORT") {
                    return TString("PLAIN");
                }
                return policy;
            }();
            auto constructor =
                NReader::IScannerConstructor::TFactory::MakeHolder(read.TableMetadataAccessor->GetOverridenScanType(scanType), context);
            if (!constructor) {
                return std::unique_ptr<IScannerConstructor>();
            }
            return std::unique_ptr<IScannerConstructor>(constructor.Release());
        }();
        if (!scannerConstructor) {
            return SendError("cannot build scanner", AppDataVerified().ColumnShardConfig.GetReaderClassName(), ctx);
        }
        if (request.HasScanCursor()) {
            auto cursorConclusion = scannerConstructor->BuildCursorFromProto(request.GetScanCursor());
            if (cursorConclusion.IsFail()) {
                return SendError("cannot build scanner cursor", cursorConclusion.GetErrorMessage(), ctx);
            }
            read.SetScanCursor(cursorConclusion.DetachResult());
        } else {
            read.SetScanCursor(nullptr);
        }
        read.ColumnIds.assign(request.GetColumnTags().begin(), request.GetColumnTags().end());
        read.StatsMode = request.GetStatsMode();

        static TVersionedPresetSchemas defaultSchemas(
            0, Self->GetStoragesManager(), Self->GetTablesManager().GetSchemaObjectsCache().GetObjectPtrVerified());
        const TVersionedPresetSchemas* vIndex =
            Self->GetIndexOptional() ? &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedSchemas() : &defaultSchemas;
        auto parseResult = scannerConstructor->ParseProgram(TProgramParsingContext(*vIndex), request, read);
        if (!parseResult) {
            return SendError("cannot parse program", parseResult.GetErrorMessage(), ctx);
        }
        {
            if (request.RangesSize()) {
                // TODO: deduplicate
                auto ydbKey = read.TableMetadataAccessor->GetPrimaryKeyInfo(*vIndex);
                auto arrowKey = read.TableMetadataAccessor->GetPrimaryKeyScheme(*vIndex);
                auto filterConclusion = NOlap::TPKRangesFilter::BuildFromProto(request, ydbKey, arrowKey);
                if (filterConclusion.IsFail()) {
                    return SendError("cannot build ranges filter", filterConclusion.GetErrorMessage(), ctx);
                }
                read.PKRangesFilter = std::make_shared<NOlap::TPKRangesFilter>(filterConclusion.DetachResult());
            }

            TInstant buildReadMetadataStart = TAppData::TimeProvider->Now();
            auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
            if (newRange.IsSuccess()) {
                Self->Counters.GetScanCounters().OnReadMetadata((TAppData::TimeProvider->Now() - buildReadMetadataStart));
                if (!request.HasReverse() && deduplicationEnabled) {
                    (*newRange)->SetFakeSort(true);
                }
                readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
            } else {
                return SendError("cannot build metadata", newRange.GetErrorMessage(), ctx);
            }
        }

        if (AppDataVerified().ColumnShardConfig.GetEnableDiagnostics()) {
            auto graphOptional = read.GetProgram().GetGraphOptional();
            TString dotGraph = graphOptional ? graphOptional->DebugDOT() : "";
            TString ssaProgram = read.GetProgram().ProtoDebugString();
            auto requestMessage = request.DebugString();
            TString pkRangesFilter;
            if (!read.PKRangesFilter || read.PKRangesFilter->IsEmpty()) {
                // Do nothing
            } else if (read.PKRangesFilter->Size() <= 3) {
                pkRangesFilter = read.PKRangesFilter->DebugString();
            } else {
                pkRangesFilter = TStringBuilder() << "<" << read.PKRangesFilter->Size() << " ranges>";
            }
            scanDiagnosticsEvent = std::make_unique<NColumnShard::TEvPrivate::TEvReportScanDiagnostics>(
                std::move(requestMessage), std::move(dotGraph), std::move(ssaProgram), std::move(pkRangesFilter), true);
        }
=======
    if (auto pathId = read.GetTableMetadataAccessor()->GetPathId()) {
        auto internalPathId = pathId->GetInternalPathIdOptional().value_or(TInternalPathId::FromRawValue(0));
        rawPathId = internalPathId.GetRawValue();
        Self->Counters.GetColumnTablesCounters()->GetPathIdCounter(internalPathId)->OnReadEvent();
>>>>>>> 1cf2a32d756 (Sort columshards out (#51902))
    }
    LWTRACK(StartScan, orbit, rawPathId, Self->TabletID(), request.GetTxId(), request.GetScanId());
    return rawPathId;
}

TConclusion<std::unique_ptr<IScannerConstructor>> TTxScan::MakeScannerConstructor(
    const TScannerConstructorContext& context, const TString& readerName) const {
    auto constructor = IScannerConstructor::TFactory::MakeHolder(readerName, context);
    if (!constructor) {
        return TConclusionStatus::Fail(AppDataVerified().ColumnShardConfig.GetReaderClassName());
    }
    return std::unique_ptr<IScannerConstructor>(constructor.Release());
}

TConclusionStatus TTxScan::InitScanCursor(TReadDescription& read, const IScannerConstructor& scannerConstructor) const {
    const auto& request = Ev->Get()->Record;
    if (!request.HasScanCursor()) {
        read.SetScanCursor(nullptr);
        return TConclusionStatus::Success();
    }
    auto cursorConclusion = scannerConstructor.BuildCursorFromProto(request.GetScanCursor(), read.GetSourcesSorting());
    if (cursorConclusion.IsFail()) {
        return TConclusionStatus::Fail(cursorConclusion.GetErrorMessage());
    }
    read.SetScanCursor(cursorConclusion.DetachResult());
    return TConclusionStatus::Success();
}

TConclusionStatus TTxScan::InitProgram(TReadDescription& read, const IScannerConstructor& scannerConstructor) const {
    return scannerConstructor.ParseProgram(TProgramParsingContext(GetPresetSchemas()), Ev->Get()->Record, read);
}

TConclusionStatus TTxScan::InitPKRangesFilter(TReadDescription& read) const {
    const auto& request = Ev->Get()->Record;
    if (!request.RangesSize()) {
        return TConclusionStatus::Success();
    }
    // TODO: deduplicate
    const TVersionedPresetSchemas& schemas = GetPresetSchemas();
    auto ydbKey = read.GetTableMetadataAccessor()->GetPrimaryKeyInfo(schemas);
    auto arrowKey = read.GetTableMetadataAccessor()->GetPrimaryKeyScheme(schemas);
    auto filterConclusion = NOlap::TPKRangesFilter::BuildFromProto(request, ydbKey, arrowKey);
    if (filterConclusion.IsFail()) {
        return TConclusionStatus::Fail(filterConclusion.GetErrorMessage());
    }
    read.PKRangesFilter = std::make_shared<NOlap::TPKRangesFilter>(filterConclusion.DetachResult());
    return TConclusionStatus::Success();
}

std::unique_ptr<TTxScan::TDiagnosticsEvent> TTxScan::MakeDiagnosticsEvent(const TReadDescription& read) const {
    if (!AppDataVerified().ColumnShardConfig.GetEnableDiagnostics()) {
        return nullptr;
    }
    auto graphOptional = read.GetProgram().GetGraphOptional();
    TString dotGraph = graphOptional ? graphOptional->DebugDOT() : "";
    TString ssaProgram = read.GetProgram().ProtoDebugString();
    auto requestMessage = Ev->Get()->Record.DebugString();
    TString pkRangesFilter;
    if (!read.PKRangesFilter || read.PKRangesFilter->IsEmpty()) {
        // Do nothing
    } else if (read.PKRangesFilter->Size() <= 3) {
        pkRangesFilter = read.PKRangesFilter->DebugString();
    } else {
        pkRangesFilter = TStringBuilder() << "<" << read.PKRangesFilter->Size() << " ranges>";
    }
    return std::make_unique<TDiagnosticsEvent>(
        std::move(requestMessage), std::move(dotGraph), std::move(ssaProgram), std::move(pkRangesFilter), true);
}

void TTxScan::StartScanActor(const TReadMetadataBase::TConstPtr& readMetadataRange, const ui64 rawPathId,
    std::shared_ptr<NLWTrace::TOrbit>&& orbit, std::unique_ptr<TDiagnosticsEvent>&& diagnostics,
    const NConveyorComposite::TCPULimitsConfig& cpuLimits, const TActorContext& ctx) const {
    const auto& request = Ev->Get()->Record;
    readMetadataRange->OnBeforeStartReading(*Self);

    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << readMetadataRange->DebugString() << ")"
                     << " req: " << request;
    }

    const TVersionedIndex* index = Self->HasIndex() ? &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex() : nullptr;
    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);
    Self->Counters.GetTabletCounters()->OnScanStarted(Self->InFlightReadsTracker.GetSelectStatsDelta());

    TComputeShardingPolicy shardingPolicy;
    AFL_VERIFY(shardingPolicy.DeserializeFromProto(request.GetComputeShardingPolicy()));

    if (diagnostics) {
        diagnostics->RequestId = requestCookie;
        ctx.Send(Self->ScanDiagnosticsActorId, std::move(diagnostics));
    }
<<<<<<< HEAD
    auto scanActorId = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->ScanDiagnosticsActorId,
        Self->GetStoragesManager(), Self->DataAccessorsManager.GetObjectPtrVerified(), Self->ColumnDataManager.GetObjectPtrVerified(),
        shardingPolicy, scanId, txId, scanGen, requestCookie, Self->TabletID(), timeout, readMetadataRange, dataFormat,
        Self->Counters.GetScanCounters(), cpuLimits, std::move(orbit), rawPathId));
=======
    const ui32 scanPoolId = request.GetUseBatchPool() ? AppDataVerified().BatchPoolId : Max<ui32>();
    auto scanActorId =
        ctx.Register(new TColumnShardScan(Self->SelfId(), Ev->Sender, Self->ScanDiagnosticsActorId, Self->GetStoragesManager(),
                         Self->DataAccessorsManager.GetObjectPtrVerified(), Self->ColumnDataManager.GetObjectPtrVerified(), shardingPolicy,
                         request.GetScanId(), request.GetTxId(), request.GetGeneration(), requestCookie, Self->TabletID(),
                         TDuration::MilliSeconds(request.GetTimeoutMs()), readMetadataRange, request.GetDataFormat(),
                         Self->Counters.GetScanCounters(), cpuLimits, std::move(orbit), rawPathId), TMailboxType::HTSwap, scanPoolId);
>>>>>>> 1cf2a32d756 (Sort columshards out (#51902))
    Self->InFlightReadsTracker.AddScanActorId(requestCookie, scanActorId);

    YDB_LOG_DEBUG("",
        {"event", "TTxScan started"},
        {"actorId", scanActorId},
        {"traceDetailed", detailedInfo});
}

bool TTxScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    return true;
}

void TTxScan::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxScan::Complete");
    const auto& request = Ev->Get()->Record;
    auto orbit = std::make_shared<NLWTrace::TOrbit>();
    const NColumnShard::TSchemeShardLocalPathId ssPathId = NColumnShard::TSchemeShardLocalPathId::FromProto(request);
    const TSnapshot snapshot = GetSnapshot(ssPathId);
    const ERequestSorting requestSorting = GetRequestSorting();
    const TScannerConstructorContext context(snapshot, request.HasItemsLimit() ? request.GetItemsLimit() : 0);
    const NConveyorComposite::TCPULimitsConfig cpuLimits = GetCpuLimits();
    if (request.GetGeneration() > 1) {
        Self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_SCAN_RESTARTED);
    }
    YDB_LOG_CREATE_CONTEXT(
        {"txId", request.GetTxId()},
        {"scanId", request.GetScanId()},
        {"gen", request.GetGeneration()},
        {"table", request.GetTablePath()},
        {"snapshot", snapshot},
        {"tablet", Self->TabletID()},
        {"timeout", TDuration::MilliSeconds(request.GetTimeoutMs())},
        {"cpuLimits", cpuLimits.DebugString()});
    LOG_S_DEBUG("TTxScan prepare txId: " << request.GetTxId() << " scanId: " << request.GetScanId() << " at tablet " << Self->TabletID());

    auto accessorConclusion = MakeTableAccessor(ssPathId, snapshot);
    if (accessorConclusion.IsFail()) {
        return SendError("cannot build table metadata accessor for request: " + accessorConclusion.GetErrorMessage(),
            AppDataVerified().ColumnShardConfig.GetReaderClassName(), ctx);
    }
    const std::shared_ptr<ITableMetadataAccessor> tableMetadataAccessor = accessorConclusion.DetachResult();
    // A table may insist on a reader of its own, so the scanner comes first: its class shapes the read.
    auto constructorConclusion = MakeScannerConstructor(context, tableMetadataAccessor->GetOverridenScanType(GetReaderName()));
    if (constructorConclusion.IsFail()) {
        return SendError("cannot build scanner", constructorConclusion.GetErrorMessage(), ctx);
    }
    const std::unique_ptr<IScannerConstructor> scannerConstructor = constructorConclusion.DetachResult();

    TReadDescription read = MakeReadDescription(snapshot, requestSorting, orbit, scannerConstructor->GetReaderClass(), tableMetadataAccessor);
    const ui64 rawPathId = OnScanStartedForPath(read, *orbit);

    if (auto status = InitScanCursor(read, *scannerConstructor); status.IsFail()) {
        return SendError("cannot build scanner cursor", status.GetErrorMessage(), ctx);
    }
    if (auto status = InitProgram(read, *scannerConstructor); status.IsFail()) {
        return SendError("cannot parse program", status.GetErrorMessage(), ctx);
    }
    if (auto status = InitPKRangesFilter(read); status.IsFail()) {
        return SendError("cannot build ranges filter", status.GetErrorMessage(), ctx);
    }

    auto metadataConclusion = MakeReadMetadata(Self, Self->Counters.GetScanCounters(), *scannerConstructor, read);
    if (metadataConclusion.IsFail()) {
        return SendError("cannot build metadata", metadataConclusion.GetErrorMessage(), ctx);
    }

    StartScanActor(metadataConclusion.DetachResult(), rawPathId, std::move(orbit), MakeDiagnosticsEvent(read), cpuLimits, ctx);
}

}   // namespace NKikimr::NOlap::NReader
