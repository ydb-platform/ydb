#include "tx_scan.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader {

void TTxScan::SendError(const TString& problem, const TString& details, const TActorContext& ctx) const {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan failed")("problem", problem)("details", details);
    const auto& request = Ev->Get()->Record;
    const TString table = request.GetTablePath();
    const ui32 scanGen = request.GetGeneration();
    const auto scanComputeActor = Ev->Sender;

    auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(scanGen, Self->TabletID());
    ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
    auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
        TStringBuilder() << "Table " << table << " (shard " << Self->TabletID() << ") scan failed, reason: " << problem << "/" << details);
    NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

    ctx.Send(scanComputeActor, ev.Release());
}

bool TTxScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    return true;
}

void TTxScan::Complete(const TActorContext& ctx) {
    TMemoryProfileGuard mpg("TTxScan::Complete");
    auto& request = Ev->Get()->Record;
    auto scanComputeActor = Ev->Sender;
    TSnapshot snapshot = TSnapshot(request.GetSnapshot().GetStep(), request.GetSnapshot().GetTxId());
    if (snapshot.IsZero()) {
        snapshot = Self->GetLastTxSnapshot();
    }
    const TReadMetadataBase::ESorting sorting = [&]() {
        if (request.HasReverse()) {
            return request.GetReverse() ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC;
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
    NConveyorComposite::TCPULimitsConfig cpuLimits;
    cpuLimits.DeserializeFromProto(request).Validate();
    if (scanGen > 1) {
        Self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_SCAN_RESTARTED);
    }
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build() ("tx_id", txId)("scan_id", scanId)("gen", scanGen)(
        "table", table)("snapshot", snapshot)("tablet", Self->TabletID())("timeout", timeout)("cpu_limits", cpuLimits.DebugString());

    TReadMetadataPtr readMetadataRange;
    {
        LOG_S_DEBUG("TTxScan prepare txId: " << txId << " scanId: " << scanId << " at tablet " << Self->TabletID());

        TReadDescription read(Self->TabletID(), snapshot, sorting);
        read.TxId = txId;
        if (request.HasLockTxId()) {
            read.LockId = request.GetLockTxId();
        }

        const auto& schemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromProto(request);
        const auto& internalPathId = Self->TablesManager.ResolveInternalPathId(schemeShardLocalPathId);
        read.PathId = NColumnShard::TUnifiedPathId{ internalPathId ? *internalPathId : TInternalPathId{}, schemeShardLocalPathId };
        read.ReadNothing = !Self->TablesManager.HasTable(read.PathId.InternalPathId);
        read.TableName = table;

        const TString defaultReader = [&]() {
            const TString defGlobal =
                AppDataVerified().ColumnShardConfig.GetReaderClassName() ? AppDataVerified().ColumnShardConfig.GetReaderClassName() : "PLAIN";
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
            auto sysViewPolicy = NSysView::NAbstract::ISysViewPolicy::BuildByPath(read.TableName);
            if (!sysViewPolicy) {
                auto constructor = NReader::IScannerConstructor::TFactory::MakeHolder(
                    request.GetCSScanPolicy() ? request.GetCSScanPolicy() : defaultReader, context);
                if (!constructor) {
                    return std::unique_ptr<IScannerConstructor>();
                }
                return std::unique_ptr<IScannerConstructor>(constructor.Release());
            } else {
                return sysViewPolicy->CreateConstructor(context);
            }
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
        }
        read.ColumnIds.assign(request.GetColumnTags().begin(), request.GetColumnTags().end());
        read.StatsMode = request.GetStatsMode();

        const TVersionedIndex* vIndex = Self->GetIndexOptional() ? &Self->GetIndexOptional()->GetVersionedIndex() : nullptr;
        auto parseResult = scannerConstructor->ParseProgram(vIndex, request, read);
        if (!parseResult) {
            return SendError("cannot parse program", parseResult.GetErrorMessage(), ctx);
        }
        {
            if (request.RangesSize()) {
                auto ydbKey = scannerConstructor->GetPrimaryKeyScheme(Self);
                {
                    auto filterConclusion = NOlap::TPKRangesFilter::BuildFromProto(request, ydbKey);
                    if (filterConclusion.IsFail()) {
                        return SendError("cannot build ranges filter", filterConclusion.GetErrorMessage(), ctx);
                    }
                    read.PKRangesFilter = std::make_shared<NOlap::TPKRangesFilter>(filterConclusion.DetachResult());
                }
            }
            auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
            if (newRange.IsSuccess()) {
                readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
            } else {
                return SendError("cannot build metadata withno ranges", newRange.GetErrorMessage(), ctx);
            }
        }
    }
    AFL_VERIFY(readMetadataRange);
    readMetadataRange->OnBeforeStartReading(*Self);

    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << readMetadataRange->DebugString() << ")"
                     << " req: " << request;
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);

    Self->Counters.GetTabletCounters()->OnScanStarted(Self->InFlightReadsTracker.GetSelectStatsDelta());

    TComputeShardingPolicy shardingPolicy;
    AFL_VERIFY(shardingPolicy.DeserializeFromProto(request.GetComputeShardingPolicy()));

    auto scanActorId = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(),
        Self->DataAccessorsManager.GetObjectPtrVerified(), shardingPolicy, scanId, txId, scanGen, requestCookie, Self->TabletID(), timeout,
        readMetadataRange, dataFormat, Self->Counters.GetScanCounters(), cpuLimits));
    Self->InFlightReadsTracker.AddScanActorId(requestCookie, scanActorId);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan started")("actor_id", scanActorId)("trace_detailed", detailedInfo);
}

}   // namespace NKikimr::NOlap::NReader
