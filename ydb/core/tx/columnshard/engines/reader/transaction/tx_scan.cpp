#include "tx_scan.h"
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>

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
    const TSnapshot snapshot(request.GetSnapshot().GetStep(), request.GetSnapshot().GetTxId());
    const auto scanId = request.GetScanId();
    const ui64 txId = request.GetTxId();
    const ui32 scanGen = request.GetGeneration();
    const TString table = request.GetTablePath();
    const auto dataFormat = request.GetDataFormat();
    const TDuration timeout = TDuration::MilliSeconds(request.GetTimeoutMs());
    if (scanGen > 1) {
        Self->Counters.GetTabletCounters()->IncCounter(NColumnShard::COUNTER_SCAN_RESTARTED);
    }
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()
        ("tx_id", txId)("scan_id", scanId)("gen", scanGen)("table", table)("snapshot", snapshot)("tablet", Self->TabletID())("timeout", timeout);

    TReadMetadataPtr readMetadataRange;
    {

        LOG_S_DEBUG("TTxScan prepare txId: " << txId << " scanId: " << scanId << " at tablet " << Self->TabletID());

        TReadDescription read(snapshot, request.GetReverse());
        read.TxId = txId;
        read.PathId = request.GetLocalPathId();
        read.ReadNothing = !Self->TablesManager.HasTable(read.PathId);
        read.TableName = table;
        bool isIndex = false;
        std::unique_ptr<IScannerConstructor> scannerConstructor = [&]() {
            const ui64 itemsLimit = request.HasItemsLimit() ? request.GetItemsLimit() : 0;
            auto sysViewPolicy = NSysView::NAbstract::ISysViewPolicy::BuildByPath(read.TableName);
            isIndex = !sysViewPolicy;
            if (!sysViewPolicy) {
                return std::unique_ptr<IScannerConstructor>(new NPlain::TIndexScannerConstructor(snapshot, itemsLimit, request.GetReverse()));
            } else {
                return sysViewPolicy->CreateConstructor(snapshot, itemsLimit, request.GetReverse());
            }
        }();
        read.ColumnIds.assign(request.GetColumnTags().begin(), request.GetColumnTags().end());
        read.StatsMode = request.GetStatsMode();

        const TVersionedIndex* vIndex = Self->GetIndexOptional() ? &Self->GetIndexOptional()->GetVersionedIndex() : nullptr;
        auto parseResult = scannerConstructor->ParseProgram(vIndex, request, read);
        if (!parseResult) {
            return SendError("cannot parse program", parseResult.GetErrorMessage(), ctx);
        }

        if (!request.RangesSize()) {
            auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
            if (newRange.IsSuccess()) {
                readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
            } else {
                return SendError("cannot build metadata withno ranges", newRange.GetErrorMessage(), ctx);
            }
        } else {
            auto ydbKey = scannerConstructor->GetPrimaryKeyScheme(Self);
            {
                auto filterConclusion = NOlap::TPKRangesFilter::BuildFromProto(request, request.GetReverse(), ydbKey);
                if (filterConclusion.IsFail()) {
                    return SendError("cannot build ranges filter", filterConclusion.GetErrorMessage(), ctx);
                }
                read.PKRangesFilter = filterConclusion.DetachResult();
            }
            auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
            if (!newRange) {
                return SendError("cannot build metadata", newRange.GetErrorMessage(), ctx);
            }
            readMetadataRange = TValidator::CheckNotNull(newRange.DetachResult());
        }
    }
    AFL_VERIFY(readMetadataRange);

    TStringBuilder detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << *readMetadataRange << ")" << " req: " << request;
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(readMetadataRange, index);

    Self->Counters.GetTabletCounters()->OnScanStarted(Self->InFlightReadsTracker.GetSelectStatsDelta());

    TComputeShardingPolicy shardingPolicy;
    AFL_VERIFY(shardingPolicy.DeserializeFromProto(request.GetComputeShardingPolicy()));

    auto scanActor = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(),
        shardingPolicy, scanId, txId, scanGen, requestCookie, Self->TabletID(), timeout, readMetadataRange, dataFormat, Self->Counters.GetScanCounters()));

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TTxScan started")("actor_id", scanActor)("trace_detailed", detailedInfo);
}

}
