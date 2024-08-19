#include "tx_scan.h"
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>

namespace NKikimr::NOlap::NReader {

std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<NScheme::TTypeInfo> types;
    types.reserve(columns.size());
    for (auto& [name, type] : columns) {
        types.push_back(type);
    }
    return types;
}

TString FromCells(const TConstArrayRef<TCell>& cells, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    Y_ABORT_UNLESS(cells.size() == columns.size());
    if (cells.empty()) {
        return {};
    }

    std::vector<NScheme::TTypeInfo> types = ExtractTypes(columns);

    NArrow::TArrowBatchBuilder batchBuilder;
    batchBuilder.Reserve(1);
    auto startStatus = batchBuilder.Start(columns);
    Y_ABORT_UNLESS(startStatus.ok(), "%s", startStatus.ToString().c_str());

    batchBuilder.AddRow(NKikimr::TDbTupleRef(), NKikimr::TDbTupleRef(types.data(), cells.data(), cells.size()));

    auto batch = batchBuilder.FlushBatch(false);
    Y_ABORT_UNLESS(batch);
    Y_ABORT_UNLESS(batch->num_columns() == (int)cells.size());
    Y_ABORT_UNLESS(batch->num_rows() == 1);
    return NArrow::SerializeBatchNoCompression(batch);
}

std::pair<TPredicate, TPredicate> RangePredicates(const TSerializedTableRange& range, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<TCell> leftCells;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> leftColumns;
    bool leftTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.From.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        leftCells.reserve(size);
        leftColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                leftCells.push_back(cells[i]);
                leftColumns.push_back(columns[i]);
                leftTrailingNull = false;
            } else {
                leftTrailingNull = true;
            }
        }
    }

    std::vector<TCell> rightCells;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> rightColumns;
    bool rightTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.To.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        rightCells.reserve(size);
        rightColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                rightCells.push_back(cells[i]);
                rightColumns.push_back(columns[i]);
                rightTrailingNull = false;
            } else {
                rightTrailingNull = true;
            }
        }
    }

    const bool fromInclusive = range.FromInclusive || leftTrailingNull;
    const bool toInclusive = range.ToInclusive && !rightTrailingNull;

    TString leftBorder = FromCells(leftCells, leftColumns);
    TString rightBorder = FromCells(rightCells, rightColumns);
    auto leftSchema = NArrow::MakeArrowSchema(leftColumns);
    Y_ASSERT(leftSchema.ok());
    auto rightSchema = NArrow::MakeArrowSchema(rightColumns);
    Y_ASSERT(rightSchema.ok());
    return std::make_pair(
        TPredicate(fromInclusive ? NKernels::EOperation::GreaterEqual : NKernels::EOperation::Greater, leftBorder, leftSchema.ValueUnsafe()),
        TPredicate(toInclusive ? NKernels::EOperation::LessEqual : NKernels::EOperation::Less, rightBorder, rightSchema.ValueUnsafe()));
}

static bool FillPredicatesFromRange(TReadDescription& read, const ::NKikimrTx::TKeyRange& keyRange,
    const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbPk, ui64 tabletId, const TIndexInfo* indexInfo, TString& error) {
    TSerializedTableRange range(keyRange);
    auto fromPredicate = std::make_shared<TPredicate>();
    auto toPredicate = std::make_shared<TPredicate>();
    std::tie(*fromPredicate, *toPredicate) = RangePredicates(range, ydbPk);

    LOG_S_DEBUG("TTxScan range predicate. From key size: " << range.From.GetCells().size()
        << " To key size: " << range.To.GetCells().size()
        << " greater predicate over columns: " << fromPredicate->ToString()
        << " less predicate over columns: " << toPredicate->ToString()
        << " at tablet " << tabletId);

    if (!read.PKRangesFilter.Add(fromPredicate, toPredicate, indexInfo)) {
        error = "Error building filter";
        return false;
    }
    return true;
}

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
            auto* indexInfo = (vIndex && isIndex) ? &vIndex->GetSchema(snapshot)->GetIndexInfo() : nullptr;
            for (auto& range : request.GetRanges()) {
                TString errorDescription;
                if (!FillPredicatesFromRange(read, range, ydbKey, Self->TabletID(), indexInfo, errorDescription)) {
                    return SendError("cannot fill predicates", errorDescription, ctx);
                }
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
