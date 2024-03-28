#include "tx_scan.h"
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/constructor/constructor.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/chunks/chunks.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/portions/portions.h>

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
    bool ok = batchBuilder.Start(columns);
    Y_ABORT_UNLESS(ok);

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
    return std::make_pair(
        TPredicate(fromInclusive ? NKernels::EOperation::GreaterEqual : NKernels::EOperation::Greater, leftBorder, NArrow::MakeArrowSchema(leftColumns)),
        TPredicate(toInclusive ? NKernels::EOperation::LessEqual : NKernels::EOperation::Less, rightBorder, NArrow::MakeArrowSchema(rightColumns)));
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

bool TTxScan::Execute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    auto& record = Ev->Get()->Record;
    TSnapshot snapshot(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
    const auto scanId = record.GetScanId();
    const ui64 txId = record.GetTxId();

    LOG_S_DEBUG("TTxScan prepare txId: " << txId << " scanId: " << scanId << " at tablet " << Self->TabletID());

    TReadDescription read(snapshot, record.GetReverse());
    read.PathId = record.GetLocalPathId();
    read.ReadNothing = !Self->TablesManager.HasTable(read.PathId);
    read.TableName = record.GetTablePath();
    bool isIndex = false;
    std::unique_ptr<IScannerConstructor> scannerConstructor = [&]() {
        const ui64 itemsLimit = record.HasItemsLimit() ? record.GetItemsLimit() : 0;
        if (read.TableName.EndsWith(TIndexInfo::STORE_INDEX_STATS_TABLE) ||
            read.TableName.EndsWith(TIndexInfo::TABLE_INDEX_STATS_TABLE)) {
            return std::unique_ptr<IScannerConstructor>(new NSysView::NChunks::TConstructor(snapshot, itemsLimit, record.GetReverse()));
        }
        if (read.TableName.EndsWith(TIndexInfo::STORE_INDEX_PORTION_STATS_TABLE) ||
            read.TableName.EndsWith(TIndexInfo::TABLE_INDEX_PORTION_STATS_TABLE)) {
            return std::unique_ptr<IScannerConstructor>(new NSysView::NPortions::TConstructor(snapshot, itemsLimit, record.GetReverse()));
        }
        isIndex = true;
        return std::unique_ptr<IScannerConstructor>(new NPlain::TIndexScannerConstructor(snapshot, itemsLimit, record.GetReverse()));
    }();
    read.ColumnIds.assign(record.GetColumnTags().begin(), record.GetColumnTags().end());
    read.StatsMode = record.GetStatsMode();

    const TVersionedIndex* vIndex = Self->GetIndexOptional() ? &Self->GetIndexOptional()->GetVersionedIndex() : nullptr;
    auto parseResult = scannerConstructor->ParseProgram(vIndex, record, read);
    if (!parseResult) {
        ErrorDescription = parseResult.GetErrorMessage();
        return true;
    }

    if (!record.RangesSize()) {
        auto range = scannerConstructor->BuildReadMetadata(Self, read);
        if (range) {
            ReadMetadataRanges = {range.DetachResult()};
        } else {
            ErrorDescription = range.GetErrorMessage();
        }
        return true;
    }

    ReadMetadataRanges.reserve(1);

    auto ydbKey = scannerConstructor->GetPrimaryKeyScheme(Self);
    auto* indexInfo = (vIndex && isIndex) ? &vIndex->GetSchema(snapshot)->GetIndexInfo() : nullptr;
    for (auto& range : record.GetRanges()) {
        if (!FillPredicatesFromRange(read, range, ydbKey, Self->TabletID(), indexInfo, ErrorDescription)) {
            ReadMetadataRanges.clear();
            return true;
        }
    }
    {
        auto newRange = scannerConstructor->BuildReadMetadata(Self, read);
        if (!newRange) {
            ErrorDescription = newRange.GetErrorMessage();
            ReadMetadataRanges.clear();
            return true;
        }
        ReadMetadataRanges.emplace_back(newRange.DetachResult());
    }
    Y_ABORT_UNLESS(ReadMetadataRanges.size() == 1);

    return true;
}

template <typename T>
struct TContainerPrinter {
    const T& Ref;

    TContainerPrinter(const T& ref)
        : Ref(ref) {
    }

    friend IOutputStream& operator << (IOutputStream& out, const TContainerPrinter& cont) {
        for (auto& ptr : cont.Ref) {
            out << *ptr << " ";
        }
        return out;
    }
};

void TTxScan::Complete(const TActorContext& ctx) {
    auto& request = Ev->Get()->Record;
    auto scanComputeActor = Ev->Sender;
    const auto& snapshot = request.GetSnapshot();
    const auto scanId = request.GetScanId();
    const ui64 txId = request.GetTxId();
    const ui32 scanGen = request.GetGeneration();
    TString table = request.GetTablePath();
    auto dataFormat = request.GetDataFormat();
    const TDuration timeout = TDuration::MilliSeconds(request.GetTimeoutMs());
    if (scanGen > 1) {
        Self->IncCounter(NColumnShard::COUNTER_SCAN_RESTARTED);
    }

    TStringStream detailedInfo;
    if (IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, NKikimrServices::TX_COLUMNSHARD)) {
        detailedInfo << " read metadata: (" << TContainerPrinter(ReadMetadataRanges) << ")" << " req: " << request;
    }
    if (ReadMetadataRanges.empty()) {
        LOG_S_DEBUG("TTxScan failed "
            << " txId: " << txId
            << " scanId: " << scanId
            << " gen: " << scanGen
            << " table: " << table
            << " snapshot: " << snapshot
            << " timeout: " << timeout
            << detailedInfo.Str()
            << " at tablet " << Self->TabletID());

        auto ev = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(scanGen, Self->TabletID());

        ev->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder()
            << "Table " << table << " (shard " << Self->TabletID() << ") scan failed, reason: " << ErrorDescription ? ErrorDescription : "unknown error");
        NYql::IssueToMessage(issue, ev->Record.MutableIssues()->Add());

        ctx.Send(scanComputeActor, ev.Release());
        return;
    }

    const TVersionedIndex* index = nullptr;
    if (Self->HasIndex()) {
        index = &Self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex();
    }
    ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(ReadMetadataRanges, index);
    auto statsDelta = Self->InFlightReadsTracker.GetSelectStatsDelta();

    Self->IncCounter(NColumnShard::COUNTER_READ_INDEX_PORTIONS, statsDelta.Portions);
    Self->IncCounter(NColumnShard::COUNTER_READ_INDEX_BLOBS, statsDelta.Blobs);
    Self->IncCounter(NColumnShard::COUNTER_READ_INDEX_ROWS, statsDelta.Rows);
    Self->IncCounter(NColumnShard::COUNTER_READ_INDEX_BYTES, statsDelta.Bytes);

    TComputeShardingPolicy shardingPolicy;
    AFL_VERIFY(shardingPolicy.DeserializeFromProto(request.GetComputeShardingPolicy()));

    auto scanActor = ctx.Register(new TColumnShardScan(Self->SelfId(), scanComputeActor, Self->GetStoragesManager(),
        shardingPolicy, scanId, txId, scanGen, requestCookie, Self->TabletID(), timeout, std::move(ReadMetadataRanges), dataFormat, Self->ScanCounters));

    LOG_S_DEBUG("TTxScan starting " << scanActor
        << " txId: " << txId
        << " scanId: " << scanId
        << " gen: " << scanGen
        << " table: " << table
        << " snapshot: " << snapshot
        << " timeout: " << timeout
        << detailedInfo.Str()
        << " at tablet " << Self->TabletID());
}

}
