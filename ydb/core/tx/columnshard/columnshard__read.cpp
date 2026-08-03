#include "columnshard.h"
#include "columnshard_impl.h"

#include "data_reader/actor.h"
#include "engines/predicate/filter.h"
#include "engines/scheme/index_info.h"

#include <ydb/core/formats/arrow/converter.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <ydb/core/scheme/scheme_tablecell.h>

#include <atomic>

namespace NKikimr::NColumnShard {

namespace {

constexpr TDuration DEFAULT_READ_TIMEOUT = TDuration::Seconds(60);

// Adapter that translates a DataShard read-iterator point lookup (TEvDataShard::TEvRead)
// into a ColumnShard internal scan (TEvColumnShard::TEvInternalScan), collects the
// FORMAT_ARROW scan output, converts it to FORMAT_CELLVEC and replies with a single
// TEvDataShard::TEvReadResult.
//
// Only point lookups by full primary key are supported (this is exactly what the KQP
// stream lookup / stream lookup join worker issues against a shard).
class TReadIteratorRestoreTask: public NOlap::NDataReader::IRestoreTask {
private:
    using TBase = NOlap::NDataReader::IRestoreTask;

    const NActors::TActorId ReplyTo;
    const ui64 Cookie;
    const ui64 ReadId;
    const NColumnShard::TUnifiedPathId PathId;
    const NOlap::TSnapshot Snapshot;
    const std::optional<ui64> SchemaVersion;

    // Primary key description (used to build the point ranges filter).
    const std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbPk;
    const std::shared_ptr<arrow::Schema> ArrPk;

    // Column ids to feed into the internal scan (defines the arrow columns produced).
    const std::vector<ui32> ScanColumnIds;

    // Output schema in the exact order requested by the reader. TArrowToYdbConverter
    // selects/reorders arrow columns by name, so building this in requested order
    // guarantees the CELLVEC column order matches the reader's expectations.
    const std::vector<std::pair<TString, NScheme::TTypeInfo>> ResultSchema;

    // Requested point keys (full PK cells).
    const std::vector<TSerializedCellVec> Keys;

    TOwnedCellVecBatch ResultBatch;
    std::atomic<bool> Finished{false};
    TString Error;

    class TRowWriter: public NArrow::IRowWriter {
    private:
        TOwnedCellVecBatch& Batch;

    public:
        TRowWriter(TOwnedCellVecBatch& batch)
            : Batch(batch)
        {
        }

        void AddRow(const TConstArrayRef<TCell>& cells) override {
            Batch.Append(cells);
        }
    };

    virtual std::unique_ptr<TEvColumnShard::TEvInternalScan> DoBuildRequestInitiator() const override {
        auto request = std::make_unique<TEvColumnShard::TEvInternalScan>(PathId, Snapshot, std::nullopt, false);
        request->TaskIdentifier = GetTaskId();
        if (SchemaVersion) {
            request->SchemaVersion = SchemaVersion;
        }

        NOlap::TRangesBuilder rangesBuilder(YdbPk, ArrPk);
        for (const auto& key : Keys) {
            const auto cells = key.GetCells();
            rangesBuilder.AddRange(TSerializedTableRange(cells, true, cells, true));
        }
        request->RangesFilter = std::make_shared<NOlap::TPKRangesFilter>(rangesBuilder.Finish().DetachResult());

        for (const auto id : ScanColumnIds) {
            request->AddColumn(id);
        }
        return request;
    }

    virtual TConclusionStatus DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) override {
        auto batch = NArrow::ToBatch(data);
        TRowWriter writer(ResultBatch);
        NArrow::TArrowToYdbConverter converter(ResultSchema, writer, false, false);
        TString errorMessage;
        if (!converter.Process(*batch, errorMessage)) {
            Error = errorMessage;
            return TConclusionStatus::Fail(errorMessage);
        }
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoOnFinished() override {
        SendResult(Ydb::StatusIds::SUCCESS);
        return TConclusionStatus::Success();
    }

    virtual void DoOnError(const TString& errorMessage) override {
        Error = errorMessage;
        SendResult(Ydb::StatusIds::INTERNAL_ERROR);
    }

    void SendResult(const Ydb::StatusIds::StatusCode code) {
        bool expected = false;
        if (!Finished.compare_exchange_strong(expected, true)) {
            return;
        }

        auto ev = std::make_unique<TEvDataShard::TEvReadResult>();
        auto& record = ev->Record;
        record.SetReadId(ReadId);
        record.SetSeqNo(1);
        record.MutableStatus()->SetCode(code);
        if (code != Ydb::StatusIds::SUCCESS && Error) {
            auto* issue = record.MutableStatus()->AddIssues();
            issue->set_message(Error.data(), Error.size());
        }
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        record.MutableSnapshot()->SetStep(Snapshot.GetPlanStep());
        record.MutableSnapshot()->SetTxId(Snapshot.GetTxId());
        record.SetFinished(true);
        if (code == Ydb::StatusIds::SUCCESS) {
            record.SetRowCount(ResultBatch.Size());
            ev->SetBatch(std::move(ResultBatch));
        } else {
            record.SetRowCount(0);
        }

        NActors::TActivationContext::AsActorContext().Send(ReplyTo, ev.release(), 0, Cookie);
    }

public:
    virtual bool IsActive() const override {
        return !Finished.load();
    }

    virtual TString GetErrorMessage() const override {
        return Error;
    }

    virtual TDuration GetTimeout() const override {
        return DEFAULT_READ_TIMEOUT;
    }

    TReadIteratorRestoreTask(const ui64 tabletId, const NActors::TActorId& tabletActorId, const NActors::TActorId& replyTo,
        const ui64 cookie, const ui64 readId, const NColumnShard::TUnifiedPathId& pathId, const NOlap::TSnapshot& snapshot,
        const std::optional<ui64> schemaVersion, std::vector<std::pair<TString, NScheme::TTypeInfo>>&& ydbPk,
        const std::shared_ptr<arrow::Schema>& arrPk, std::vector<ui32>&& scanColumnIds,
        std::vector<std::pair<TString, NScheme::TTypeInfo>>&& resultSchema, std::vector<TSerializedCellVec>&& keys)
        : TBase(tabletId, tabletActorId, "read_iterator::" + ::ToString(readId))
        , ReplyTo(replyTo)
        , Cookie(cookie)
        , ReadId(readId)
        , PathId(pathId)
        , Snapshot(snapshot)
        , SchemaVersion(schemaVersion)
        , YdbPk(std::move(ydbPk))
        , ArrPk(arrPk)
        , ScanColumnIds(std::move(scanColumnIds))
        , ResultSchema(std::move(resultSchema))
        , Keys(std::move(keys))
    {
    }
};

void SendReadError(const TActorContext& ctx, const NActors::TActorId& replyTo, const ui64 cookie, const ui64 readId,
    const Ydb::StatusIds::StatusCode code, const TString& message) {
    auto ev = std::make_unique<TEvDataShard::TEvReadResult>();
    auto& record = ev->Record;
    record.SetReadId(readId);
    record.SetSeqNo(1);
    record.MutableStatus()->SetCode(code);
    if (message) {
        auto* issue = record.MutableStatus()->AddIssues();
        issue->set_message(message.data(), message.size());
    }
    record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
    record.SetRowCount(0);
    record.SetFinished(true);
    ctx.Send(replyTo, ev.release(), 0, cookie);
}

}   // namespace

void TColumnShard::Handle(TEvDataShard::TEvRead::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    const NActors::TActorId replyTo = ev->Sender;
    const ui64 cookie = ev->Cookie;
    const ui64 readId = record.GetReadId();

    if (!record.HasTableId()) {
        SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::BAD_REQUEST, "TEvRead without TableId");
        return;
    }

    if (!ev->Get()->Ranges.empty()) {
        SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::UNSUPPORTED,
            "range reads are not supported by column shard read iterator");
        return;
    }

    if (ev->Get()->Keys.empty()) {
        SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::BAD_REQUEST, "TEvRead without keys");
        return;
    }

    if (!TablesManager.HasPrimaryIndex()) {
        SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::NOT_FOUND, "no primary index at column shard");
        return;
    }

    const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(record.GetTableId().GetTableId());
    const auto internalPathId = TablesManager.ResolveInternalPathIdOptional(schemeShardLocalPathId, false);
    if (!internalPathId) {
        SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::NOT_FOUND, "unknown path id at column shard");
        return;
    }
    const auto pathId = TUnifiedPathId::BuildValid(*internalPathId, schemeShardLocalPathId);

    NOlap::TSnapshot requestSnapshot = NOlap::TSnapshot::Zero();
    if (record.HasSnapshot() && record.GetSnapshot().GetStep()) {
        requestSnapshot = NOlap::TSnapshot(record.GetSnapshot().GetStep(), record.GetSnapshot().GetTxId());
    } else {
        requestSnapshot = GetMaxReadVersion();
    }
    const NOlap::TSnapshot snapshot = TablesManager.ResolveReadSnapshot(schemeShardLocalPathId, requestSnapshot);

    const NOlap::TIndexInfo& indexInfo = TablesManager.GetIndexInfo(snapshot);

    // Build the requested output schema (name + type) in the exact requested order.
    const auto& columns = indexInfo.GetColumns();
    std::vector<std::pair<TString, NScheme::TTypeInfo>> resultSchema;
    std::vector<ui32> scanColumnIds;
    resultSchema.reserve(record.ColumnsSize());
    scanColumnIds.reserve(record.ColumnsSize());
    for (const ui32 columnId : record.GetColumns()) {
        auto it = columns.find(columnId);
        if (it == columns.end()) {
            SendReadError(ctx, replyTo, cookie, readId, Ydb::StatusIds::SCHEME_ERROR,
                "unknown column id " + ::ToString(columnId) + " at column shard");
            return;
        }
        resultSchema.emplace_back(it->second);
        scanColumnIds.emplace_back(columnId);
    }

    // The internal scan needs at least one column to produce rows. When the reader asks
    // for no columns (e.g. Count(*)) fall back to reading the first PK column and emit
    // empty cell rows (the result schema stays empty).
    if (scanColumnIds.empty()) {
        scanColumnIds.emplace_back(indexInfo.GetPKFirstColumnId());
    }

    std::vector<std::pair<TString, NScheme::TTypeInfo>> ydbPk = indexInfo.GetPrimaryKeyColumns();
    std::shared_ptr<arrow::Schema> arrPk = indexInfo.GetPrimaryKey();

    std::vector<TSerializedCellVec> keys(ev->Get()->Keys.begin(), ev->Get()->Keys.end());

    std::optional<ui64> schemaVersion;
    if (record.GetTableId().HasSchemaVersion() && record.GetTableId().GetSchemaVersion()) {
        schemaVersion = record.GetTableId().GetSchemaVersion();
    }

    auto task = std::make_shared<TReadIteratorRestoreTask>(TabletID(), SelfId(), replyTo, cookie, readId, pathId, snapshot,
        schemaVersion, std::move(ydbPk), arrPk, std::move(scanColumnIds), std::move(resultSchema), std::move(keys));
    ctx.Register(new NOlap::NDataReader::TActor(task));
}

}   // namespace NKikimr::NColumnShard
