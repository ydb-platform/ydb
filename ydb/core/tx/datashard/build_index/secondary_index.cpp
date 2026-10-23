#include "build_index_scan_manager.h"
#include "build_scan_upload.h"
#include "generated_columns_scan.h"
#include "common_helper.h"
#include "../datashard_impl.h"
#include "../range_ops.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <util/generic/algorithm.h>
#include <util/generic/bitops.h>
#include <util/string/builder.h>

#include <deque>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard {

static std::shared_ptr<NTxProxy::TUploadTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings) {
    auto types = GetAllTypes(tableInfo);

    Y_ENSURE(buildSettings.columnSize() > 0);
    auto result = std::make_shared<NTxProxy::TUploadTypes>();
    result->reserve(tableInfo.KeyColumnIds.size() + buildSettings.columnSize());

    for (const auto& keyColId : tableInfo.KeyColumnIds) {
        auto it = tableInfo.Columns.at(keyColId);
        Ydb::Type type;
        NScheme::ProtoFromTypeInfo(it.Type, type);
        result->emplace_back(it.Name, type);
    }
    for (size_t i = 0; i < buildSettings.columnSize(); i++) {
        const auto& column = buildSettings.column(i);
        result->emplace_back(column.GetColumnName(), column.default_from_literal().type());
    }
    return result;
}

static std::shared_ptr<NTxProxy::TUploadTypes> BuildTypes(const TUserTable& tableInfo, TProtoColumnsCRef indexColumns, TProtoColumnsCRef dataColumns) {
    auto types = GetAllTypes(tableInfo);

    auto result = std::make_shared<NTxProxy::TUploadTypes>();
    result->reserve(indexColumns.size() + dataColumns.size());

    for (const auto& colName : indexColumns) {
        Ydb::Type type;
        NScheme::ProtoFromTypeInfo(types.at(colName), type);
        result->emplace_back(colName, type);
    }
    for (const auto& colName : dataColumns) {
        Ydb::Type type;
        NScheme::ProtoFromTypeInfo(types.at(colName), type);
        result->emplace_back(colName, type);
    }
    return result;
}

bool BuildExtraColumns(TVector<TCell>& cells, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings, TString& err, TMemoryPool& valueDataPool) {
    cells.clear();
    cells.reserve(buildSettings.columnSize());
    for (size_t i = 0; i < buildSettings.columnSize(); i++) {
        const auto& column = buildSettings.column(i);

        auto& back = cells.emplace_back();

        if (!column.default_from_sequence().empty()) {
            // Sequence-default columns are filled per-row in Feed() from TEvNextValResult.
            continue;
        }

        NScheme::TTypeInfo typeInfo;
        i32 typeMod = -1;
        Ydb::StatusIds::StatusCode status;

        if (column.default_from_literal().type().has_pg_type()) {
            typeMod = column.default_from_literal().type().pg_type().typmod();
        }

        TString unusedtm;
        if (!ExtractColumnTypeInfo(typeInfo, unusedtm, column.default_from_literal().type(), status, err)) {
            return false;
        }

        if (!CellFromProtoVal(typeInfo, typeMod, &column.default_from_literal().value(), false, back, err, valueDataPool)) {
            return false;
        }
    }

    return true;
}


class TBuildIndexScan final: public TBuildScanUpload<NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR> {
    const NTable::TPos TargetDataColumnPos; // position of first data column in target table

public:
    TBuildIndexScan(ui64 buildIndexId,
                    const TString& databaseName,
                    const TString& target,
                    const TScanRecord::TSeqNo& seqNo,
                    ui64 dataShardId,
                    const TActorId& progressActorId,
                    const TSerializedTableRange& range,
                    TProtoColumnsCRef targetIndexColumns,
                    TProtoColumnsCRef targetDataColumns,
                    const TUserTable& tableInfo,
                    const TIndexBuildScanSettings& scanSettings)
        : TBuildScanUpload(buildIndexId, databaseName, target, seqNo, dataShardId, progressActorId, range, tableInfo, scanSettings, false)
        , TargetDataColumnPos(targetIndexColumns.size()) {
        ScanTags = BuildTags(tableInfo, targetIndexColumns, targetDataColumns);
        UploadColumnsTypes = BuildTypes(tableInfo, targetIndexColumns, targetDataColumns);
        UploadMode = NTxProxy::EUploadRowsMode::WriteToTableShadow;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final {
        return FeedImpl(key, row, [&] {
            const auto rowCells = *row;

            ReadBuf.AddRow(
                rowCells.Slice(0, TargetDataColumnPos),
                rowCells.Slice(TargetDataColumnPos),
                key);
        });
    }
};

class TBuildColumnsScan final: public TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR> {
    TVector<TCell> Value;
    TString ValueSerialized;
    ui32 SequenceRetryCount = 0;

    struct TSequenceColumn {
        size_t ColumnIdx;
        TString SequencePath;
        bool BitReverse = false;
        std::deque<ui64> Buffer;
        ui32 InFlight = 0;
        ui64 LastValue = 0; // stable storage for the TCell::Make pointer
    };
    TVector<TSequenceColumn> SequenceColumns;
    bool ScanWaitingForSequences = false;

    // When Feed cannot make progress because the sequence buffer is empty, we copy
    // the current row's key here and return EScan::Sleep. The flat-scan framework
    // calls Iter->Next() before each Feed, so on resume the iterator has already
    // advanced past this row. We drain PendingKeys from HandleNextVal (or below in
    // Feed if values are now available) so the row is not lost.
    //
    // Multiple keys can pile up if the scan is woken from a non-sequence source
    // (e.g. TEvUploadRowsResponse) while the sequence buffer is still empty: every
    // such wake-up advances Iter past another row, and each must be remembered.
    std::deque<TSerializedCellVec> PendingKeys;

    // True iff Exhausted() was called while PendingKeys was non-empty. We returned
    // Sleep instead of Reset so the scan would not finalize before those rows had
    // a chance to be written. Cleared and the driver re-touched once the queue is
    // drained.
    bool ExhaustedWaitingForDrain = false;

    static constexpr ui32 SequenceBatchTarget = 256;

public:
    TBuildColumnsScan(ui64 buildIndexId,
                      const TString& databaseName,
                      const TString& target,
                      const TScanRecord::TSeqNo& seqNo,
                      ui64 dataShardId,
                      const TActorId& progressActorId,
                      const TSerializedTableRange& range,
                      const NKikimrIndexBuilder::TColumnBuildSettings& columnBuildSettings,
                      const TUserTable& tableInfo,
                      const TIndexBuildScanSettings& scanSettings)
        : TBuildScanUpload(buildIndexId, databaseName, target, seqNo, dataShardId, progressActorId, range, tableInfo, scanSettings, true) {
        Y_ENSURE(columnBuildSettings.columnSize() > 0);
        UploadColumnsTypes = BuildTypes(tableInfo, columnBuildSettings);
        UploadMode = NTxProxy::EUploadRowsMode::UpsertIfExists;

        TMemoryPool valueDataPool(256);
        TString err;
        Y_ENSURE(BuildExtraColumns(Value, columnBuildSettings, err, valueDataPool));

        for (size_t i = 0; i < static_cast<size_t>(columnBuildSettings.columnSize()); ++i) {
            const auto& column = columnBuildSettings.column(i);
            if (!column.default_from_sequence().empty()) {
                SequenceColumns.push_back({
                    .ColumnIdx = i,
                    .SequencePath = column.default_from_sequence(),
                    .BitReverse = column.bit_reverse_sequence_value(),
                });
            }
        }

        if (SequenceColumns.empty()) {
            ValueSerialized = TSerializedCellVec::Serialize(Value);
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        auto state = TBuildScanUpload::Prepare(driver, scheme);
        if (SequenceColumns.empty()) {
            return state;
        }
        // Switch to a StateWork that also handles TEvNextValResult.
        this->Become(&TBuildColumnsScan::SequenceStateFunc);
        ScanWaitingForSequences = true;
        TopUpSequenceRequests();
        return {EScan::Sleep, {}};
    }

    EScan Exhausted() override {
        // If Feed had to defer rows because the sequence buffer was empty, the
        // iterator may exhaust before those rows are written. Returning Reset
        // here (the default) would land in Seek(seq > 0) → Final and lose them.
        // Wait until HandleNextVal drains the queue.
        if (!PendingKeys.empty()) {
            ExhaustedWaitingForDrain = true;
            ScanWaitingForSequences = true;
            return EScan::Sleep;
        }
        return TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR>::Exhausted();
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final {
        if (!SequenceColumns.empty()) {
            // Drain any keys deferred from earlier sleeps before processing this row.
            DrainPendingIfReady();

            for (auto& s : SequenceColumns) {
                if (s.Buffer.empty()) {
                    // Buffer is empty — Iter will advance past this row before
                    // Feed is called again, so queue the key for later processing.
                    PendingKeys.emplace_back(key);
                    ScanWaitingForSequences = true;
                    return EScan::Sleep;
                }
            }
            for (auto& s : SequenceColumns) {
                s.LastValue = s.Buffer.front();
                s.Buffer.pop_front();
                Value[s.ColumnIdx] = TCell::Make<ui64>(s.LastValue);
            }
            TopUpSequenceRequests();
            TString valueSerialized = TSerializedCellVec::Serialize(Value);
            return FeedImpl(key, row, [&] {
                ReadBuf.AddRow(
                    key,
                    Value,
                    std::move(valueSerialized),
                    key);
            });
        }
        return FeedImpl(key, row, [&] {
            auto valueSerializedCopy = ValueSerialized;
            ReadBuf.AddRow(
                key,
                Value,
                std::move(valueSerializedCopy),
                key);
        });
    }

private:
    STFUNC(SequenceStateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSequenceProxy::TEvSequenceProxy::TEvNextValResult, HandleNextVal);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                this->StateWork(ev);
        }
    }

    // If rows were deferred via PendingKeys when Feed had to sleep, consume sequence
    // values for them now and add them to ReadBuf. Drains greedily until either the
    // queue is empty or any sequence column's buffer runs out.
    void DrainPendingIfReady() {
        while (!PendingKeys.empty()) {
            for (auto& s : SequenceColumns) {
                if (s.Buffer.empty()) {
                    return;
                }
            }
            for (auto& s : SequenceColumns) {
                s.LastValue = s.Buffer.front();
                s.Buffer.pop_front();
                Value[s.ColumnIdx] = TCell::Make<ui64>(s.LastValue);
            }
            TopUpSequenceRequests();
            auto pendingKey = std::move(PendingKeys.front());
            PendingKeys.pop_front();
            TConstArrayRef<TCell> pendingKeyCells = pendingKey.GetCells();
            TString pendingSerialized = TSerializedCellVec::Serialize(Value);
            ReadBuf.AddRow(
                pendingKeyCells,
                Value,
                std::move(pendingSerialized),
                pendingKeyCells);
        }
    }

    void TopUpSequenceRequests() {
        for (size_t i = 0; i < SequenceColumns.size(); ++i) {
            auto& seqCol = SequenceColumns[i];
            ui32 total = static_cast<ui32>(seqCol.Buffer.size()) + seqCol.InFlight;
            while (total < SequenceBatchTarget) {
                this->Send(NSequenceProxy::MakeSequenceProxyServiceID(),
                    new NSequenceProxy::TEvSequenceProxy::TEvNextVal(DatabaseName, seqCol.SequencePath),
                    0, i);
                ++seqCol.InFlight;
                ++total;
            }
        }
    }

    void HandleWakeup(const NActors::TActorContext& ctx) override {
        if (!SequenceColumns.empty()) {
            TopUpSequenceRequests();
        }
        TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR>::HandleWakeup(ctx);
    }

    void HandleNextVal(NSequenceProxy::TEvSequenceProxy::TEvNextValResult::TPtr& ev, const NActors::TActorContext& ctx) {
        const size_t colIdx = ev->Cookie;
        Y_ENSURE(colIdx < SequenceColumns.size());
        auto& seqCol = SequenceColumns[colIdx];
        if (seqCol.InFlight > 0) {
            --seqCol.InFlight;
        }

        if (ev->Get()->Status == Ydb::StatusIds::UNAVAILABLE) {
            // Retry UNAVAILABLE errors
            ctx.Schedule(GetRetryWakeupTimeoutBackoff(SequenceRetryCount++), new TEvents::TEvWakeup());
            return;
        }
        SequenceRetryCount = 0;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            UploadStatus.StatusCode = ev->Get()->Status;
            UploadStatus.Issues.AddIssues(ev->Get()->Issues);
            UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder()
                << "Failed to allocate sequence value for build scan, sequence: " << seqCol.SequencePath));
            if (Driver) {
                Driver->Touch(EScan::Final);
            }
            return;
        }

        ui64 val = static_cast<ui64>(ev->Get()->Value);
        if (seqCol.BitReverse) {
            // The bit-reverse flag is set only for __ydb_row_id; apply the shared row-id layout (spread
            // bucket in the high bits, dense counter in the low bits) so backfilled values match those
            // produced by the online-insert sequencer (kqp_sequencer_actor.cpp). The spread keeps writes
            // to the unique index / posting / docs tables distributed instead of hot-spotting on a tail.
            val = NTableIndex::NFulltext::RowIdFromSeq(val);
        }
        seqCol.Buffer.push_back(val);

        // Drain any pending row first so we don't lose it when the scan resumes
        // (Iter will advance past it on the next Process iteration).
        DrainPendingIfReady();

        if (ScanWaitingForSequences) {
            bool shouldWake = false;
            if (ExhaustedWaitingForDrain && PendingKeys.empty()) {
                // We held the scan in Exhausted-Sleep because rows were queued;
                // now that the queue is empty, let the scan finalize via
                // Seek(seq > 0) which will flush ReadBuf and upload.
                ExhaustedWaitingForDrain = false;
                shouldWake = true;
            } else if (!ExhaustedWaitingForDrain) {
                bool allReady = true;
                for (auto& s : SequenceColumns) {
                    if (s.Buffer.empty()) {
                        allReady = false;
                        break;
                    }
                }
                shouldWake = allReady;
            }
            if (shouldWake) {
                ScanWaitingForSequences = false;
                if (Driver) {
                    Driver->Touch(EScan::Feed);
                }
            }
        }
    }
};

TAutoPtr<NTable::IScan> CreateBuildIndexScan(
    ui64 buildIndexId,
    const TString& databaseName,
    const TString& target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    TProtoColumnsCRef targetIndexColumns,
    TProtoColumnsCRef targetDataColumns,
    const NKikimrIndexBuilder::TColumnBuildSettings& columnsToBuild,
    const TUserTable& tableInfo,
    const TIndexBuildScanSettings& scanSettings) {
    if (columnsToBuild.columnSize() > 0) {
        return new TBuildColumnsScan(
            buildIndexId, databaseName, target, seqNo, dataShardId, progressActorId, range, columnsToBuild, tableInfo, scanSettings);
    }
    return new TBuildIndexScan(
        buildIndexId, databaseName, target, seqNo, dataShardId, progressActorId, range, targetIndexColumns, targetDataColumns, tableInfo, scanSettings);
}

class TDataShard::TTxHandleSafeBuildIndexScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildIndexScan(TDataShard* self, TEvDataShard::TEvBuildIndexCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) {
        auto& request = Ev->Get()->Record;
        const ui64 buildId = request.GetId();
        const ui64 seqNoGeneration = request.GetSeqNoGeneration();
        const ui64 seqNoRound = request.GetSeqNoRound();

        NIceDb::TNiceDb db(txc.DB);

        const auto& scans = Self->BuildIndexScanManager.GetScans();
        bool generated = false;
        for (const auto& setting : request.GetColumnBuildSettings().column()) {
            generated |= setting.has_generated_from_expression();
        }
        NKikimrTxDataShard::TGeneratedColumnFillContext context;
        if (generated) {
            TString error;
            const auto table = Self->GetUserTables().find(request.GetPathId());
            bool valid = request.GetOwnerId() == Self->GetPathOwnerId() && table != Self->GetUserTables().end() &&
                ValidateGeneratedColumnsBuild(request, *table->second, context, error);
            if (valid) {
                const TSnapshotKey snapshot(TPathId(request.GetOwnerId(), request.GetPathId()),
                    request.GetSnapshotStep(), request.GetSnapshotTxId());
                valid = Self->GetSnapshotManager().FindAvailable(snapshot) != nullptr;
                if (!valid) {
                    error = "Generated build snapshot is no longer available";
                }
            }
            if (const auto* old = scans.FindPtr(buildId); valid && old) {
                const TScanRecord::TSeqNo previous{old->SeqNoGeneration, old->SeqNoRound};
                const TScanRecord::TSeqNo incoming{seqNoGeneration, seqNoRound};
                if (incoming < previous) {
                    valid = false;
                    error = "Stale generated scan attempt";
                } else if (incoming == previous && !old->GeneratedFillContextSerialized.empty()) {
                    valid = Self->BuildIndexScanManager.ValidateGeneratedFill(context, error);
                }
            }
            if (!valid) {
                auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
                FillScanResponseCommonFields(*response, buildId, Self->TabletID(), TScanRecord::TSeqNo{seqNoGeneration, seqNoRound});
                response->Record.SetStatus(NKikimrIndexBuilder::BAD_REQUEST);
                response->Record.AddIssues()->set_message(TStringBuilder() << "Invalid generated column build: " << error);
                ctx.Send(Ev->Sender, response.Release());
                return true;
            }
        }
        if (const auto* old = scans.FindPtr(buildId)) {
            if (old->SeqNoGeneration != seqNoGeneration || old->SeqNoRound != seqNoRound) {
                Self->BuildIndexScanManager.PersistRemove(db, buildId, old->SeqNoGeneration, old->SeqNoRound);
                Self->PendingBuildIndexFinalResponses.erase(buildId);
            }
        }

        Self->HandleSafe(Ev, ctx);

        if (Ev && Self->GetScanManager().Get(buildId)) {
            Self->BuildIndexScanManager.PersistAdd(db, buildId, seqNoGeneration, seqNoRound,
                static_cast<ui32>(EBuildIndexEventType::SecondaryIndexProgressResponse), generated ? &context : nullptr);
        } else if (Ev) {
            Self->BuildIndexScanManager.PersistRemove(db, buildId, seqNoGeneration, seqNoRound);
        }

        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvBuildIndexCreateRequest::TPtr Ev;
};

class TDataShard::TTxHandleBuildIndexScanProgress: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleBuildIndexScanProgress(TDataShard* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) {
        auto& record = Ev->Get()->Record;
        BuildId = record.GetId();
        const ui64 seqNoGeneration = record.GetRequestSeqNoGeneration();
        const ui64 seqNoRound = record.GetRequestSeqNoRound();

        const auto& scans = Self->BuildIndexScanManager.GetScans();
        if (const auto* info = scans.FindPtr(BuildId)) {
            if (info->SeqNoGeneration == seqNoGeneration && info->SeqNoRound == seqNoRound) {
                ShouldSkip = false;
                NIceDb::TNiceDb db(txc.DB);
                TString serialized;
                Y_ENSURE(record.SerializeToString(&serialized));
                Self->BuildIndexScanManager.PersistMarkFinalResponse(db, BuildId, seqNoGeneration, seqNoRound,
                    serialized);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) {
        if (!ShouldSkip) {
            auto copy = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
            copy->Record = Ev->Get()->Record;
            Self->PendingBuildIndexFinalResponses[BuildId] = std::move(copy);
            Self->SendPendingBuildIndexFinalResponses(ctx);
        }
    }

private:
    TEvDataShard::TEvBuildIndexProgressResponse::TPtr Ev;
    ui64 BuildId = 0;
    bool ShouldSkip = true;
};

void TDataShard::Handle(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& ev, const TActorContext& ctx) {
    const auto status = ev->Get()->Record.GetStatus();
    if (status == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
        if (!StateReportPipe) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.RetryPolicy = SchemeShardPipeRetryPolicy;
            StateReportPipe = ctx.Register(
                NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, clientConfig));
        }
        NTabletPipe::SendData(ctx, StateReportPipe, ev->Release().Release());
        return;
    }
    Execute(new TTxHandleBuildIndexScanProgress(this, std::move(ev)));
}

void TDataShard::Handle(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeBuildIndexScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    TRowVersion rowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId());
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        FillScanResponseCommonFields(*response, request.GetId(), TabletID(), seqNo);

        YDB_LOG_NOTICE("Starting secondary index build scan",
            {"tabletId", TabletID()},
            {"request", request.ShortDebugString()},
            {"rowVersion", rowVersion});

        // Note: it's very unlikely that we have volatile txs before this snapshot
        if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
            VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
            return;
        }

        auto badRequest = [&](const TString& error) {
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        };
        auto trySendBadRequest = [&] {
            if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
                YDB_LOG_ERROR("Rejecting invalid secondary index build scan request",
                    {"tabletId", TabletID()},
                    {"request", request.ShortDebugString()},
                    {"responseRecord", response->Record.ShortDebugString()});
                ctx.Send(ev->Sender, std::move(response));
                return true;
            } else {
                return false;
            }
        };

        // 1. Validating table and path existence
        const auto tableId = TTableId(request.GetOwnerId(), request.GetPathId());
        if (request.GetTabletId() != TabletID()) {
            badRequest(TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << TabletID());
        }
        if (!IsStateActive()) {
            badRequest(TStringBuilder() << "Shard " << TabletID() << " is " << State << " and not ready for requests");
        }
        if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
            badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        }
        if (trySendBadRequest()) {
            return;
        }
        const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);

        // 2. Validating request fields
        if (!request.HasSnapshotStep() || !request.HasSnapshotTxId()) {
            badRequest(TStringBuilder() << "Missing snapshot");
        } else {
            const TSnapshotKey snapshotKey(tableId.PathId, rowVersion.Step, rowVersion.TxId);
            if (!SnapshotManager.FindAvailable(snapshotKey)) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                    << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
            }
        }

        TSerializedTableRange requestedRange;
        requestedRange.Load(request.GetKeyRange());
        auto scanRange = Intersect(userTable.KeyColumnTypes, requestedRange.ToTableRange(), userTable.Range.ToTableRange());
        if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
            badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
                << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
                << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
                << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
        }

        if (!request.GetTargetName()) {
            badRequest(TStringBuilder() << "Empty target table name");
        }

        auto tags = GetAllTags(userTable);
        for (auto column : request.GetIndexColumns()) {
            if (!tags.contains(column)) {
                badRequest(TStringBuilder() << "Unknown index column: " << column);
            }
        }
        for (auto column : request.GetDataColumns()) {
            if (!tags.contains(column)) {
                badRequest(TStringBuilder() << "Unknown data column: " << column);
            }
        }

        bool generated = false;
        for (const auto& setting : request.GetColumnBuildSettings().column()) {
            generated |= setting.has_generated_from_expression();
        }
        NKikimrTxDataShard::TGeneratedColumnFillContext generatedContext;
        if (generated) {
            TString error;
            if (request.GetOwnerId() != GetPathOwnerId() ||
                !ValidateGeneratedColumnsBuild(request, userTable, generatedContext, error))
            {
                badRequest(TStringBuilder() << "Invalid generated column build: " << error);
            }
        }
        if (trySendBadRequest()) {
            return;
        }

        // 3. Creating scan
        TAutoPtr<NTable::IScan> scan;
        if (generated) {
            scan = CreateGeneratedColumnsScan(id, request.GetDatabaseName(), request.GetTargetName(),
                seqNo, request.GetTabletId(), SelfId(), requestedRange,
                request.GetColumnBuildSettings().column(0).generated_from_expression(), generatedContext,
                userTable, request.GetScanSettings());
        } else {
            scan = CreateBuildIndexScan(id,
            request.GetDatabaseName(),
            request.GetTargetName(),
            seqNo,
            request.GetTabletId(),
            SelfId(),
            requestedRange,
            request.GetIndexColumns(),
            request.GetDataColumns(),
            request.GetColumnBuildSettings(),
            userTable,
            request.GetScanSettings());
        }

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvBuildIndexProgressResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TBuildIndexScan");
    }
}

}


#undef YDB_LOG_THIS_FILE_COMPONENT

