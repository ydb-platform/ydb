#include "datashard_impl.h"
#include "range_ops.h"
#include "scan_common.h"
#include "upload_stats.h"
#include "buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NDataShard {

#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

static std::shared_ptr<NTxProxy::TUploadTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings) {
    auto types = GetAllTypes(tableInfo);

    Y_ABORT_UNLESS(buildSettings.columnSize() > 0);
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

        auto& back = cells.emplace_back();
        if (!CellFromProtoVal(typeInfo, typeMod, &column.default_from_literal().value(), false, back, err, valueDataPool)) {
            return false;
        }
    }

    return true;
}

template <NKikimrServices::TActivity::EType Activity>
class TBuildScanUpload: public TActor<TBuildScanUpload<Activity>>, public NTable::IScan {
    using TThis = TBuildScanUpload<Activity>;
    using TBase = TActor<TThis>;

protected:
    const TUploadLimits Limits;

    const ui64 BuildIndexId;
    const TString TargetTable;
    const TScanRecord::TSeqNo SeqNo;

    const ui64 DataShardId;
    const TActorId ProgressActorId;

    TTags ScanTags;                                             // first: columns we scan, order as in IndexTable
    std::shared_ptr<NTxProxy::TUploadTypes> UploadColumnsTypes; // columns types we upload to indexTable
    NTxProxy::EUploadRowsMode UploadMode;

    const TTags KeyColumnIds;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;

    IDriver* Driver = nullptr;

    TBufferData ReadBuf;
    TBufferData WriteBuf;
    TSerializedCellVec LastUploadedKey;

    TActorId Uploader;
    ui64 RetryCount = 0;

    TUploadMonStats Stats = TUploadMonStats("tablets", "build_index_upload");
    TUploadStatus UploadStatus;

    TBuildScanUpload(ui64 buildIndexId,
                     const TString& target,
                     const TScanRecord::TSeqNo& seqNo,
                     ui64 dataShardId,
                     const TActorId& progressActorId,
                     const TSerializedTableRange& range,
                     const TUserTable& tableInfo,
                     TUploadLimits limits)
        : TBase(&TThis::StateWork)
        , Limits(limits)
        , BuildIndexId(buildIndexId)
        , TargetTable(target)
        , SeqNo(seqNo)
        , DataShardId(dataShardId)
        , ProgressActorId(progressActorId)
        , KeyColumnIds(tableInfo.KeyColumnIds)
        , KeyTypes(tableInfo.KeyColumnTypes)
        , TableRange(tableInfo.Range)
        , RequestedRange(range) {
    }

    template <typename TAddRow>
    EScan FeedImpl(TArrayRef<const TCell> key, const TRow& /*row*/, TAddRow&& addRow) {
        LOG_T("Feed key " << DebugPrintPoint(KeyTypes, key, *AppData()->TypeRegistry) << " " << Debug());

        addRow();

        if (!ReadBuf.IsReachLimits(Limits)) {
            return EScan::Feed;
        }

        if (!WriteBuf.IsEmpty()) {
            return EScan::Sleep;
        }

        ReadBuf.FlushTo(WriteBuf);

        Upload();

        return EScan::Feed;
    }

public:
    static constexpr auto ActorActivityType() {
        return Activity;
    }

    ~TBuildScanUpload() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) override {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        LOG_D("Prepare " << Debug());

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) override {
        LOG_T("Seek no " << seq << " " << Debug());
        if (seq) {
            if (!WriteBuf.IsEmpty()) {
                return EScan::Sleep;
            }

            if (!ReadBuf.IsEmpty()) {
                ReadBuf.FlushTo(WriteBuf);
                Upload();
                return EScan::Sleep;
            }

            if (UploadStatus.IsNone()) {
                UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
                UploadStatus.Issues.AddIssue(NYql::TIssue("Shard or requested range is empty"));
            }

            return EScan::Final;
        }

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) override {
        if (Uploader) {
            this->Send(Uploader, new TEvents::TEvPoisonPill);
            Uploader = {};
        }

        TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
        progress->Record.SetBuildIndexId(BuildIndexId);
        progress->Record.SetTabletId(DataShardId);
        progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
        progress->Record.SetRequestSeqNoRound(SeqNo.Round);

        if (abort != EAbort::None) {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
            UploadStatus.Issues.AddIssue(NYql::TIssue("Aborted by scan host env"));

            LOG_W(Debug());
        } else if (!UploadStatus.IsSuccess()) {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        } else {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
        }

        UploadStatusToMessage(progress->Record);

        this->Send(ProgressActorId, progress.Release());

        LOG_D("Finish " << Debug());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    void UploadStatusToMessage(NKikimrTxDataShard::TEvBuildIndexProgressResponse& msg) {
        msg.SetUploadStatus(UploadStatus.StatusCode);
        NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
    }

    void Describe(IOutputStream& out) const override {
        out << Debug();
    }

    TString Debug() const {
        return TStringBuilder() << "TBuildIndexScan: "
                                << "datashard: " << DataShardId
                                << ", requested range: " << DebugPrintRange(KeyTypes, RequestedRange.ToTableRange(), *AppData()->TypeRegistry)
                                << ", last acked point: " << DebugPrintPoint(KeyTypes, LastUploadedKey.GetCells(), *AppData()->TypeRegistry)
                                << Stats.ToString()
                                << UploadStatus.ToString();
    }

    EScan PageFault() override {
        LOG_T("Page fault"
              << " ReadBuf empty: " << ReadBuf.IsEmpty()
              << " WriteBuf empty: " << WriteBuf.IsEmpty()
              << " " << Debug());

        if (!ReadBuf.IsEmpty() && WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload();
        }

        return EScan::Feed;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("TBuildIndexScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/) {
        LOG_D("Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            RetryUpload();
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_T("Handle TEvUploadRowsResponse "
              << Debug()
              << " Uploader: " << Uploader.ToString()
              << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender,
                       "Mismatch"
                           << " Uploader: " << Uploader.ToString()
                           << " ev->Sender: " << ev->Sender.ToString());
        } else {
            Y_ABORT_UNLESS(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues.AddIssues(ev->Get()->Issues);

        if (UploadStatus.IsSuccess()) {
            Stats.Aggr(&WriteBuf);
            LastUploadedKey = WriteBuf.ExtractLastKey();

            //send progress
            TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
            progress->Record.SetBuildIndexId(BuildIndexId);
            progress->Record.SetTabletId(DataShardId);
            progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
            progress->Record.SetRequestSeqNoRound(SeqNo.Round);

            // TODO(mbkkt) ReleaseBuffer isn't possible, we use LastUploadedKey for logging
            progress->Record.SetLastKeyAck(LastUploadedKey.GetBuffer());
            progress->Record.SetRowsDelta(WriteBuf.GetRows());
            progress->Record.SetBytesDelta(WriteBuf.GetBytes());
            WriteBuf.Clear();

            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
            UploadStatusToMessage(progress->Record);

            this->Send(ProgressActorId, progress.Release());

            if (!ReadBuf.IsEmpty() && ReadBuf.IsReachLimits(Limits)) {
                ReadBuf.FlushTo(WriteBuf);
                Upload();
            }

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < Limits.MaxUploadRowsRetryCount && UploadStatus.IsRetriable()) {
            LOG_N("Got retriable error, " << Debug());

            ctx.Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug());

        Driver->Touch(EScan::Final);
    }

    void RetryUpload() {
        Upload(true);
    }

    void Upload(bool isRetry = false) {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        LOG_D("Upload, last key " << DebugPrintPoint(KeyTypes, WriteBuf.GetLastKey().GetCells(), *AppData()->TypeRegistry) << " " << Debug());

        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(), TargetTable,
            UploadColumnsTypes,
            WriteBuf.GetRowsData(),
            UploadMode,
            true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }
};

class TBuildIndexScan final: public TBuildScanUpload<NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR> {
    const ui32 TargetDataColumnPos; // positon of first data column in target table

public:
    TBuildIndexScan(ui64 buildIndexId,
                    const TString& target,
                    const TScanRecord::TSeqNo& seqNo,
                    ui64 dataShardId,
                    const TActorId& progressActorId,
                    const TSerializedTableRange& range,
                    TProtoColumnsCRef targetIndexColumns,
                    TProtoColumnsCRef targetDataColumns,
                    const TUserTable& tableInfo,
                    TUploadLimits limits)
        : TBuildScanUpload(buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits)
        , TargetDataColumnPos(targetIndexColumns.size()) {
        ScanTags = BuildTags(tableInfo, targetIndexColumns, targetDataColumns);
        UploadColumnsTypes = BuildTypes(tableInfo, targetIndexColumns, targetDataColumns);
        UploadMode = NTxProxy::EUploadRowsMode::WriteToTableShadow;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final {
        return FeedImpl(key, row, [&] {
            const auto rowCells = *row;

            ReadBuf.AddRow(
                TSerializedCellVec(key),
                TSerializedCellVec(rowCells.Slice(0, TargetDataColumnPos)),
                TSerializedCellVec::Serialize(rowCells.Slice(TargetDataColumnPos)));
        });
    }
};

class TBuildColumnsScan final: public TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR> {
    TString ValueSerialized;

public:
    TBuildColumnsScan(ui64 buildIndexId,
                      const TString& target,
                      const TScanRecord::TSeqNo& seqNo,
                      ui64 dataShardId,
                      const TActorId& progressActorId,
                      const TSerializedTableRange& range,
                      const NKikimrIndexBuilder::TColumnBuildSettings& columnBuildSettings,
                      const TUserTable& tableInfo,
                      TUploadLimits limits)
        : TBuildScanUpload(buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits) {
        Y_ABORT_UNLESS(columnBuildSettings.columnSize() > 0);
        UploadColumnsTypes = BuildTypes(tableInfo, columnBuildSettings);
        UploadMode = NTxProxy::EUploadRowsMode::UpsertIfExists;

        TMemoryPool valueDataPool(256);
        TVector<TCell> cells;
        TString err;
        Y_ABORT_UNLESS(BuildExtraColumns(cells, columnBuildSettings, err, valueDataPool));
        ValueSerialized = TSerializedCellVec::Serialize(cells);
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final {
        return FeedImpl(key, row, [&] {
            TSerializedCellVec pk(key);
            auto pkTarget = pk;
            auto valueTarget = ValueSerialized;
            ReadBuf.AddRow(
                std::move(pk),
                std::move(pkTarget),
                std::move(valueTarget));
        });
    }
};

TAutoPtr<NTable::IScan> CreateBuildIndexScan(
    ui64 buildIndexId,
    TString target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    TProtoColumnsCRef targetIndexColumns,
    TProtoColumnsCRef targetDataColumns,
    const NKikimrIndexBuilder::TColumnBuildSettings& columnsToBuild,
    const TUserTable& tableInfo,
    TUploadLimits limits) {
    if (columnsToBuild.columnSize() > 0) {
        return new TBuildColumnsScan(
            buildIndexId, target, seqNo, dataShardId, progressActorId, range, columnsToBuild, tableInfo, limits);
    }
    return new TBuildIndexScan(
        buildIndexId, target, seqNo, dataShardId, progressActorId, range, targetIndexColumns, targetDataColumns, tableInfo, limits);
}

class TDataShard::TTxHandleSafeBuildIndexScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildIndexScan(TDataShard* self, TEvDataShard::TEvBuildIndexCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvBuildIndexCreateRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeBuildIndexScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildIndexCreateRequest::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion,
                                                     std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    auto badRequest = [&](const TString& error) {
        auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        response->Record.SetBuildIndexId(record.GetBuildIndexId());
        response->Record.SetTabletId(TabletID());
        response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
        response->Record.SetRequestSeqNoRound(seqNo.Round);
        response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
        ctx.Send(ev->Sender, std::move(response));
    };

    const ui64 buildIndexId = record.GetBuildIndexId();
    const ui64 shardId = record.GetTabletId();
    const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());

    if (shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        return;
    }

    if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
        badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        return;
    }

    const auto& userTable = *GetUserTables().at(tableId.PathId.LocalPathId);

    if (const auto* recCard = ScanManager.Get(buildIndexId)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            CancelScan(userTable.LocalTid, scanId);
        }
        ScanManager.Drop(buildIndexId);
    }

    TSerializedTableRange requestedRange;
    requestedRange.Load(record.GetKeyRange());

    auto scanRange = Intersect(userTable.KeyColumnTypes, requestedRange.ToTableRange(), userTable.Range.ToTableRange());

    if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
                                    << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
                                    << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
                                    << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
        return;
    }

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
        return;
    }

    const TSnapshotKey snapshotKey(tableId.PathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                   << " , snapshot step is " << snapshotKey.Step
                   << " , snapshot tx is " << snapshotKey.TxId);
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10);

    TUploadLimits limits;
    if (record.HasMaxBatchRows()) {
        limits.BatchRowsLimit = record.GetMaxBatchRows();
    }
    if (record.HasMaxBatchBytes()) {
        limits.BatchBytesLimit = record.GetMaxBatchBytes();
    }
    if (record.HasMaxRetries()) {
        limits.MaxUploadRowsRetryCount = record.GetMaxRetries();
    }

    const auto scanId = QueueScan(userTable.LocalTid,
                                  CreateBuildIndexScan(buildIndexId,
                                                       record.GetTargetName(),
                                                       seqNo,
                                                       shardId,
                                                       ev->Sender,
                                                       requestedRange,
                                                       record.GetIndexColumns(),
                                                       record.GetDataColumns(),
                                                       record.GetColumnBuildSettings(),
                                                       userTable,
                                                       limits),
                                  0,
                                  scanOpts);

    ScanManager.Set(buildIndexId, seqNo).push_back(scanId);
}

}
