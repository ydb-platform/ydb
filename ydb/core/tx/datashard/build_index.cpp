#include "build_index.h"
#include "datashard_impl.h"
#include "upload_stats.h"
#include "range_ops.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NDataShard {

using TColumnsTags = THashMap<TString, NTable::TTag>;
using TColumnsTypes = THashMap<TString, NScheme::TTypeInfo>;

using TTypes = TVector<std::pair<TString, Ydb::Type>>;
using TTags = TVector<NTable::TTag>;

using TRows = TVector<std::pair<TSerializedCellVec, TString>>;

static TColumnsTags GetAllTags(const TUserTable::TCPtr tableInfo) {
    TColumnsTags result;

    for (const auto& it: tableInfo->Columns) {
        result[it.second.Name] = it.first;
    }

    return result;
}

static TColumnsTypes GetAllTypes(const TUserTable::TCPtr tableInfo) {
    TColumnsTypes result;

    for (const auto& it: tableInfo->Columns) {
        result[it.second.Name] = it.second.Type;
    }

    return result;
}

static TTags BuildTags(const TColumnsTags& allTags, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    TTags result;
    result.reserve(indexColumns.size());

    for (const auto& colName: indexColumns) {
        result.push_back(allTags.at(colName));
    }

    for (const auto& colName: dataColumns) {
        result.push_back(allTags.at(colName));
    }

    return result;
}

static std::shared_ptr<TTypes> BuildTypes(const TColumnsTypes& types, const TUserTable::TCPtr& tableInfo, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    auto result = std::make_shared<TTypes>();
    result->reserve(indexColumns.size());

    if (buildSettings.columnSize() > 0) {
        for(const auto& keyColId : tableInfo->KeyColumnIds) {
            auto it = tableInfo->Columns.at(keyColId);
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(it.Type, type);
            result->emplace_back(it.Name, type);
        }

        for(size_t i = 0; i < buildSettings.columnSize(); i++) {
            const auto& column = buildSettings.column(i);
            result->emplace_back(column.GetColumnName(), column.default_from_literal().type());   
        }

    } else {
        for (const auto& colName: indexColumns) {
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(types.at(colName), type);
            result->emplace_back(colName, type);
        }

        for (const auto& colName: dataColumns) {
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(types.at(colName), type);
            result->emplace_back(colName, type);
        }

    }

    return result;
}

bool BuildExtraColumns(TVector<TCell>& cells, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings, TString& err, TMemoryPool& valueDataPool) {
    cells.clear();
    cells.reserve(buildSettings.columnSize());
    for(size_t i = 0; i < buildSettings.columnSize(); i++) {
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

        cells.push_back({});
        if (!CellFromProtoVal(typeInfo, typeMod, &column.default_from_literal().value(), cells.back(), err, valueDataPool)) {
            return false;
        }
    }

    return true;
}

struct TStatus {
    Ydb::StatusIds::StatusCode StatusCode = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;

    bool IsNone() const {
        return StatusCode == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    bool IsSuccess() const {
        return StatusCode == Ydb::StatusIds::SUCCESS;
    }

    bool IsRetriable() const {
        return StatusCode == Ydb::StatusIds::UNAVAILABLE
            || StatusCode == Ydb::StatusIds::OVERLOADED
            || StatusCode == Ydb::StatusIds::TIMEOUT
            ;
    }

    TString ToString() const {
        return TStringBuilder()
            << "Status {"
            << " Code: " << Ydb::StatusIds_StatusCode_Name(StatusCode)
            << " Issues: " << Issues.ToString()
            << " }";
    }
};

struct TUploadLimits {
    ui64 BatchRowsLimit  = 500;
    ui64 BatchBytesLimit = 1u << 23; // 8MB
    ui32 MaxUploadRowsRetryCount = 50;
    ui32 BackoffCeiling = 3;

    TDuration GetTimeoutBackouff(ui32 retryNo) const {
        return TDuration::Seconds(1u << Min(retryNo, BackoffCeiling));
    }
};

class TBufferData : public IStatHolder, public TNonCopyable {
public:
    TBufferData()
        : Rows(new TRows)
    { }

    ui64 GetRows() const override final {
        return Rows->size();
    }

    std::shared_ptr<TRows> GetRowsData() const {
        return Rows;
    }

    ui64 GetBytes() const override final {
        return ByteSize;
    }

    void FlushTo(TBufferData& other) {
        if (this == &other) {
            return;
        }

        Y_ABORT_UNLESS(other.Rows);
        Y_ABORT_UNLESS(other.IsEmpty());

        other.Rows.swap(Rows);
        other.ByteSize = ByteSize;
        other.LastKey = std::move(LastKey);

        Clear();
    }

    void Clear() {
        Rows->clear();
        ByteSize = 0;
        LastKey = {};
    }

    void AddRow(TSerializedCellVec&& key, TSerializedCellVec&& targetPk, TString&& targetValue) {
        Rows->emplace_back(std::move(targetPk), std::move(targetValue));
        ByteSize += Rows->back().first.GetBuffer().size() + Rows->back().second.size();
        LastKey = std::move(key);
    }

    bool IsEmpty() const {
        return Rows->empty();
    }

    bool IsReachLimits(const TUploadLimits& Limits) {
        return Rows->size() >= Limits.BatchRowsLimit || ByteSize > Limits.BatchBytesLimit;
    }

    void ExtractLastKey(TSerializedCellVec& out) {
        out = std::move(LastKey);
    }

    const TSerializedCellVec& GetLastKey() const {
        return LastKey;
    }

private:
    std::shared_ptr<TRows> Rows;
    ui64 ByteSize = 0;
    TSerializedCellVec LastKey;
};

class TBuildIndexScan : public TActor<TBuildIndexScan>, public NTable::IScan {
    const TUploadLimits Limits;
    const NKikimrIndexBuilder::TColumnBuildSettings ColumnBuildSettings;

    const ui64 BuildIndexId;
    const TString TargetTable;
    const TBuildIndexRecord::TSeqNo SeqNo;

    const ui64 DataShardId;
    const TActorId DatashardActorId;
    const TActorId SchemeShardActorID;

    const TTags ScanTags; // first: columns we scan, order as in IndexTable
    const std::shared_ptr<TTypes> UploadColumnsTypes; // columns types we upload to indexTable
    const ui32 TargetDataColumnPos; // positon of first data column in target table

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
    TStatus UploadStatus;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR;
    }

    TBuildIndexScan(ui64 buildIndexId,
                    const TString& target,
                    const TBuildIndexRecord::TSeqNo& seqNo,
                    ui64 dataShardId,
                    const TActorId& datashardActorId,
                    const TActorId& schemeshardActorId,
                    const TSerializedTableRange& range,
                    const TVector<TString> targetIndexColumns,
                    const TVector<TString> targetDataColumns,
                    NKikimrIndexBuilder::TColumnBuildSettings&& columnsToBuild,
                    TUserTable::TCPtr tableInfo,
                    TUploadLimits limits)
        : TActor(&TThis::StateWork)
        , Limits(limits)
        , ColumnBuildSettings(std::move(columnsToBuild))
        , BuildIndexId(buildIndexId)
        , TargetTable(target)
        , SeqNo(seqNo)
        , DataShardId(dataShardId)
        , DatashardActorId(datashardActorId)
        , SchemeShardActorID(schemeshardActorId)
        , ScanTags(BuildTags(GetAllTags(tableInfo), targetIndexColumns, targetDataColumns))
        , UploadColumnsTypes(BuildTypes(GetAllTypes(tableInfo), tableInfo, ColumnBuildSettings, targetIndexColumns, targetDataColumns))
        , TargetDataColumnPos(targetIndexColumns.size())
        , KeyColumnIds(tableInfo->KeyColumnIds)
        , KeyTypes(tableInfo->KeyColumnTypes)
        , TableRange(tableInfo->Range)
        , RequestedRange(range)
    {
        
    }

    ~TBuildIndexScan() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept override {
        auto selfActorId = TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        auto ctx = TActivationContext::AsActorContext().MakeFor(selfActorId);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Prepared " << Debug());

        Driver = driver;

        return { EScan::Feed, { } };
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Seek no " << seq << " " << Debug());
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

        if (bool(scanRange.From)) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, { }, NTable::ESeek::Lower);
        }

        if (bool(scanRange.To)) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Feed key " << DebugPrintPoint(KeyTypes, key, *AppData()->TypeRegistry)
                                << " " << Debug());

        const TConstArrayRef<TCell> rowCells = *row;


        if (ColumnBuildSettings.columnSize() > 0) {
            TMemoryPool valueDataPool(256);
            TVector<TCell> cells;
            TString err;
            Y_ABORT_UNLESS(BuildExtraColumns(cells, ColumnBuildSettings, err, valueDataPool));
            TSerializedCellVec valueCells(cells);
            TString serializedValue = TSerializedCellVec::Serialize(cells);
            TSerializedCellVec keyCopy(key);
            ReadBuf.AddRow(
                TSerializedCellVec(key),
                std::move(keyCopy),
                std::move(serializedValue));
        } else {
            ReadBuf.AddRow(
                TSerializedCellVec(key),
                TSerializedCellVec(rowCells.Slice(0, TargetDataColumnPos)),
                TSerializedCellVec::Serialize(rowCells.Slice(TargetDataColumnPos)));
        }

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

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

        if (Uploader) {
            TAutoPtr<TEvents::TEvPoisonPill> poison
                = new TEvents::TEvPoisonPill;
            ctx.Send(Uploader, poison.Release());
            Uploader = {};
        }

        TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress
            = new TEvDataShard::TEvBuildIndexProgressResponse;
        progress->Record.SetBuildIndexId(BuildIndexId);
        progress->Record.SetTabletId(DataShardId);
        progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
        progress->Record.SetRequestSeqNoRound(SeqNo.Round);

        if (abort != EAbort::None) {
            progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::ABORTED);
            UploadStatus.Issues.AddIssue(NYql::TIssue("Aborted by scan host env"));

            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                       Debug());
        } else if (!UploadStatus.IsSuccess()) {
            progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::BUILD_ERROR);
        } else {
            progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE);
        }

        UploadStatusToMessage(progress->Record);

        ctx.Send(SchemeShardActorID, progress.Release());;

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Finish " << Debug());

        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    void UploadStatusToMessage(NKikimrTxDataShard::TEvBuildIndexProgressResponse& msg) {
        msg.SetUploadStatus(UploadStatus.StatusCode);
        NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
    }

    void Describe(IOutputStream& out) const noexcept override {
        out << Debug();
    }

    TString Debug() const {
        TStringBuilder result;
        result << "TBuildIndexScan: "
               << ", datashard: " << DataShardId
               << ", requested range: " << DebugPrintRange(KeyTypes, RequestedRange.ToTableRange(), *AppData()->TypeRegistry)
               << ", last acked point: " << DebugPrintPoint(KeyTypes, LastUploadedKey.GetCells(), *AppData()->TypeRegistry)
               << Stats.ToString()
               << UploadStatus.ToString();
        return result;
    }

    EScan PageFault() noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Page fault"
                        << " ReadBuf empty: " << ReadBuf.IsEmpty()
                        << " WriteBuf empty: " << WriteBuf.IsEmpty()
                        << " " << Debug());

        if (ReadBuf.IsEmpty()) {
            return EScan::Feed;
        }

        if (WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload();
        }

        return EScan::Feed;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
                default:
                LOG_ERROR(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                          "TBuildIndexScan: StateWork unexpected event type: %" PRIx32 " event: %s",
                          ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void HandleWakeup(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            RetryUpload();
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Handle TEvUploadRowsResponse"
                        << " " << Debug()
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
            WriteBuf.ExtractLastKey(LastUploadedKey);

            //send progress
            TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress
                = new TEvDataShard::TEvBuildIndexProgressResponse;
            progress->Record.SetBuildIndexId(BuildIndexId);
            progress->Record.SetTabletId(DataShardId);
            progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
            progress->Record.SetRequestSeqNoRound(SeqNo.Round);

            progress->Record.SetLastKeyAck(TSerializedCellVec::Serialize(LastUploadedKey.GetCells()));
            progress->Record.SetRowsDelta(WriteBuf.GetRows());
            progress->Record.SetBytesDelta(WriteBuf.GetBytes());
            WriteBuf.Clear();

            progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::INPROGRESS);
            UploadStatusToMessage(progress->Record);

            ctx.Send(SchemeShardActorID, progress.Release());

            if (!ReadBuf.IsEmpty() && ReadBuf.IsReachLimits(Limits)) {
                ReadBuf.FlushTo(WriteBuf);
                Upload();
            }

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < Limits.MaxUploadRowsRetryCount && UploadStatus.IsRetriable()) {
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                         "Got retriable error, " << Debug());

            ctx.Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                     "Got error, abort scan, " << Debug());

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

        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Upload, last key " << DebugPrintPoint(KeyTypes, WriteBuf.GetLastKey().GetCells(), *AppData()->TypeRegistry)
                                        << " " << Debug());

        auto writeMode = NTxProxy::EUploadRowsMode::WriteToTableShadow;
        if (ColumnBuildSettings.columnSize() > 0) {
            writeMode = NTxProxy::EUploadRowsMode::UpsertIfExists;
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            SelfId(), TargetTable,
            UploadColumnsTypes,
            WriteBuf.GetRowsData(),
            writeMode,
            true /*writeToPrivateTable*/);

        Uploader = TActivationContext::AsActorContext().MakeFor(SelfId()).Register(actor);
    }
};

TAutoPtr<NTable::IScan> CreateBuildIndexScan(
        ui64 buildIndexId,
        TString target,
        const TBuildIndexRecord::TSeqNo& seqNo,
        ui64 dataShardId,
        const TActorId& datashardActorId,
        const TActorId& schemeshardActorId,
        const TSerializedTableRange& range,
        const TVector<TString>& targetIndexColumns,
        const TVector<TString>& targetDataColumns,
        NKikimrIndexBuilder::TColumnBuildSettings&& columnsToBuild,
        TUserTable::TCPtr tableInfo,
        TUploadLimits limits)
{
    return new TBuildIndexScan(
        buildIndexId, target, seqNo, dataShardId, datashardActorId, schemeshardActorId, range, targetIndexColumns, targetDataColumns, std::move(columnsToBuild), tableInfo, limits);
}

class TDataShard::TTxHandleSafeBuildIndexScan : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildIndexScan(TDataShard* self, TEvDataShard::TEvBuildIndexCreateRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

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

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId()))) {
        VolatileTxManager.AttachWaitingSnapshotEvent(
            TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId()),
            std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
    response->Record.SetBuildIndexId(record.GetBuildIndexId());
    response->Record.SetTabletId(TabletID());
    response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::ACCEPTED);

    TBuildIndexRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    auto badRequest = [&] (const TString& error) {
        response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
    };

    const ui64 buildIndexId = record.GetBuildIndexId();
    const ui64 shardId = record.GetTabletId();
    const auto tableId = TTableId(record.GetOwnerId(), record.GetPathId());

    if (shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    if (!GetUserTables().contains(tableId.PathId.LocalPathId)) {
        badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    TUserTable::TCPtr userTable = GetUserTables().at(tableId.PathId.LocalPathId);


    if (BuildIndexManager.Contains(buildIndexId)) {
        TBuildIndexRecord recCard = BuildIndexManager.Get(buildIndexId);
        if (recCard.SeqNo == seqNo) {
            // do no start one more scan
            ctx.Send(ev->Sender, std::move(response));
            return;
        }

        CancelScan(userTable->LocalTid, recCard.ScanId);
    }


    TSerializedTableRange requestedRange;
    requestedRange.Load(record.GetKeyRange());

    auto scanRange = Intersect(userTable->KeyColumnTypes, requestedRange.ToTableRange(), userTable->Range.ToTableRange());

    if (scanRange.IsEmptyRange(userTable->KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
                                    << " requestedRange: " << DebugPrintRange(userTable->KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
                                    << " tableRange: " << DebugPrintRange(userTable->KeyColumnTypes, userTable->Range.ToTableRange(), *AppData()->TypeRegistry)
                                    << " scanRange: " << DebugPrintRange(userTable->KeyColumnTypes, scanRange, *AppData()->TypeRegistry) );
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    const TVector<TString> targetIndexColumns(record.GetIndexColumns().begin(), record.GetIndexColumns().end());
    const TVector<TString> targetDataColumns(record.GetDataColumns().begin(), record.GetDataColumns().end());

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    const TSnapshotKey snapshotKey(tableId.PathId, record.GetSnapshotStep(), record.GetSnapshotTxId());
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " <<tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                   << " , snapshot step is " <<  snapshotKey.Step
                   << " , snapshot tx is " <<  snapshotKey.TxId);
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(TRowVersion(snapshotKey.Step, snapshotKey.TxId));
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

    NKikimrIndexBuilder::TColumnBuildSettings columnsToBuild;
    columnsToBuild.Swap(ev->Get()->Record.MutableColumnBuildSettings());

    const auto scanId = QueueScan(userTable->LocalTid,
                            CreateBuildIndexScan(buildIndexId,
                                                 record.GetTargetName(),
                                                 seqNo,
                                                 shardId,
                                                 ctx.SelfID,
                                                 ev->Sender,
                                                 requestedRange,
                                                 targetIndexColumns,
                                                 targetDataColumns,
                                                 std::move(columnsToBuild),
                                                 userTable,
                                                 limits),
                            ev->Cookie,
                            scanOpts);

    TBuildIndexRecord recCard = {scanId, seqNo};

    BuildIndexManager.Set(buildIndexId, recCard);

    ctx.Send(ev->Sender, std::move(response));
}

}
