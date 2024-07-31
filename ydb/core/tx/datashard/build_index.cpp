#include "build_index.h"

namespace NKikimr::NDataShard {
using EScan = NTable::IScan::EScan;
using TInitialState = NTable::IScan::TInitialState;

void ProtoYdbTypeFromTypeInfo(Ydb::Type* type, const NScheme::TTypeInfo typeInfo) {
    if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
        auto* typeDesc = typeInfo.GetTypeDesc();
        auto* pg = type->mutable_pg_type();
        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
    } else {
        type->set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
    }
}

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TColumnBuildSettings& buildSettings) {
    auto types = GetAllTypes(tableInfo);

    Y_ABORT_UNLESS(buildSettings.columnSize() > 0);
    auto result = std::make_shared<TTypes>();
    result->reserve(tableInfo.KeyColumnIds.size() + buildSettings.columnSize());

    for (const auto& keyColId : tableInfo.KeyColumnIds) {
        auto it = tableInfo.Columns.at(keyColId);
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, it.Type);
        result->emplace_back(it.Name, type);
    }
    for (size_t i = 0; i < buildSettings.columnSize(); i++) {
        const auto& column = buildSettings.column(i);
        result->emplace_back(column.GetColumnName(), column.default_from_literal().type());
    }
    return result;
}

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, TProtoColumnsCRef indexColumns, TProtoColumnsCRef dataColumns) {
    auto types = GetAllTypes(tableInfo);

    auto result = std::make_shared<TTypes>();
    result->reserve(indexColumns.size() + dataColumns.size());

    for (const auto& colName : indexColumns) {
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, types.at(colName));
        result->emplace_back(colName, type);
    }
    for (const auto& colName : dataColumns) {
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, types.at(colName));
        result->emplace_back(colName, type);
    }
    return result;
}

std::shared_ptr<TTypes> BuildTypes(const TUserTable& tableInfo, const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings) {
    auto types = GetAllTypes(tableInfo);

    auto result = std::make_shared<TTypes>();
    result->reserve(checkingNotNullSettings.columnSize() + tableInfo.KeyColumnIds.size());

    for (const auto& keyColId : tableInfo.KeyColumnIds) {
        auto it = tableInfo.Columns.at(keyColId);
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, it.Type);
        result->emplace_back(it.Name, type);
    }

    for (size_t i = 0; i < checkingNotNullSettings.columnSize(); i++) {
        const auto& colName = checkingNotNullSettings.column(i).GetColumnName();
        Ydb::Type type;
        ProtoYdbTypeFromTypeInfo(&type, types.at(colName));
        result->emplace_back(colName, type);
    }

    return result;
}

bool CheckNotNullConstraint(const TConstArrayRef<TCell>& cells) {
    for (const auto& cell : cells) {
        if (cell.IsNull()) {
            return false;
        }
    }

    return true;
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
        if (!CellFromProtoVal(typeInfo, typeMod, &column.default_from_literal().value(), back, err, valueDataPool)) {
            return false;
        }
    }

    return true;
}

TBuildScanUpload::TBuildScanUpload (
    ui64 buildIndexId,
    const TString& target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    const TUserTable& tableInfo,
    TUploadLimits limits
)
    : TActor(&TThis::StateWork)
    , Limits(limits)
    , BuildIndexId(buildIndexId)
    , TargetTable(target)
    , SeqNo(seqNo)
    , DataShardId(dataShardId)
    , ProgressActorId(progressActorId)
    , KeyColumnIds(tableInfo.KeyColumnIds)
    , KeyTypes(tableInfo.KeyColumnTypes)
    , TableRange(tableInfo.Range)
    , RequestedRange(range)
{
}

template <typename TAddRow>
EScan TBuildScanUpload::FeedImpl(TArrayRef<const TCell> key, const TRow& row, TAddRow&& addRow) noexcept {
    LOG_T("Feed key " << DebugPrintPoint(KeyTypes, key, *AppData()->TypeRegistry) << " " << Debug());

    addRow();

    if (CheckingNotNullStatus == ECheckingNotNullStatus::NullFound) {
        return EScan::Final;
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

constexpr NKikimrServices::TActivity::EType TBuildScanUpload::ActorActivityType() {
    return NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR;
}

TInitialState TBuildScanUpload::Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept {
    TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

    LOG_D("Prepare " << Debug());

    Driver = driver;

    return {EScan::Feed, {}};
}

EScan TBuildScanUpload::Seek(TLead& lead, ui64 seq) noexcept {
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

TAutoPtr<IDestructable> TBuildScanUpload::Finish(EAbort abort) noexcept {
    auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

    if (Uploader) {
        TAutoPtr<TEvents::TEvPoisonPill> poison = new TEvents::TEvPoisonPill;
        ctx.Send(Uploader, poison.Release());
        Uploader = {};
    }

    TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
    progress->Record.SetBuildIndexId(BuildIndexId);
    progress->Record.SetTabletId(DataShardId);
    progress->Record.SetRequestSeqNoGeneration(SeqNo.Generation);
    progress->Record.SetRequestSeqNoRound(SeqNo.Round);

    if (abort != EAbort::None) {
        progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::ABORTED);
        UploadStatus.Issues.AddIssue(NYql::TIssue("Aborted by scan host env"));

        LOG_W(Debug());
    } else if (CheckingNotNullStatus != ECheckingNotNullStatus::None) {
        switch (CheckingNotNullStatus) {
            case ECheckingNotNullStatus::NullFound:
                progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::CHECKING_NOT_NULL_ERROR);
                UploadStatus.Issues.AddIssue(NYql::TIssue("Column contains null value, so not-null constraint was not set."));
                break;
            case ECheckingNotNullStatus::Ok:
                progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE);
                break;
            default:
                Y_UNREACHABLE();
        }
    } else if (!UploadStatus.IsSuccess()) {
        progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::BUILD_ERROR);
    } else {
        progress->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::DONE);
    }

    UploadStatusToMessage(progress->Record);

    ctx.Send(ProgressActorId, progress.Release());

    LOG_D("Finish " << Debug());

    Driver = nullptr;
    PassAway();
    return nullptr;
}

void TBuildScanUpload::UploadStatusToMessage(NKikimrTxDataShard::TEvBuildIndexProgressResponse& msg) {
    msg.SetUploadStatus(UploadStatus.StatusCode);
    NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
}

void TBuildScanUpload::Describe(IOutputStream& out) const noexcept {
    out << Debug();
}

TString TBuildScanUpload::Debug() const {
    return TStringBuilder() << "TBuildIndexScan: "
                            << "datashard: " << DataShardId
                            << ", requested range: " << DebugPrintRange(KeyTypes, RequestedRange.ToTableRange(), *AppData()->TypeRegistry)
                            << ", last acked point: " << DebugPrintPoint(KeyTypes, LastUploadedKey.GetCells(), *AppData()->TypeRegistry)
                            << Stats.ToString()
                            << UploadStatus.ToString();
}

EScan TBuildScanUpload::PageFault() noexcept {
    LOG_T("Page fault"
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

void TBuildScanUpload::HandleWakeup(const NActors::TActorContext& ctx) {
    LOG_D("Retry upload " << Debug());

    if (!WriteBuf.IsEmpty()) {
        RetryUpload();
    }
}

void TBuildScanUpload::Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx) {
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
        WriteBuf.ExtractLastKey(LastUploadedKey);

        //send progress
        TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
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

        ctx.Send(ProgressActorId, progress.Release());

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

void TBuildScanUpload::RetryUpload() {
    Upload(true);
}

void TBuildScanUpload::Upload(bool isRetry) {
    if (isRetry) {
        ++RetryCount;
    } else {
        RetryCount = 0;
    }

    LOG_D("Upload, last key " << DebugPrintPoint(KeyTypes, WriteBuf.GetLastKey().GetCells(), *AppData()->TypeRegistry) << " " << Debug());

    auto actor = NTxProxy::CreateUploadRowsInternal(
        SelfId(), TargetTable,
        UploadColumnsTypes,
        WriteBuf.GetRowsData(),
        UploadMode,
        true /*writeToPrivateTable*/);

    Uploader = TActivationContext::AsActorContext().MakeFor(SelfId()).Register(actor);
}

TBuildIndexScan::TBuildIndexScan (
    ui64 buildIndexId,
    const TString& target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    TProtoColumnsCRef targetIndexColumns,
    TProtoColumnsCRef targetDataColumns,
    const TUserTable& tableInfo,
    TUploadLimits limits
)
    : TBuildScanUpload(buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits)
    , TargetDataColumnPos(targetIndexColumns.size())
{
    ScanTags = BuildTags(tableInfo, targetIndexColumns, targetDataColumns);
    UploadColumnsTypes = BuildTypes(tableInfo, targetIndexColumns, targetDataColumns);
    UploadMode = NTxProxy::EUploadRowsMode::WriteToTableShadow;
}

EScan TBuildIndexScan::Feed(TArrayRef<const TCell> key, const TRow& row) noexcept {
    return FeedImpl(key, row, [&] {
        const auto rowCells = *row;

        ReadBuf.AddRow(
            TSerializedCellVec(key),
            TSerializedCellVec(rowCells.Slice(0, TargetDataColumnPos)),
            TSerializedCellVec::Serialize(rowCells.Slice(TargetDataColumnPos)));
    });
}

TBuildColumnsScan::TBuildColumnsScan (
    ui64 buildIndexId,
    const TString& target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    const TUserTable& tableInfo,
    TUploadLimits limits,
    const NKikimrIndexBuilder::TColumnBuildSettings& columnBuildSettings
)
    : TBuildScanUpload(buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits)
{
    Y_ABORT_UNLESS(columnBuildSettings.columnSize() > 0);
    UploadColumnsTypes = BuildTypes(tableInfo, columnBuildSettings);
    UploadMode = NTxProxy::EUploadRowsMode::UpsertIfExists;

    TMemoryPool valueDataPool(256);
    TVector<TCell> cells;
    TString err;
    Y_ABORT_UNLESS(BuildExtraColumns(cells, columnBuildSettings, err, valueDataPool));
    ValueSerialized = TSerializedCellVec::Serialize(cells);
}

EScan TBuildColumnsScan::Feed(TArrayRef<const TCell> key, const TRow& row) noexcept {
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

TCheckColumnScan::TCheckColumnScan (
    ui64 buildIndexId,
    const TString& target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    const TUserTable& tableInfo,
    TUploadLimits limits,
    const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings
)
    : TBuildScanUpload(buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits)
{
    Y_ABORT_UNLESS(checkingNotNullSettings.columnSize() > 0);

    TVector<TString> columnNames;
    for (auto& col : checkingNotNullSettings.Getcolumn()) {
        columnNames.push_back(col.GetColumnName());
    }

    ScanTags = BuildTags(tableInfo, std::move(columnNames));
    UploadColumnsTypes = BuildTypes(tableInfo, checkingNotNullSettings);
    UploadMode = NTxProxy::EUploadRowsMode::Normal;
}

EScan TCheckColumnScan::Feed(TArrayRef<const TCell> key, const TRow& row) noexcept  {
    return FeedImpl(key, row, [&] {
        const TConstArrayRef<TCell> rowCells = *row;

        if (!CheckNotNullConstraint(rowCells)) {
            CheckingNotNullStatus = ECheckingNotNullStatus::NullFound;
        } else {
            CheckingNotNullStatus = ECheckingNotNullStatus::Ok;
        }
    });
}

TAutoPtr<NTable::IScan> CreateBuildIndexScan (
    ui64 buildIndexId,
    TString target,
    const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId,
    const TActorId& progressActorId,
    const TSerializedTableRange& range,
    TProtoColumnsCRef targetIndexColumns,
    TProtoColumnsCRef targetDataColumns,
    const NKikimrIndexBuilder::TColumnBuildSettings& columnsToBuild,
    const NKikimrIndexBuilder::TCheckingNotNullSettings& checkingNotNullSettings,
    const TUserTable& tableInfo,
    TUploadLimits limits
)
{
    if (columnsToBuild.columnSize() > 0) {
        return new TBuildColumnsScan(
            buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits, columnsToBuild
        );
    }

    if (checkingNotNullSettings.columnSize() > 0) {
        return new TCheckColumnScan(
            buildIndexId, target, seqNo, dataShardId, progressActorId, range, tableInfo, limits, checkingNotNullSettings
        );
    }

    return new TBuildIndexScan(
        buildIndexId, target, seqNo, dataShardId, progressActorId, range, targetIndexColumns, targetDataColumns, tableInfo, limits
    );
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

    auto response = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
    response->Record.SetBuildIndexId(record.GetBuildIndexId());
    response->Record.SetTabletId(TabletID());
    response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::ACCEPTED);

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    auto badRequest = [&](const TString& error) {
        response->Record.SetStatus(NKikimrTxDataShard::TEvBuildIndexProgressResponse::BAD_REQUEST);
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
            ctx.Send(ev->Sender, std::move(response));
            return;
        }

        CancelScan(userTable.LocalTid, recCard->ScanId);
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
                                                       record.GetCheckingNotNullSettings(),
                                                       userTable,
                                                       limits),
                                  ev->Cookie,
                                  scanOpts);

    TScanRecord recCard = {scanId, seqNo};

    ScanManager.Set(buildIndexId, recCard);

    ctx.Send(ev->Sender, std::move(response));
}

} // NKikimr::NDataShard
