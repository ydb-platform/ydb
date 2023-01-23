#include "change_record_body_serializer.h"
#include "datashard_impl.h"

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan] " << stream)
#define LOG_I(stream) LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan] " << stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan] " << stream)

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;
using namespace NTabletFlatExecutor;

TCdcStreamScanManager::TCdcStreamScanManager() {
    Clear();
}

void TCdcStreamScanManager::Enqueue(ui64 txId, ui64 scanId, const TPathId& streamPathId) {
    TxId = txId;
    ScanId = scanId;
    StreamPathId = streamPathId;
}

void TCdcStreamScanManager::Register(const TActorId& actorId) {
    ActorId = actorId;
}

void TCdcStreamScanManager::Clear() {
    TxId = 0;
    ScanId = 0;
    ActorId = TActorId();
    StreamPathId = TPathId();
}

void TCdcStreamScanManager::Forget(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId) {
    NIceDb::TNiceDb nicedb(db);
    RemoveLastKey(nicedb, tablePathId, streamPathId);
}

void TCdcStreamScanManager::PersistLastKey(NIceDb::TNiceDb& db, const TSerializedCellVec& value,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Update<Schema::CdcStreamScans::LastKey>(value.GetBuffer());
}

bool TCdcStreamScanManager::LoadLastKey(NIceDb::TNiceDb& db, TMaybe<TSerializedCellVec>& result,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Select();

    if (!rowset.IsReady()) {
        return false;
    }

    if (rowset.IsValid()) {
        result.ConstructInPlace();
        Y_VERIFY(TSerializedCellVec::TryParse(rowset.GetValue<Schema::CdcStreamScans::LastKey>(), *result));
    }

    return true;
}

void TCdcStreamScanManager::RemoveLastKey(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Delete();
}

class TDataShard::TTxCdcStreamScanProgress
    : public TTransactionBase<TDataShard>
    , protected TChangeRecordBodySerializer
{
    TDataShard::TEvPrivate::TEvCdcStreamScanProgress::TPtr Request;
    THolder<TDataShard::TEvPrivate::TEvCdcStreamScanContinue> Response;
    TVector<NMiniKQL::IChangeCollector::TChange> ChangeRecords;

    static TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, TUserTable::TCPtr table) {
        TVector<TRawTypeValue> key(Reserve(cells.size()));

        Y_VERIFY(cells.size() == table->KeyColumnTypes.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            key.emplace_back(cells.at(pos).AsRef(), table->KeyColumnTypes.at(pos));
        }

        return key;
    }

    static TVector<TUpdateOp> MakeUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, TUserTable::TCPtr table) {
        TVector<TUpdateOp> updates(Reserve(cells.size()));

        Y_VERIFY(cells.size() == tags.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            const auto tag = tags.at(pos);
            auto it = table->Columns.find(tag);
            Y_VERIFY(it != table->Columns.end());
            updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type));
        }

        return updates;
    }

    static TRowState MakeRow(TArrayRef<const TCell> cells) {
        TRowState row(cells.size());

        row.Touch(ERowOp::Upsert);
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            row.Set(pos, ECellOp::Set, cells.at(pos));
        }

        return row;
    }

public:
    explicit TTxCdcStreamScanProgress(TDataShard* self, TDataShard::TEvPrivate::TEvCdcStreamScanProgress::TPtr ev)
        : TBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_CDC_STREAM_SCAN_PROGRESS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& ev = *Request->Get();
        const auto& tablePathId = ev.TablePathId;
        const auto& streamPathId = ev.StreamPathId;
        const auto& readVersion = ev.ReadVersion;
        const auto& valueTags = ev.ValueTags;

        LOG_D("Progress"
            << ": streamPathId# " << streamPathId);

        if (Self->CheckChangesQueueOverflow()) {
            return true;
        }

        Y_VERIFY(Self->GetUserTables().contains(tablePathId.LocalPathId));
        auto table = Self->GetUserTables().at(tablePathId.LocalPathId);

        auto it = table->CdcStreams.find(streamPathId);
        Y_VERIFY(it != table->CdcStreams.end());

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [k, v] : ev.Rows) {
            const auto key = MakeKey(k.GetCells(), table);
            const auto& keyTags = table->KeyColumnIds;

            TRowState row(0);
            TSelectStats stats;
            auto ready = txc.DB.Select(table->LocalTid, key, {}, row, stats, 0, readVersion);
            if (ready == EReady::Page) {
                return false;
            }

            if (ready == EReady::Gone || stats.InvisibleRowSkips) {
                continue;
            }

            NKikimrChangeExchange::TChangeRecord::TDataChange body;
            switch (it->second.Mode) {
                case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
                    Serialize(body, ERowOp::Upsert, key, keyTags, {});
                    break;
                case NKikimrSchemeOp::ECdcStreamModeUpdate:
                    Serialize(body, ERowOp::Upsert, key, keyTags, MakeUpdates(v.GetCells(), valueTags, table));
                    break;
                case NKikimrSchemeOp::ECdcStreamModeNewImage:
                case NKikimrSchemeOp::ECdcStreamModeNewAndOldImages: {
                    const auto newImage = MakeRow(v.GetCells());
                    Serialize(body, ERowOp::Upsert, key, keyTags, nullptr, &newImage, valueTags);
                    break;
                }
                case NKikimrSchemeOp::ECdcStreamModeOldImage: {
                    const auto oldImage = MakeRow(v.GetCells());
                    Serialize(body, ERowOp::Upsert, key, keyTags, &oldImage, nullptr, valueTags);
                    break;
                }
                default:
                    Y_FAIL_S("Invalid stream mode: " << static_cast<ui32>(it->second.Mode));
            }

            auto record = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
                .WithOrder(Self->AllocateChangeRecordOrder(db))
                .WithGroup(0)
                .WithStep(readVersion.Step)
                .WithTxId(readVersion.TxId)
                .WithPathId(streamPathId)
                .WithTableId(tablePathId)
                .WithSchemaVersion(table->GetTableSchemaVersion())
                .WithBody(body.SerializeAsString())
                .Build();

            ChangeRecords.push_back(NMiniKQL::IChangeCollector::TChange{
                .Order = record.GetOrder(),
                .Group = record.GetGroup(),
                .Step = record.GetStep(),
                .TxId = record.GetTxId(),
                .PathId = record.GetPathId(),
                .BodySize = record.GetBody().size(),
                .TableId = record.GetTableId(),
                .SchemaVersion = record.GetSchemaVersion(),
            });

            Self->PersistChangeRecord(db, record);
        }

        if (ev.Rows) {
            const auto& [key, _] = ev.Rows.back();
            Self->CdcStreamScanManager.PersistLastKey(db, key, tablePathId, streamPathId);
        }

        Response = MakeHolder<TDataShard::TEvPrivate::TEvCdcStreamScanContinue>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Response) {
            LOG_I("Enqueue " << ChangeRecords.size() << " change record(s)"
                << ": streamPathId# " << Request->Get()->StreamPathId);

            Self->EnqueueChangeRecords(std::move(ChangeRecords));
            ctx.Send(Request->Sender, Response.Release());
        } else {
            LOG_I("Re-run progress tx"
                << ": streamPathId# " << Request->Get()->StreamPathId);

            // re-schedule tx
            Self->Execute(new TDataShard::TTxCdcStreamScanProgress(Self, Request), ctx);
        }
    }

}; // TTxCdcStreamScanProgress

class TCdcStreamScan: public IActorCallback, public IScan {
    struct TDataShardId {
        TActorId ActorId;
        ui64 TabletId;
    };

    struct TLimits {
        ui32 BatchMaxBytes;
        ui32 BatchMinRows;
        ui32 BatchMaxRows;

        TLimits(const NKikimrTxDataShard::TEvCdcStreamScanRequest::TLimits& proto)
            : BatchMaxBytes(proto.GetBatchMaxBytes())
            , BatchMinRows(proto.GetBatchMinRows())
            , BatchMaxRows(proto.GetBatchMaxRows())
        {
        }
    };

    class TBuffer {
    public:
        void AddRow(TArrayRef<const TCell> key, TArrayRef<const TCell> value) {
            const auto& [k, v] = Data.emplace_back(
                TSerializedCellVec(TSerializedCellVec::Serialize(key)),
                TSerializedCellVec(TSerializedCellVec::Serialize(value))
            );
            ByteSize += k.GetBuffer().size() + v.GetBuffer().size();
        }

        auto&& Flush() {
            ByteSize = 0;
            return std::move(Data);
        }

        ui64 Bytes() const {
            return ByteSize;
        }

        ui64 Rows() const {
            return Data.size();
        }

        explicit operator bool() const {
            return !Data.empty();
        }

    private:
        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> Data; // key & value (if any)
        ui64 ByteSize = 0;
    };

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvCdcStreamScanRequest, Handle);
            hFunc(TDataShard::TEvPrivate::TEvCdcStreamScanContinue, Handle);
        }
    }

    void Handle(TEvDataShard::TEvCdcStreamScanRequest::TPtr& ev) {
        ReplyTo = ev->Sender;
        Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::IN_PROGRESS);
    }

    void Handle(TDataShard::TEvPrivate::TEvCdcStreamScanContinue::TPtr&) {
        Driver->Touch(NoMoreData ? EScan::Final : EScan::Feed);
    }

    void Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status, const TString& error = {}) {
        auto response = MakeHolder<TEvDataShard::TEvCdcStreamScanResponse>();

        response->Record.SetTabletId(DataShard.TabletId);
        PathIdFromPathId(TablePathId, response->Record.MutableTablePathId());
        PathIdFromPathId(StreamPathId, response->Record.MutableStreamPathId());
        response->Record.SetStatus(status);
        response->Record.SetErrorDescription(error);

        Send(ReplyTo, std::move(response));
    }

public:
    explicit TCdcStreamScan(const TDataShard* const self, const TActorId& replyTo, ui64 txId,
            const TPathId& tablePathId, const TPathId& streamPathId, const TRowVersion& readVersion,
            const TVector<TTag>& valueTags, const TMaybe<TSerializedCellVec>& lastKey, const TLimits& limits)
        : IActorCallback(static_cast<TReceiveFunc>(&TCdcStreamScan::StateWork), NKikimrServices::TActivity::CDC_STREAM_SCAN_ACTOR)
        , DataShard{self->SelfId(), self->TabletID()}
        , ReplyTo(replyTo)
        , TxId(txId)
        , TablePathId(tablePathId)
        , StreamPathId(streamPathId)
        , ReadVersion(readVersion)
        , ValueTags(valueTags)
        , LastKey(lastKey)
        , Limits(limits)
        , Driver(nullptr)
        , NoMoreData(false)
    {
    }

    void Describe(IOutputStream& o) const noexcept override {
        o << "CdcStreamScan {"
          << " TxId: " << TxId
          << " TablePathId: " << TablePathId
          << " StreamPathId: " << StreamPathId
        << " }";
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);
        Driver = driver;
        Y_VERIFY(!LastKey || LastKey->GetCells().size() == scheme->Tags(true).size());
        return {EScan::Feed, {}};
    }

    void Registered(TActorSystem* sys, const TActorId&) override {
        sys->Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvCdcStreamScanRegistered(TxId, SelfId()));
    }

    EScan Seek(TLead& lead, ui64) noexcept override {
        if (LastKey) {
            lead.To(ValueTags, LastKey->GetCells(), ESeek::Upper);
        } else {
            lead.To(ValueTags, {}, ESeek::Lower);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept override {
        Buffer.AddRow(key, *row);
        if (Buffer.Bytes() < Limits.BatchMaxBytes) {
            if (Buffer.Rows() < Limits.BatchMaxRows) {
                return EScan::Feed;
            }
        } else {
            if (Buffer.Rows() < Limits.BatchMinRows) {
                return EScan::Feed;
            }
        }

        Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvCdcStreamScanProgress(
            TablePathId, StreamPathId, ReadVersion, ValueTags, std::move(Buffer.Flush())
        ));
        return EScan::Sleep;
    }

    EScan Exhausted() noexcept override {
        NoMoreData = true;

        if (!Buffer) {
            return EScan::Final;
        }

        Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvCdcStreamScanProgress(
            TablePathId, StreamPathId, ReadVersion, ValueTags, std::move(Buffer.Flush())
        ));
        return EScan::Sleep;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        if (abort != EAbort::None) {
            Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::ABORTED);
        } else {
            Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE);
        }

        PassAway();
        return nullptr;
    }

private:
    const TDataShardId DataShard;
    TActorId ReplyTo;
    const ui64 TxId;
    const TPathId TablePathId;
    const TPathId StreamPathId;
    const TRowVersion ReadVersion;
    const TVector<TTag> ValueTags;
    const TMaybe<TSerializedCellVec> LastKey;
    const TLimits Limits;

    IDriver* Driver;
    bool NoMoreData;
    TBuffer Buffer;

}; // TCdcStreamScan

class TDataShard::TTxCdcStreamScanRun: public TTransactionBase<TDataShard> {
    TEvDataShard::TEvCdcStreamScanRequest::TPtr Request;
    THolder<IEventHandle> Response; // response to sender or forward to scanner

    THolder<IEventHandle> MakeResponse(const TActorContext& ctx,
            NKikimrTxDataShard::TEvCdcStreamScanResponse::EStatus status, const TString& error = {}) const
    {
        return MakeHolder<IEventHandle>(Request->Sender, ctx.SelfID, new TEvDataShard::TEvCdcStreamScanResponse(
            Request->Get()->Record, Self->TabletID(), status, error
        ));
    }

public:
    explicit TTxCdcStreamScanRun(TDataShard* self, TEvDataShard::TEvCdcStreamScanRequest::TPtr ev)
        : TBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_CDC_STREAM_SCAN_RUN; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;

        LOG_D("Run"
            << ": ev# " << record.ShortDebugString());

        const auto tablePathId = PathIdFromPathId(record.GetTablePathId());
        if (!Self->GetUserTables().contains(tablePathId.LocalPathId)) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::BAD_REQUEST,
                TStringBuilder() << "Unknown table"
                    << ": tablePathId# " << tablePathId);
            return true;
        }

        auto table = Self->GetUserTables().at(tablePathId.LocalPathId);
        if (record.GetTableSchemaVersion() != table->GetTableSchemaVersion()) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::SCHEME_ERROR,
                TStringBuilder() << "Schema version mismatch"
                    << ": tablePathId# " << tablePathId
                    << ", got# " << record.GetTableSchemaVersion()
                    << ", expected# " << table->GetTableSchemaVersion());
            return true;
        }

        const auto streamPathId = PathIdFromPathId(record.GetStreamPathId());
        auto it = table->CdcStreams.find(streamPathId);
        if (it == table->CdcStreams.end()) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::SCHEME_ERROR,
                TStringBuilder() << "Unknown stream"
                    << ": tablePathId# " << tablePathId
                    << ", streamPathId# " << streamPathId);
            return true;
        }

        if (it->second.State != NKikimrSchemeOp::ECdcStreamStateScan) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::SCHEME_ERROR,
                TStringBuilder() << "Unexpected stream state"
                    << ": tablePathId# " << tablePathId
                    << ", streamPathId# " << streamPathId
                    << ", state# " << it->second.State);
            return true;
        }

        if (Self->CdcStreamScanManager.GetScanId()) {
            if (Self->CdcStreamScanManager.GetStreamPathId() == streamPathId) {
                if (const auto& to = Self->CdcStreamScanManager.GetActorId()) {
                    Response = Request->Forward(to);
                } else {
                    // nop, scan actor will report state when it starts
                }
                return true;
            }

            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::OVERLOADED);
            return true;
        }

        if (!record.HasSnapshotStep()) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::BAD_REQUEST,
                "SnapshotStep was not specified");
            return true;
        }

        if (!record.HasSnapshotTxId()) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::BAD_REQUEST,
                "SnapshotTxId was not specified");
            return true;
        }

        const TSnapshotKey snapshotKey(tablePathId, record.GetSnapshotStep(), record.GetSnapshotTxId());
        if (!Self->SnapshotManager.FindAvailable(snapshotKey)) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::BAD_REQUEST,
                TStringBuilder() << "Snapshot was not found"
                    << ": key# " << snapshotKey.ToTuple());
            return true;
        }

        TVector<TTag> valueTags;
        if (it->second.Mode != NKikimrSchemeOp::ECdcStreamModeKeysOnly) {
            valueTags.reserve(table->Columns.size() - 1);
            for (const auto& [tag, column] : table->Columns) {
                if (!column.IsKey) {
                    valueTags.push_back(tag);
                }
            }
        }

        NIceDb::TNiceDb db(txc.DB);
        TMaybe<TSerializedCellVec> lastKey;
        if (!Self->CdcStreamScanManager.LoadLastKey(db, lastKey, tablePathId, streamPathId)) {
            return false;
        }

        auto* appData = AppData(ctx);
        const auto& taskName = appData->DataShardConfig.GetCdcInitialScanTaskName();
        const auto taskPrio = appData->DataShardConfig.GetCdcInitialScanTaskPriority();

        const auto readVersion = TRowVersion(snapshotKey.Step, snapshotKey.TxId);
        const ui64 localTxId = ++Self->NextTieBreakerIndex;
        auto scan = MakeHolder<TCdcStreamScan>(Self, Request->Sender, localTxId,
            tablePathId, streamPathId, readVersion, valueTags, lastKey, record.GetLimits());
        const ui64 scanId = Self->QueueScan(table->LocalTid, scan.Release(), localTxId,
            TScanOptions()
                .SetResourceBroker(taskName, taskPrio)
                .SetSnapshotRowVersion(readVersion)
        );
        Self->CdcStreamScanManager.Enqueue(localTxId, scanId, streamPathId);

        LOG_I("Run scan"
            << ": streamPathId# " << streamPathId);

        Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::ACCEPTED);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Response) {
            ctx.Send(Response.Release());
        }
    }

}; // TTxCdcStreamScanRun

void TDataShard::Handle(TEvDataShard::TEvCdcStreamScanRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCdcStreamScanRun(this, ev), ctx);
}

void TDataShard::Handle(TEvPrivate::TEvCdcStreamScanRegistered::TPtr& ev, const TActorContext& ctx) {
    if (CdcStreamScanManager.GetTxId() != ev->Get()->TxId) {
        LOG_W("Unknown cdc stream scan actor registered"
            << ": at: " << TabletID());
        return;
    }

    CdcStreamScanManager.Register(ev->Get()->ActorId);
}

void TDataShard::Handle(TEvPrivate::TEvCdcStreamScanProgress::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCdcStreamScanProgress(this, ev), ctx);
}

}
