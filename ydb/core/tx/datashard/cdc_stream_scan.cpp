#include "cdc_stream_scan.h"
#include "change_record_body_serializer.h"
#include "datashard_impl.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan][" << TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan][" << TabletID() << "] " << stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, "[CdcStreamScan][" << TabletID() << "] " << stream)

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;
using namespace NTabletFlatExecutor;

void TCdcStreamScanManager::TStats::Serialize(NKikimrTxDataShard::TEvCdcStreamScanResponse_TStats& proto) const {
    proto.SetRowsProcessed(RowsProcessed);
    proto.SetBytesProcessed(BytesProcessed);
}

void TCdcStreamScanManager::Reset() {
    Scans.clear();
    TxIdToPathId.clear();
}

bool TCdcStreamScanManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::CdcStreamScans>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const auto streamPathId = TPathId(
            rowset.GetValue<Schema::CdcStreamScans::StreamOwnerId>(),
            rowset.GetValue<Schema::CdcStreamScans::StreamPathId>()
        );

        Y_ABORT_UNLESS(!Scans.contains(streamPathId));
        auto& info = Scans[streamPathId];

        info.SnapshotVersion = TRowVersion(
            rowset.GetValue<Schema::CdcStreamScans::SnapshotStep>(),
            rowset.GetValue<Schema::CdcStreamScans::SnapshotTxId>()
        );

        if (rowset.HaveValue<Schema::CdcStreamScans::LastKey>()) {
            info.LastKey.ConstructInPlace();
            Y_ABORT_UNLESS(TSerializedCellVec::TryParse(rowset.GetValue<Schema::CdcStreamScans::LastKey>(), *info.LastKey));
        }

        info.Stats.RowsProcessed = rowset.GetValueOrDefault<Schema::CdcStreamScans::RowsProcessed>(0);
        info.Stats.BytesProcessed = rowset.GetValueOrDefault<Schema::CdcStreamScans::BytesProcessed>(0);

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

void TCdcStreamScanManager::Add(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId,
        const TRowVersion& snapshotVersion)
{
    Y_ABORT_UNLESS(!Scans.contains(streamPathId));
    auto& info = Scans[streamPathId];
    info.SnapshotVersion = snapshotVersion;

    NIceDb::TNiceDb nicedb(db);
    PersistAdd(nicedb, tablePathId, streamPathId, info);
}

void TCdcStreamScanManager::Forget(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId) {
    NIceDb::TNiceDb nicedb(db);
    PersistRemove(nicedb, tablePathId, streamPathId);
}

void TCdcStreamScanManager::Enqueue(const TPathId& streamPathId, ui64 txId, ui64 scanId) {
    Y_ABORT_UNLESS(Scans.contains(streamPathId));
    auto& info = Scans.at(streamPathId);
    info.TxId = txId;
    info.ScanId = scanId;
    TxIdToPathId[txId] = streamPathId;
}

void TCdcStreamScanManager::Register(ui64 txId, const TActorId& actorId) {
    Y_ABORT_UNLESS(TxIdToPathId.contains(txId));
    Scans[TxIdToPathId.at(txId)].ActorId = actorId;
}

void TCdcStreamScanManager::Complete(const TPathId& streamPathId) {
    auto it = Scans.find(streamPathId);
    if (it == Scans.end()) {
        return;
    }

    CompletedScans[streamPathId] = it->second.Stats;
    TxIdToPathId.erase(it->second.TxId);
    Scans.erase(it);
}

void TCdcStreamScanManager::Complete(ui64 txId) {
    Y_ABORT_UNLESS(TxIdToPathId.contains(txId));
    Complete(TxIdToPathId.at(txId));
}

bool TCdcStreamScanManager::IsCompleted(const TPathId& streamPathId) const {
    return CompletedScans.contains(streamPathId);
}

const TCdcStreamScanManager::TStats& TCdcStreamScanManager::GetCompletedStats(const TPathId& streamPathId) const {
    Y_ABORT_UNLESS(CompletedScans.contains(streamPathId));
    return CompletedScans.at(streamPathId);
}

TCdcStreamScanManager::TScanInfo* TCdcStreamScanManager::Get(const TPathId& streamPathId) {
    return Scans.FindPtr(streamPathId);
}

const TCdcStreamScanManager::TScanInfo* TCdcStreamScanManager::Get(const TPathId& streamPathId) const {
    return Scans.FindPtr(streamPathId);
}

bool TCdcStreamScanManager::Has(const TPathId& streamPathId) const {
    return Scans.contains(streamPathId);
}

bool TCdcStreamScanManager::Has(ui64 txId) const {
    return TxIdToPathId.contains(txId);
}

ui32 TCdcStreamScanManager::Size() const {
    return Scans.size();
}

void TCdcStreamScanManager::PersistAdd(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId, const TScanInfo& info)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::CdcStreamScans::SnapshotStep>(info.SnapshotVersion.Step),
            NIceDb::TUpdate<Schema::CdcStreamScans::SnapshotTxId>(info.SnapshotVersion.TxId)
        );
}

void TCdcStreamScanManager::PersistRemove(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Delete();
}

void TCdcStreamScanManager::PersistProgress(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId, const TScanInfo& info)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamScans>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::CdcStreamScans::LastKey>(info.LastKey->GetBuffer()),
            NIceDb::TUpdate<Schema::CdcStreamScans::RowsProcessed>(info.Stats.RowsProcessed),
            NIceDb::TUpdate<Schema::CdcStreamScans::BytesProcessed>(info.Stats.BytesProcessed)
        );
}

class TDataShard::TTxCdcStreamScanProgress
    : public TTransactionBase<TDataShard>
    , protected TChangeRecordBodySerializer
{
    TDataShard::TEvPrivate::TEvCdcStreamScanProgress::TPtr Request;
    THolder<TDataShard::TEvPrivate::TEvCdcStreamScanContinue> Response;
    TVector<IDataShardChangeCollector::TChange> ChangeRecords;
    bool Reschedule = false;

    static TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, TUserTable::TCPtr table) {
        TVector<TRawTypeValue> key(Reserve(cells.size()));

        Y_ABORT_UNLESS(cells.size() == table->KeyColumnTypes.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            key.emplace_back(cells.at(pos).AsRef(), table->KeyColumnTypes.at(pos));
        }

        return key;
    }

    static TVector<TUpdateOp> MakeUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, TUserTable::TCPtr table) {
        TVector<TUpdateOp> updates(Reserve(cells.size()));

        Y_ABORT_UNLESS(cells.size() == tags.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            const auto tag = tags.at(pos);
            auto it = table->Columns.find(tag);
            Y_ABORT_UNLESS(it != table->Columns.end());
            updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type));
        }

        return updates;
    }

    static std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, TUserTable::TCPtr table) {
        Y_ABORT_UNLESS(cells.size() >= 1);
        TVector<TUpdateOp> updates(::Reserve(cells.size() - 1));

        bool foundSpecialColumn = false;
        Y_ABORT_UNLESS(cells.size() == tags.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            const auto tag = tags.at(pos);
            auto it = table->Columns.find(tag);
            Y_ABORT_UNLESS(it != table->Columns.end());
            if (it->second.Name == "__ydb_incrBackupImpl_deleted") {
                if (const auto& cell = cells.at(pos); !cell.IsNull() && cell.AsValue<bool>()) {
                    return std::nullopt;
                }
                foundSpecialColumn = true;
                continue;
            }
            updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type));
        }
        Y_ABORT_UNLESS(foundSpecialColumn);

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

    ui64 TabletID() const {
        return Self->TabletID();
    }

public:
    explicit TTxCdcStreamScanProgress(TDataShard* self, TDataShard::TEvPrivate::TEvCdcStreamScanProgress::TPtr ev)
        : TBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_CDC_STREAM_SCAN_PROGRESS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& ev = *Request->Get();
        const auto& tablePathId = ev.TablePathId;
        const auto& streamPathId = ev.StreamPathId;
        const auto& readVersion = ev.ReadVersion;
        const auto& valueTags = ev.ValueTags;

        LOG_D("Progress"
            << ": streamPathId# " << streamPathId);

        if (!Self->GetUserTables().contains(tablePathId.LocalPathId)) {
            LOG_W("Cannot progress on unknown table"
                << ": tablePathId# " << tablePathId);
            return true;
        }

        auto table = Self->GetUserTables().at(tablePathId.LocalPathId);

        auto it = table->CdcStreams.find(streamPathId);
        if (it == table->CdcStreams.end()) {
            LOG_W("Cannot progress on unknown cdc stream"
                << ": streamPathId# " << streamPathId);
            return true;
        }

        ChangeRecords.clear();

        if (!ev.ReservationCookie) {
            ev.ReservationCookie = Self->ReserveChangeQueueCapacity(ev.Rows.size());
        }

        if (!ev.ReservationCookie) {
            LOG_I("Cannot reserve change queue capacity");
            Reschedule = true;
            return true;
        }

        if (Self->GetFreeChangeQueueCapacity(ev.ReservationCookie) < ev.Rows.size()) {
            LOG_I("Not enough change queue capacity");
            Reschedule = true;
            return true;
        }

        if (Self->CheckChangesQueueOverflow(ev.ReservationCookie)) {
            LOG_I("Change queue overflow");
            Reschedule = true;
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        bool pageFault = false;

        for (const auto& [k, v] : ev.Rows) {
            const auto key = MakeKey(k.GetCells(), table);
            const auto& keyTags = table->KeyColumnIds;

            TRowState row(0);
            TSelectStats stats;
            auto ready = txc.DB.Select(table->LocalTid, key, {}, row, stats, 0, readVersion);
            if (ready == EReady::Page) {
                pageFault = true;
            }

            if (pageFault || ready == EReady::Gone || stats.InvisibleRowSkips) {
                continue;
            }

            NKikimrChangeExchange::TDataChange body;
            switch (it->second.Mode) {
                case NKikimrSchemeOp::ECdcStreamModeKeysOnly:
                    Serialize(body, ERowOp::Upsert, key, keyTags, {});
                    break;
                case NKikimrSchemeOp::ECdcStreamModeUpdate:
                    Serialize(body, ERowOp::Upsert, key, keyTags, MakeUpdates(v.GetCells(), valueTags, table));
                    break;
                case NKikimrSchemeOp::ECdcStreamModeRestoreIncrBackup:
                    if (auto updates = MakeRestoreUpdates(v.GetCells(), valueTags, table); updates) {
                        Serialize(body, ERowOp::Upsert, key, keyTags, *updates);
                    } else {
                        Serialize(body, ERowOp::Erase, key, keyTags, {});
                    }
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

            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::CdcDataChange)
                .WithOrder(Self->AllocateChangeRecordOrder(db))
                .WithGroup(0)
                .WithStep(readVersion.Step)
                .WithTxId(readVersion.TxId)
                .WithPathId(streamPathId)
                .WithTableId(tablePathId)
                .WithSchemaVersion(table->GetTableSchemaVersion())
                .WithBody(body.SerializeAsString())
                .WithSource(TChangeRecord::ESource::InitialScan)
                .Build();

            const auto& record = *recordPtr;
            Self->PersistChangeRecord(db, record);

            ChangeRecords.push_back(IDataShardChangeCollector::TChange{
                .Order = record.GetOrder(),
                .Group = record.GetGroup(),
                .Step = record.GetStep(),
                .TxId = record.GetTxId(),
                .PathId = record.GetPathId(),
                .BodySize = record.GetBody().size(),
                .TableId = record.GetTableId(),
                .SchemaVersion = record.GetSchemaVersion(),
            });
        }

        if (pageFault) {
            return false;
        }

        if (ev.Rows) {
            const auto& [key, _] = ev.Rows.back();

            auto* info = Self->CdcStreamScanManager.Get(streamPathId);
            Y_ABORT_UNLESS(info);

            info->LastKey = key;
            info->Stats = ev.Stats;
            Self->CdcStreamScanManager.PersistProgress(db, tablePathId, streamPathId, *info);
        }

        Response = MakeHolder<TDataShard::TEvPrivate::TEvCdcStreamScanContinue>();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Response) {
            LOG_I("Enqueue " << ChangeRecords.size() << " change record(s)"
                << ": streamPathId# " << Request->Get()->StreamPathId);

            Self->EnqueueChangeRecords(std::move(ChangeRecords), Request->Get()->ReservationCookie);
            ctx.Send(Request->Sender, Response.Release());
        } else if (Reschedule) {
            LOG_I("Re-schedule progress tx"
                << ": streamPathId# " << Request->Get()->StreamPathId);

            // re-schedule tx
            ctx.TActivationContext::Schedule(TDuration::Seconds(1), Request->Forward(ctx.SelfID));
        }
    }

}; // TTxCdcStreamScanProgress

class TCdcStreamScan: public IActorCallback, public IScan {
    using TStats = TCdcStreamScanManager::TStats;

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
                TSerializedCellVec(key),
                TSerializedCellVec(value)
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

    void Progress() {
        Stats.RowsProcessed += Buffer.Rows();
        Stats.BytesProcessed += Buffer.Bytes();

        Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvCdcStreamScanProgress(
            TablePathId, StreamPathId, ReadVersion, ValueTags, std::move(Buffer.Flush()), Stats
        ));
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
        Stats.Serialize(*response->Record.MutableStats());

        Send(ReplyTo, std::move(response));
    }

public:
    explicit TCdcStreamScan(const TDataShard* const self, const TActorId& replyTo, ui64 txId,
            const TPathId& tablePathId, const TPathId& streamPathId, const TRowVersion& readVersion,
            const TVector<TTag>& valueTags, const TMaybe<TSerializedCellVec>& lastKey,
            const TStats& stats, const TLimits& limits)
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
        , Stats(stats)
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
        Y_ABORT_UNLESS(!LastKey || LastKey->GetCells().size() == scheme->Tags(true).size());
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

        Progress();
        return EScan::Sleep;
    }

    EScan Exhausted() noexcept override {
        NoMoreData = true;

        if (!Buffer) {
            return EScan::Final;
        }

        Progress();
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
    TStats Stats;

}; // TCdcStreamScan

class TDataShard::TTxCdcStreamScanRun: public TTransactionBase<TDataShard> {
    TEvDataShard::TEvCdcStreamScanRequest::TPtr Request;
    THolder<IEventHandle> Response; // response to sender or forward to scanner

    template <typename... Args>
    THolder<IEventHandle> MakeResponse(const TActorContext& ctx, Args&&... args) const {
        return MakeHolder<IEventHandle>(Request->Sender, ctx.SelfID, new TEvDataShard::TEvCdcStreamScanResponse(
            Request->Get()->Record, Self->TabletID(), std::forward<Args>(args)...
        ));
    }

    ui64 TabletID() const {
        return Self->TabletID();
    }

public:
    explicit TTxCdcStreamScanRun(TDataShard* self, TEvDataShard::TEvCdcStreamScanRequest::TPtr ev)
        : TBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_CDC_STREAM_SCAN_RUN; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
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

        if (const auto* info = Self->CdcStreamScanManager.Get(streamPathId)) {
            if (const auto& to = info->ActorId) {
                Response = Request->Forward(to);
                return true;
            } else if (info->ScanId) {
                return true; // nop, scan actor will report state when it starts
            }
        } else if (Self->CdcStreamScanManager.IsCompleted(streamPathId)) {
            Response = MakeResponse(ctx, NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE);
            Self->CdcStreamScanManager.GetCompletedStats(streamPathId).Serialize(
                *Response->Get<TEvDataShard::TEvCdcStreamScanResponse>()->Record.MutableStats());
            return true;
        } else if (Self->CdcStreamScanManager.Size()) {
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

        const auto* info = Self->CdcStreamScanManager.Get(streamPathId);
        Y_ABORT_UNLESS(info);

        auto* appData = AppData(ctx);
        const auto& taskName = appData->DataShardConfig.GetCdcInitialScanTaskName();
        const auto taskPrio = appData->DataShardConfig.GetCdcInitialScanTaskPriority();

        const auto snapshotVersion = TRowVersion(snapshotKey.Step, snapshotKey.TxId);
        Y_ABORT_UNLESS(info->SnapshotVersion == snapshotVersion);

        // Note: cdc stream is added with a schema transaction and those wait for volatile txs
        Y_ABORT_UNLESS(!Self->GetVolatileTxManager().HasVolatileTxsAtSnapshot(snapshotVersion));

        const ui64 localTxId = Self->NextTieBreakerIndex++;
        auto scan = MakeHolder<TCdcStreamScan>(Self, Request->Sender, localTxId,
            tablePathId, streamPathId, snapshotVersion, valueTags, info->LastKey, info->Stats, record.GetLimits());
        const ui64 scanId = Self->QueueScan(table->LocalTid, scan.Release(), localTxId,
            TScanOptions()
                .SetResourceBroker(taskName, taskPrio)
                .SetSnapshotRowVersion(snapshotVersion)
        );
        Self->CdcStreamScanManager.Enqueue(streamPathId, localTxId, scanId);

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
    if (!CdcStreamScanManager.Has(ev->Get()->TxId)) {
        LOG_W("Unknown cdc stream scan actor registered");
        return;
    }

    CdcStreamScanManager.Register(ev->Get()->TxId, ev->Get()->ActorId);
}

void TDataShard::Handle(TEvPrivate::TEvCdcStreamScanProgress::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCdcStreamScanProgress(this, ev), ctx);
}

}
