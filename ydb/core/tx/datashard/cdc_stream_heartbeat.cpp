#include "cdc_stream_heartbeat.h"
#include "datashard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr::NDataShard {

static TRowVersion AdjustVersion(const TRowVersion& version, TDuration interval) {
    const auto intervalMs = interval.MilliSeconds();
    const ui64 intervalNo = version.Step / intervalMs;
    return TRowVersion(intervalMs * intervalNo, 0);
}

static TRowVersion NextVersion(const TRowVersion& last, TDuration interval) {
    const auto intervalMs = interval.MilliSeconds();
    const ui64 intervalNo = last.Step / intervalMs;
    return TRowVersion(intervalMs * (intervalNo + 1), 0);
}

class TDataShard::TTxCdcStreamEmitHeartbeats: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    const TRowVersion Edge;
    TVector<IDataShardChangeCollector::TChange> ChangeRecords;

public:
    explicit TTxCdcStreamEmitHeartbeats(TDataShard* self, const TRowVersion& edge)
        : TBase(self)
        , Edge(edge)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_CDC_STREAM_EMIT_HEARTBEATS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Self->State != TShardState::Ready) {
            return true;
        }

        YDB_LOG_INFO("[CdcStreamHeartbeat] Emitting CDC stream heartbeat change records",
            {"edge", Edge},
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);
        const auto heartbeats = Self->GetCdcStreamHeartbeatManager().EmitHeartbeats(txc.DB, Edge);

        for (const auto& [streamPathId, info] : heartbeats) {
            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::CdcHeartbeat)
                .WithOrder(Self->AllocateChangeRecordOrder(db))
                .WithGroup(0)
                .WithStep(info.Last.Step)
                .WithTxId(info.Last.TxId)
                .WithPathId(streamPathId)
                .WithTableId(info.TablePathId)
                .WithSchemaVersion(0) // not used
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

        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_INFO("[CdcStreamHeartbeat] Enqueuing CDC stream heartbeat change records",
            {"changeRecordsCount", ChangeRecords.size()},
            {"tabletId", Self->TabletID()});
        Self->EnqueueChangeRecords(std::move(ChangeRecords));
        Self->EmitHeartbeats();
    }

}; // TTxCdcStreamEmitHeartbeats

void TDataShard::EmitHeartbeats() {
    YDB_LOG_DEBUG("[CdcStreamHeartbeat] Scheduling CDC stream heartbeat emission",
        {"tabletId", TabletID()});

    if (State != TShardState::Ready) {
        return;
    }

    const auto lowest = CdcStreamHeartbeatManager.LowestVersion();
    if (lowest.IsMax()) {
        return;
    }

    // We may possibly have more writes at this version
    TRowVersion edge = GetMvccTxVersion(EMvccTxMode::ReadWrite);
    bool wait = true;

    if (const auto& plan = TransQueue.GetPlan()) {
        edge = Min(edge, plan.begin()->ToRowVersion());
        wait = false;
    }

    if (auto version = VolatileTxManager.GetMinUncertainVersion(); !version.IsMax()) {
        edge = Min(edge, version);
        wait = false;
    }

    if (CdcStreamHeartbeatManager.ShouldEmitHeartbeat(edge)) {
        return Execute(new TTxCdcStreamEmitHeartbeats(this, edge));
    }

    if (wait) {
        WaitPlanStep(lowest.Next().Step);
    }
}

void TCdcStreamHeartbeatManager::Reset() {
    CdcStreams.clear();
    Schedule.clear();
}

bool TCdcStreamHeartbeatManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    auto rowset = db.Table<Schema::CdcStreamHeartbeats>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        const auto tablePathId = TPathId(
            rowset.GetValue<Schema::CdcStreamHeartbeats::TableOwnerId>(),
            rowset.GetValue<Schema::CdcStreamHeartbeats::TablePathId>()
        );
        const auto streamPathId = TPathId(
            rowset.GetValue<Schema::CdcStreamHeartbeats::StreamOwnerId>(),
            rowset.GetValue<Schema::CdcStreamHeartbeats::StreamPathId>()
        );
        const auto interval = TDuration::MilliSeconds(
            rowset.GetValue<Schema::CdcStreamHeartbeats::IntervalMs>()
        );
        const auto last = TRowVersion(
            rowset.GetValue<Schema::CdcStreamHeartbeats::LastStep>(),
            rowset.GetValue<Schema::CdcStreamHeartbeats::LastTxId>()
        );

        Y_ENSURE(!CdcStreams.contains(streamPathId));
        CdcStreams.emplace(streamPathId, THeartbeatInfo{
            .TablePathId = tablePathId,
            .Interval = interval,
            .Last = last,
        });

        Schedule.emplace(streamPathId, NextVersion(last, interval));

        if (!rowset.Next()) {
            return false;
        }
    }

    return true;
}

void TCdcStreamHeartbeatManager::AddCdcStream(NTable::TDatabase& db,
        const TPathId& tablePathId, const TPathId& streamPathId, TDuration heartbeatInterval)
{
    const auto last = TRowVersion::Min();

    Y_ENSURE(!CdcStreams.contains(streamPathId));
    auto res = CdcStreams.emplace(streamPathId, THeartbeatInfo{
        .TablePathId = tablePathId,
        .Interval = heartbeatInterval,
        .Last = last,
    });

    Schedule.emplace(streamPathId, NextVersion(last, heartbeatInterval));

    NIceDb::TNiceDb nicedb(db);
    PersistUpdate(nicedb, tablePathId, streamPathId, res.first->second);
}

void TCdcStreamHeartbeatManager::DropCdcStream(NTable::TDatabase& db,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    auto it = CdcStreams.find(streamPathId);
    if (it == CdcStreams.end()) {
        return;
    }

    CdcStreams.erase(it);
    // rebuild schedule
    Schedule.clear();
    for (const auto& [pathId, info] : CdcStreams) {
        Schedule.emplace(pathId, NextVersion(info.Last, info.Interval));
    }

    NIceDb::TNiceDb nicedb(db);
    PersistRemove(nicedb, tablePathId, streamPathId);
}

TRowVersion TCdcStreamHeartbeatManager::LowestVersion() const {
    if (Schedule.empty()) {
        return TRowVersion::Max();
    }

    return Schedule.top().Version;
}

bool TCdcStreamHeartbeatManager::ShouldEmitHeartbeat(const TRowVersion& edge) const {
    if (Schedule.empty()) {
        return false;
    }

    if (Schedule.top().Version >= edge) {
        return false;
    }

    return true;
}

THashMap<TPathId, TCdcStreamHeartbeatManager::THeartbeatInfo> TCdcStreamHeartbeatManager::EmitHeartbeats(
        NTable::TDatabase& db, const TRowVersion& edge)
{
    if (!ShouldEmitHeartbeat(edge)) {
        return {};
    }

    NIceDb::TNiceDb nicedb(db);
    THashMap<TPathId, THeartbeatInfo> heartbeats;

    while (true) {
        const auto& top = Schedule.top();
        if (top.Version >= edge) {
            break;
        }

        auto it = CdcStreams.find(top.StreamPathId);
        Y_ENSURE(it != CdcStreams.end());

        const auto& streamPathId = it->first;
        auto& info = it->second;

        info.Last = AdjustVersion(edge, info.Interval);
        PersistUpdate(nicedb, info.TablePathId, streamPathId, info);

        Schedule.pop();
        Schedule.emplace(streamPathId, NextVersion(info.Last, info.Interval));

        heartbeats[streamPathId] = info;
    }

    return heartbeats;
}

void TCdcStreamHeartbeatManager::PersistUpdate(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId, const THeartbeatInfo& info)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamHeartbeats>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::CdcStreamHeartbeats::IntervalMs>(info.Interval.MilliSeconds()),
            NIceDb::TUpdate<Schema::CdcStreamHeartbeats::LastStep>(info.Last.Step),
            NIceDb::TUpdate<Schema::CdcStreamHeartbeats::LastTxId>(info.Last.TxId)
        );
}

void TCdcStreamHeartbeatManager::PersistRemove(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::CdcStreamHeartbeats>()
        .Key(tablePathId.OwnerId, tablePathId.LocalPathId, streamPathId.OwnerId, streamPathId.LocalPathId)
        .Delete();
}

}
