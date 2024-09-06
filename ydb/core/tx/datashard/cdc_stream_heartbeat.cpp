#include "cdc_stream_heartbeat.h"
#include "datashard_impl.h"

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[CdcStreamHeartbeat] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[CdcStreamHeartbeat] " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "[CdcStreamHeartbeat] " << stream)

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

        LOG_I("Emit change records"
            << ": edge# " << Edge
            << ", at tablet# " << Self->TabletID());

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
        LOG_I("Enqueue " << ChangeRecords.size() << " change record(s)"
            << ": at tablet# " << Self->TabletID());
        Self->EnqueueChangeRecords(std::move(ChangeRecords));
        Self->EmitHeartbeats();
    }

}; // TTxCdcStreamEmitHeartbeats

void TDataShard::EmitHeartbeats() {
    LOG_D("Emit heartbeats"
        << ": at tablet# " << TabletID());

    if (State != TShardState::Ready) {
        return;
    }

    const auto lowest = CdcStreamHeartbeatManager.LowestVersion();
    if (lowest.IsMax()) {
        return;
    }

    if (const auto& plan = TransQueue.GetPlan()) {
        const auto version = Min(plan.begin()->ToRowVersion(), VolatileTxManager.GetMinUncertainVersion());
        if (CdcStreamHeartbeatManager.ShouldEmitHeartbeat(version)) {
            return Execute(new TTxCdcStreamEmitHeartbeats(this, version));
        }
        return;
    }

    if (auto version = VolatileTxManager.GetMinUncertainVersion(); !version.IsMax()) {
        if (CdcStreamHeartbeatManager.ShouldEmitHeartbeat(version)) {
            return Execute(new TTxCdcStreamEmitHeartbeats(this, version));
        }
        return;
    }

    const TRowVersion nextWrite = GetMvccTxVersion(EMvccTxMode::ReadWrite);
    if (CdcStreamHeartbeatManager.ShouldEmitHeartbeat(nextWrite)) {
        return Execute(new TTxCdcStreamEmitHeartbeats(this, nextWrite));
    }

    WaitPlanStep(lowest.Next().Step);
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

        Y_ABORT_UNLESS(!CdcStreams.contains(streamPathId));
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

    Y_ABORT_UNLESS(!CdcStreams.contains(streamPathId));
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

    if (Schedule.top().Version > edge) {
        return false;
    }

    return true;
}

THashMap<TPathId, TCdcStreamHeartbeatManager::THeartbeatInfo> TCdcStreamHeartbeatManager::EmitHeartbeats(
        NTable::TDatabase& db, const TRowVersion& edge)
{
    if (Schedule.empty() || Schedule.top().Version > edge) {
        return {};
    }

    NIceDb::TNiceDb nicedb(db);
    THashMap<TPathId, THeartbeatInfo> heartbeats;

    while (true) {
        const auto& top = Schedule.top();
        if (top.Version > edge) {
            break;
        }

        auto it = CdcStreams.find(top.StreamPathId);
        Y_ABORT_UNLESS(it != CdcStreams.end());

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
