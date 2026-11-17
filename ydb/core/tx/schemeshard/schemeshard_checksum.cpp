#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr::NSchemeShard {

struct TSchemeShard::TTxChecksumAdmission: NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvSchemeShard::TEvChecksumAdmission::TPtr Event;
    TVector<std::tuple<TActorId, ui64, NKikimrReplication::TEvChecksumAdmissionResult>> Replies;

    TTxChecksumAdmission(TSchemeShard* self, TEvSchemeShard::TEvChecksumAdmission::TPtr event = {})
        : TTransactionBase(self)
        , Event(std::move(event))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CHECKSUM_ADMISSION;
    }

    void Reply(const TChecksumAdmissionKey& key, const NKikimrReplication::TChecksumAdmissionState& state,
        Ydb::StatusIds::StatusCode status, const TString& issue = {})
    {
        const auto waiter = Self->ChecksumAdmissionWaiters.find(key);
        if (waiter == Self->ChecksumAdmissionWaiters.end()) {
            return;
        }
        NKikimrReplication::TEvChecksumAdmissionResult result;
        result.SetRequestId(key.second);
        *result.MutableDestinationPathId() = state.GetRequest().GetDestinationPathId();
        result.SetStatus(status);
        result.SetIssue(issue);
        result.SetReleased(Event && Event->Get()->Record.GetRelease()
            && Event->Get()->Record.GetControllerId() == key.first
            && Event->Get()->Record.GetRequestId() == key.second);
        result.SetGrantedShards(state.GetGrantedShards());
        result.SetDeadlineUs(state.GetDeadlineUs());
        Replies.emplace_back(waiter->second.first, waiter->second.second, std::move(result));
        Self->ChecksumAdmissionWaiters.erase(waiter);
    }

    void Save(NIceDb::TNiceDb& db, const TChecksumAdmissionKey& key,
        const NKikimrReplication::TChecksumAdmissionState& state)
    {
        db.Table<Schema::ChecksumAdmissions>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::ChecksumAdmissions::State>(state.SerializeAsString()));
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        const auto now = ctx.Now().MicroSeconds();
        const auto retention = TDuration::Hours(1).MicroSeconds();
        if (Event) {
            const auto& request = Event->Get()->Record;
            const auto key = std::make_pair(request.GetControllerId(), request.GetRequestId());
            Self->ChecksumAdmissionWaiters[key] = {Event->Sender, Event->Cookie};
            auto [it, inserted] = Self->ChecksumAdmissions.try_emplace(key);
            auto& state = it->second;
            if (inserted) {
                *state.MutableRequest() = request;
                state.SetOrder(++Self->NextChecksumAdmissionOrder);
            }
            if (TPathId::FromProto(request.GetDestinationPathId()) != TPathId::FromProto(state.GetRequest().GetDestinationPathId())) {
                Reply(key, state, Ydb::StatusIds::BAD_REQUEST, "Admission request ID was reused for another table");
            } else if (request.GetRelease()) {
                // Keep a tombstone so delayed enqueue messages cannot resurrect a reservation.
                state.MutableRequest()->SetRelease(true);
                state.SetReleased(true);
                state.SetGrantedShards(0);
                state.SetDeadlineUs(now + retention);
                Save(db, key, state);
                Reply(key, state, Ydb::StatusIds::SUCCESS);
            } else if (state.GetReleased()) {
                Reply(key, state, Ydb::StatusIds::TIMEOUT, "Checksum admission has expired or was released");
            } else {
                Save(db, key, state);
                if (state.GetGrantedShards()) {
                    Reply(key, state, Ydb::StatusIds::SUCCESS);
                }
            }
        }

        THashMap<TPathId, ui64> reserved;
        TMap<ui64, TChecksumAdmissionKey> pending;
        for (auto it = Self->ChecksumAdmissions.begin(); it != Self->ChecksumAdmissions.end();) {
            const auto& key = it->first;
            auto& state = it->second;
            if (state.GetReleased()) {
                if (now >= state.GetDeadlineUs()) {
                    db.Table<Schema::ChecksumAdmissions>().Key(key.first, key.second).Delete();
                    it = Self->ChecksumAdmissions.erase(it);
                    continue;
                }
            } else if (state.GetGrantedShards()) {
                if (now >= state.GetDeadlineUs()) {
                    state.SetGrantedShards(0);
                    state.SetReleased(true);
                    state.SetDeadlineUs(now + retention);
                    Save(db, key, state);
                    Reply(key, state, Ydb::StatusIds::TIMEOUT, "Checksum execution deadline expired");
                } else {
                    reserved[TPathId::FromProto(state.GetDatabasePathId())] += state.GetGrantedShards();
                }
            } else {
                pending.emplace(state.GetOrder(), key);
            }
            ++it;
        }

        THashSet<TPathId> blocked;
        const ui64 budget = Self->MaxReplicationChecksumShardsInFlight;
        for (const auto& [_, key] : pending) {
            auto& state = Self->ChecksumAdmissions.at(key);
            const auto pathId = TPathId::FromProto(state.GetRequest().GetDestinationPathId());
            const auto table = Self->Tables.find(pathId);
            const auto path = Self->PathsById.find(pathId);
            if (table == Self->Tables.end() || path == Self->PathsById.end() || pathId.OwnerId != Self->TabletID()) {
                state.SetReleased(true);
                state.SetDeadlineUs(now + retention);
                Save(db, key, state);
                Reply(key, state, Ydb::StatusIds::SCHEME_ERROR, "Checksum destination table does not exist");
                continue;
            }
            const auto database = path->second->DomainPathId;
            if (blocked.contains(database) || !budget || !AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
                continue;
            }
            const ui64 reservation = Min<ui64>(Max<ui64>(1, table->second->GetPartitions().size()), budget);
            if (reserved[database] + reservation > budget) {
                blocked.insert(database);
                continue;
            }
            database.ToProto(state.MutableDatabasePathId());
            state.SetGrantedShards(reservation);
            state.SetDeadlineUs(now + TDuration::Hours(1).MicroSeconds());
            reserved[database] += reservation;
            Save(db, key, state);
            Reply(key, state, Ydb::StatusIds::SUCCESS);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        for (const auto& [actor, cookie, record] : Replies) {
            auto reply = MakeHolder<TEvSchemeShard::TEvChecksumAdmissionResult>();
            reply->Record = record;
            ctx.Send(actor, reply.Release(), 0, cookie);
        }
        if (!Self->ChecksumAdmissionWakeupScheduled && !Self->ChecksumAdmissions.empty()) {
            Self->ChecksumAdmissionWakeupScheduled = true;
            ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvProgressChecksumAdmission());
        }
    }
};

void TSchemeShard::Handle(TEvSchemeShard::TEvChecksumAdmission::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxChecksumAdmission(this, ev), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvProgressChecksumAdmission::TPtr&, const TActorContext& ctx) {
    ChecksumAdmissionWakeupScheduled = false;
    Execute(new TTxChecksumAdmission(this), ctx);
}

} // namespace NKikimr::NSchemeShard
