#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxRunWorker: public TTxBase {
    TEvService::TEvRunWorker::TPtr Event;
    TWorkerId Id;
    bool Register = false;

public:
    TTxRunWorker(TController* self, TEvService::TEvRunWorker::TPtr& ev)
        : TTxBase("TxRunWorker", self)
        , Event(ev)
        , Id(0, 0, 0)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_RUN_WORKER;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Event->Get()->Record;
        Id = TWorkerId::Parse(record.GetWorker());
        if (!Self->IsValidWorker(Id)) {
            return true;
        }

        // A zero heartbeat means "registered but has not reported a heartbeat" and
        // is intentionally ignored by the heartbeat quorum on recovery.
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Workers>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Update(
            NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(0),
            NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(0));
        Register = true;
        return true;
    }

    void Complete(const TActorContext&) override {
        if (!Register) {
            return;
        }

        auto& record = Event->Get()->Record;
        auto* cmd = record.MutableCommand();
        auto* worker = Self->GetOrCreateWorker(Id, cmd);

        if (!worker->HasCommand()) {
            worker->SetCommand(cmd);
        }

        if (!worker->HasSession()) {
            Self->BootQueue.insert(Id);
        }

        Self->ScheduleProcessQueues();
    }
};

void TController::Handle(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX(ctx, "Handle",
        {"ev", ev->Get()->ToString()});

    RunTxRunWorker(ev, ctx);
}

void TController::RunTxRunWorker(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxRunWorker(this, ev), ctx);
}

class TController::TTxRemoveWorker: public TTxBase {
    const TWorkerId Id;
    bool Remove = false;

public:
    TTxRemoveWorker(TController* self, const TWorkerId& id)
        : TTxBase("TxRemoveWorker", self)
        , Id(id)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_REMOVE_WORKER;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->RemoveQueue.contains(Id)) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        // Once any worker leaves, the persisted worker set is no longer
        // complete. Keep schema reports blocked until
        // the next registration pass recreates it.
        db.Table<Schema::Targets>().Key(Id.ReplicationId(), Id.TargetId()).Update(
            NIceDb::TUpdate<Schema::Targets::WorkerSetComplete>(false));
        db.Table<Schema::Workers>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Delete();
        Remove = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Remove || !Self->RemoveQueue.contains(Id)) {
            return;
        }

        Self->RemoveQueue.erase(Id);
        Self->CompleteWorkerSets.erase(std::make_pair(Id.ReplicationId(), Id.TargetId()));

        const auto worker = Self->Workers.find(Id);
        if (worker != Self->Workers.end() && worker->second.HasHeartbeat()) {
            const auto heartbeat = worker->second.GetHeartbeat();
            auto workers = Self->WorkersByHeartbeat.find(heartbeat);
            if (workers != Self->WorkersByHeartbeat.end()) {
                workers->second.erase(Id);
                if (workers->second.empty()) {
                    Self->WorkersByHeartbeat.erase(workers);
                }
            }
        }

        Self->WorkersWithHeartbeat.erase(Id);
        Self->PendingHeartbeats.erase(Id);
        Self->Workers.erase(Id);
        Self->TabletCounters->Simple()[COUNTER_WORKERS] = Self->Workers.size();
        Self->TabletCounters->Simple()[COUNTER_WORKERS_WITH_HEARTBEAT] = Self->WorkersWithHeartbeat.size();
        Self->TabletCounters->Simple()[COUNTER_WORKERS_PENDING_HEARTBEAT] = Self->PendingHeartbeats.size();

        const auto replication = Self->Find(Id.ReplicationId());
        if (!replication) {
            return;
        }

        auto* target = replication->FindTarget(Id.TargetId());
        if (!target) {
            return;
        }

        target->RemoveWorker(Id.WorkerId());
        target->Progress(ctx);
    }
};

void TController::RunTxRemoveWorker(const TWorkerId& id, const TActorContext& ctx) {
    Execute(new TTxRemoveWorker(this, id), ctx);
}

class TController::TTxCompleteWorkerSet: public TTxBase {
    TEvPrivate::TEvCompleteWorkerSet::TPtr Event;

public:
    TTxCompleteWorkerSet(TController* self, TEvPrivate::TEvCompleteWorkerSet::TPtr& ev)
        : TTxBase("TxCompleteWorkerSet", self)
        , Event(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_COMPLETE_WORKER_SET;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = std::make_pair(Event->Get()->ReplicationId, Event->Get()->TargetId);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::WorkerSetComplete>(true));
        return true;
    }

    void Complete(const TActorContext&) override {
        Self->CompleteWorkerSets.insert({Event->Get()->ReplicationId, Event->Get()->TargetId});
    }
};

void TController::Handle(TEvPrivate::TEvCompleteWorkerSet::TPtr& ev, const TActorContext& ctx) {
    RunTxCompleteWorkerSet(ev, ctx);
}

void TController::RunTxCompleteWorkerSet(TEvPrivate::TEvCompleteWorkerSet::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCompleteWorkerSet(this, ev), ctx);
}

}
