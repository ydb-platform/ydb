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
        // Registration is part of the schema-barrier durability protocol.
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Event->Get()->Record;
        Id = TWorkerId::Parse(record.GetWorker());
        if (!Self->IsValidWorker(Id)) {
            return true;
        }

        // Persist a roster entry before the worker can be booted. A zero
        // heartbeat means "registered but has not reported a heartbeat" and
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
    TWorkerId Id;
    bool Remove = false;

public:
    TTxRemoveWorker(TController* self, const TWorkerId& id)
        : TTxBase("TxRemoveWorker", self)
        , Id(id)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!Self->RemoveQueue.contains(Id)) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        // Once any worker leaves, the persisted roster is no longer a
        // complete membership snapshot. Keep schema reports blocked until
        // the next registration pass recreates it.
        db.Table<Schema::WorkerSnapshots>().Key(Id.ReplicationId(), Id.TargetId()).Delete();
        db.Table<Schema::Workers>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Delete();
        Remove = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Remove || !Self->RemoveQueue.contains(Id)) {
            return;
        }

        Self->RemoveQueue.erase(Id);
        Self->WorkerSnapshots.erase(std::make_pair(Id.ReplicationId(), Id.TargetId()));

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

class TController::TTxWorkersRegistered: public TTxBase {
    TEvPrivate::TEvWorkersRegistered::TPtr Event;

public:
    TTxWorkersRegistered(TController* self, TEvPrivate::TEvWorkersRegistered::TPtr& ev)
        : TTxBase("TxWorkersRegistered", self)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEMA_CHANGE_REPORT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = std::make_pair(Event->Get()->ReplicationId, Event->Get()->TargetId);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::WorkerSnapshots>().Key(key.first, key.second).Update();
        return true;
    }

    void Complete(const TActorContext&) override {
        Self->WorkerSnapshots.insert({Event->Get()->ReplicationId, Event->Get()->TargetId});
    }
};

void TController::Handle(TEvPrivate::TEvWorkersRegistered::TPtr& ev, const TActorContext& ctx) {
    RunTxWorkersRegistered(ev, ctx);
}

void TController::RunTxWorkersRegistered(TEvPrivate::TEvWorkersRegistered::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWorkersRegistered(this, ev), ctx);
}

}
