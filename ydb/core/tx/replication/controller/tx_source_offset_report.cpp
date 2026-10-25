#include "controller_impl.h"

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

bool TController::TracksSourceOffsets(const TWorkerId& id) const {
    const auto replication = Find(id.ReplicationId());
    const auto* target = FindTarget(id);
    return replication && target
        && replication->GetConfig().GetConsistencySettings().HasGlobal()
        && target->GetKind() != TReplication::ETargetKind::Transfer;
}

bool TController::HasSourceOffsetMismatchError(const TReplication& replication) const {
    return replication.GetState() == TReplication::EState::Error
        && ReplicationsWithSourceOffsetMismatch.contains(replication.GetId());
}

bool TController::IsStoppedAfterSourceOffsetMismatch(ui64 replicationId) const {
    return ReplicationsWithSourceOffsetMismatch.contains(replicationId)
        && !DrainDeadlines.contains(replicationId);
}

bool TController::IsMismatchBlockingCommit(ui64 replicationId, TInstant now) const {
    if (!ReplicationsWithSourceOffsetMismatch.contains(replicationId)) {
        return false;
    }
    const auto* deadline = DrainDeadlines.FindPtr(replicationId);
    return !deadline || now >= *deadline;
}

void TController::DeleteProcessedSourceOffsets(ui64 replicationId, ui64 targetId, NIceDb::TNiceDb& db) {
    for (auto it = ProcessedSourceOffsets.begin(); it != ProcessedSourceOffsets.end();) {
        const auto& id = it->first;
        if (id.ReplicationId() == replicationId && id.TargetId() == targetId) {
            db.Table<Schema::ProcessedSourceOffsets>().Key(replicationId, targetId, id.WorkerId()).Delete();
            ProcessedSourceOffsets.erase(it++);
        } else {
            ++it;
        }
    }
}

void TController::SaveHeartbeat(const TWorkerId& id, const TRowVersion& version, NIceDb::TNiceDb& db) {
    auto* worker = Workers.FindPtr(id);
    AFL_VERIFY(worker)("worker", id);
    if (!IsHeartbeatParticipant(id)) {
        return;
    }
    if (worker->HasHeartbeat()) {
        const auto previous = worker->GetHeartbeat();
        if (version <= previous) {
            return;
        }
        if (auto bucket = WorkersByHeartbeat.find(previous); bucket != WorkersByHeartbeat.end()) {
            bucket->second.erase(id);
            if (bucket->second.empty()) {
                WorkersByHeartbeat.erase(bucket);
            }
        }
    }
    worker->SetHeartbeat(version);
    WorkersWithHeartbeat.insert(id);
    WorkersByHeartbeat[version].insert(id);
    db.Table<Schema::Workers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
        NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(version.Step),
        NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(version.TxId));
}

void TController::StartMismatchDrain(const TWorkerId& id, const TString& issue,
        NIceDb::TNiceDb& db, const TActorContext& ctx)
{
    AFL_VERIFY(!ReplicationsWithSourceOffsetMismatch.contains(id.ReplicationId()))("worker", id);
    auto replication = Find(id.ReplicationId());
    AFL_VERIFY(replication)("worker", id);
    YDB_LOG_ERROR_CTX(ctx, "Destination source offset mismatch", {"worker", id}, {"issue", issue});
    replication->SetState(TReplication::EState::Error, issue);
    ReplicationsWithSourceOffsetMismatch.insert(id.ReplicationId());
    const auto deadline = ctx.Now() + DrainTimeout;
    DrainDeadlines[id.ReplicationId()] = deadline;
    db.Table<Schema::Replications>().Key(id.ReplicationId()).Update(
        NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
        NIceDb::TUpdate<Schema::Replications::Issue>(issue),
        NIceDb::TUpdate<Schema::Replications::SourceOffsetMismatch>(true),
        NIceDb::TUpdate<Schema::Replications::DrainDeadline>(deadline.MilliSeconds()));

    // Retain failed workers in the GLOBAL quorum.
    for (auto& [workerId, worker] : Workers) {
        if (workerId.ReplicationId() != id.ReplicationId()) {
            continue;
        }
        worker.DrainPending = true;
        db.Table<Schema::Workers>().Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Update(
            NIceDb::TUpdate<Schema::Workers::DrainPending>(true));
    }
}

class TController::TTxSourceOffsetReport: public TTxBase {
    TEvService::TEvSourceOffsetReport::TPtr Event;
    THolder<TEvService::TEvSourceOffsetResult> Result;
    TWorkerId Id;
    TMaybe<ui64> PersistedNextOffset;
    bool Stopping = false;

public:
    TTxSourceOffsetReport(TController* self, TEvService::TEvSourceOffsetReport::TPtr& ev)
        : TTxBase("TxSourceOffsetReport", self)
        , Event(ev)
        , Id(TWorkerId::Parse(Event->Get()->Record.GetWorker()))
    {}

    TTxType GetTxType() const override { return TXTYPE_SOURCE_OFFSET_REPORT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Event->Get()->Record;
        const auto& controller = request.GetController();
        if (controller.GetTabletId() != Self->TabletID()
            || controller.GetGeneration() != Self->Executor()->Generation()
            || !Self->HasWorkerSession(Id, Event->Sender.NodeId())
            || !Self->TracksSourceOffsets(Id))
        {
            return true;
        }
        auto& worker = Self->Workers.at(Id);
        NIceDb::TNiceDb db(txc.DB);
        if (!Self->ReplicationsWithSourceOffsetMismatch.contains(Id.ReplicationId()) || worker.DrainPending) {
            const auto* savedOffset = Self->ProcessedSourceOffsets.FindPtr(Id);
            if (request.HasNextOffset() && (!savedOffset || request.GetNextOffset() > savedOffset->NextOffset)) {
                PersistedNextOffset = request.GetNextOffset();
                Self->ProcessedSourceOffsets[Id].NextOffset = *PersistedNextOffset;
                db.Table<Schema::ProcessedSourceOffsets>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Update(
                    NIceDb::TUpdate<Schema::ProcessedSourceOffsets::NextOffset>(request.GetNextOffset()));
            }
            if (request.GetDrained() && Self->DrainDeadlines.contains(Id.ReplicationId())) {
                if (request.HasHeartbeat()) {
                    const auto version = TRowVersion::FromProto(request.GetHeartbeat());
                    Self->SaveHeartbeat(Id, version, db);
                    // Re-evaluate the quorum after persisting the final heartbeat.
                    Self->PendingHeartbeats[Id] = Max(Self->PendingHeartbeats[Id], version);
                }
                worker.DrainPending = false;
                db.Table<Schema::Workers>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Update(
                    NIceDb::TUpdate<Schema::Workers::DrainPending>(false));
            }
        }
        Stopping = Self->ReplicationsWithSourceOffsetMismatch.contains(Id.ReplicationId());
        Result = MakeHolder<TEvService::TEvSourceOffsetResult>();
        Result->Record.SetStop(Stopping);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Result) {
            return;
        }
        if (PersistedNextOffset) {
            if (auto* savedOffset = Self->ProcessedSourceOffsets.FindPtr(Id)) {
                savedOffset->PersistedNextOffset = Max(savedOffset->PersistedNextOffset.GetOrElse(0), *PersistedNextOffset);
            }
        }
        // Only this post-commit acknowledgement authorizes a source commit.
        ctx.Send(Event->Sender, Result.Release(), 0, Event->Cookie);
        if (Stopping) {
            Self->StopWorkersForMismatch(Id.ReplicationId(), false, ctx);
        }
    }

};

void TController::QueueReplicationWorkerStops(ui64 replicationId) {
    for (const auto& [id, worker] : Workers) {
        if (id.ReplicationId() == replicationId && worker.HasSession()) {
            StopQueue.emplace(id, worker.GetSession());
        }
    }
    ScheduleProcessQueues();
}

void TController::RetryWorkerRemovals(ui64 replicationId, const TActorContext& ctx) {
    const TVector<TWorkerId> pending(RemoveQueue.begin(), RemoveQueue.end());
    for (const auto& id : pending) {
        if (id.ReplicationId() != replicationId) {
            continue;
        }
        if (const auto* worker = Workers.FindPtr(id); worker && worker->HasSession()) {
            StopQueue.emplace(id, worker->GetSession());
        } else {
            MaybeRemoveWorker(id, ctx);
        }
    }
    ScheduleProcessQueues();
}

void TController::StopWorkersForMismatch(ui64 replicationId, bool drainStarted, const TActorContext& ctx) {
    if (drainStarted) {
        for (auto it = IndexReadyActors.begin(); it != IndexReadyActors.end();) {
            if (it->first.first == replicationId) {
                ctx.Send(it->second, new TEvents::TEvPoison());
                IndexReadyActors.erase(it++);
            } else {
                ++it;
            }
        }
        for (const auto& [key, _] : SchemaBarriers) {
            if (key.first == replicationId) {
                StopSchemaChangeDstAlter(key, ctx);
            }
        }
    }
    QueueReplicationWorkerStops(replicationId);
    RunTxHeartbeat(ctx);
    if (drainStarted) {
        ctx.Send(ctx.SelfID, new TEvPrivate::TEvCheckDrain(replicationId));
    }
}

class TController::TTxFinishDrain: public TTxBase {
    const ui64 ReplicationId;
    bool Finished = false;

public:
    TTxFinishDrain(TController* self, ui64 replicationId)
        : TTxBase("TxFinishDrain", self)
        , ReplicationId(replicationId)
    {}

    TTxType GetTxType() const override { return TXTYPE_FINISH_DRAIN; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto it = Self->DrainDeadlines.find(ReplicationId);
        if (it == Self->DrainDeadlines.end()) {
            return true;
        }
        const bool expired = ctx.Now() >= it->second;
        const auto pending = CountIf(Self->Workers, [this](const auto& item) {
            return item.first.ReplicationId() == ReplicationId && item.second.DrainPending;
        });
        if (!expired && (pending || Self->ProcessHeartbeatsInFlight || !Self->PendingHeartbeats.empty() || Self->CommittingTxId
            || !Self->IndexCommits.empty() || !Self->SchemaTargetFlushes.empty())) {
            return true;
        }
        auto replication = Self->Find(ReplicationId);
        AFL_VERIFY(replication)("replication", ReplicationId);
        auto issue = TStringBuilder() << replication->GetIssue();
        if (pending) {
            issue << " Drain timed out with " << pending << " unfinished workers.";
        }
        if (Self->CommittingTxId) {
            issue << " GLOBAL commit " << Self->CommittingTxId << " was unconfirmed when draining stopped.";
        }
        if (!Self->IndexCommits.empty() || !Self->SchemaTargetFlushes.empty()) {
            issue << " Auxiliary commit proposals have an unknown outcome.";
        }
        if (!Self->AssignedTxIds.empty()) {
            issue << " " << Self->AssignedTxIds.size() << " GLOBAL transactions remain unresolved.";
        }
        YDB_LOG_ERROR_CTX(ctx, "Replication drain finished", {"replication", ReplicationId}, {"issue", issue});
        replication->SetState(TReplication::EState::Error, issue);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(ReplicationId).Update(
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue()),
            NIceDb::TUpdate<Schema::Replications::DrainDeadline>(0));
        Self->DrainDeadlines.erase(it);
        Finished = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Self->DrainDeadlines.contains(ReplicationId) && !Finished) {
            return;
        }
        Self->QueueReplicationWorkerStops(ReplicationId);
        if (Finished) {
            Self->RetryWorkerRemovals(ReplicationId, ctx);
        }
    }
};

void TController::Handle(TEvService::TEvSourceOffsetReport::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSourceOffsetReport(this, ev), ctx);
}

void TController::Handle(TEvPrivate::TEvCheckDrain::TPtr& ev, const TActorContext& ctx) {
    if (!DrainDeadlines.contains(ev->Get()->ReplicationId)) {
        return;
    }
    ctx.Schedule(DrainCheckInterval, new TEvPrivate::TEvCheckDrain(ev->Get()->ReplicationId));
    RunTxHeartbeat(ctx);
}

void TController::CheckDrains(const TActorContext& ctx) {
    // Finalize only after the latest heartbeat transaction completes.
    TVector<ui64> ids;
    for (const auto& [id, _] : DrainDeadlines) {
        ids.push_back(id);
    }
    for (const auto id : ids) {
        Execute(new TTxFinishDrain(this, id), ctx);
    }
}

}
