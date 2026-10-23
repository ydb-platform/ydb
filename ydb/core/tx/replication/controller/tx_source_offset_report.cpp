#include "controller_impl.h"

#include <util/generic/algorithm.h>
#include <util/generic/mapfindptr.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

bool TController::TracksSourceOffsets(const TWorkerId& id) const {
    const auto replication = Find(id.ReplicationId());
    const auto* target = FindTarget(id);
    return replication && target
        && replication->GetConfig().GetConsistencySettings().HasGlobal()
        && target->GetKind() != TReplication::ETargetKind::Transfer;
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
    const auto it = Workers.find(id);
    if (it == Workers.end() || !IsHeartbeatParticipant(id)) {
        return;
    }
    auto& worker = it->second;
    if (worker.HasHeartbeat()) {
        const auto previous = worker.GetHeartbeat();
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
    worker.SetHeartbeat(version);
    WorkersWithHeartbeat.insert(id);
    WorkersByHeartbeat[version].insert(id);
    db.Table<Schema::Workers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
        NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(version.Step),
        NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(version.TxId));
}

void TController::SaveDrainCommit(NIceDb::TNiceDb& db) {
    for (const auto rid : ReplicationsWithSourceOffsetMismatch) {
        if (auto* drain = MapFindPtr(DrainStateByReplicationId, rid)) {
            drain->CommitTxId = CommittingTxId;
            drain->UnknownCommit = false;
        }
        // A late success also clears the recorded attempt after the timeout.
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::DrainCommitTxId>(CommittingTxId));
    }
}

void TController::StartMismatchDrain(const TWorkerId& id, const TString& issue,
        NIceDb::TNiceDb& db, const TActorContext& ctx)
{
    if (ReplicationsWithSourceOffsetMismatch.contains(id.ReplicationId())) {
        return;
    }
    auto replication = Find(id.ReplicationId());
    Y_ABORT_UNLESS(replication);
    YDB_LOG_ERROR_CTX(ctx, "Destination source offset mismatch", {"worker", id}, {"issue", issue});
    replication->SetState(TReplication::EState::Error, issue);
    ReplicationsWithSourceOffsetMismatch.insert(id.ReplicationId());
    auto& drain = DrainStateByReplicationId[id.ReplicationId()];
    drain.Deadline = ctx.Now() + DrainTimeout;
    drain.CommitTxId = CommittingTxId;
    db.Table<Schema::Replications>().Key(id.ReplicationId()).Update(
        NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
        NIceDb::TUpdate<Schema::Replications::Issue>(issue),
        NIceDb::TUpdate<Schema::Replications::SourceOffsetMismatch>(true),
        NIceDb::TUpdate<Schema::Replications::DrainDeadline>(drain.Deadline.MilliSeconds()),
        NIceDb::TUpdate<Schema::Replications::DrainCommitTxId>(drain.CommitTxId));

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
        TMaybe<ui64> savedOffset;
        if (const auto saved = Self->ProcessedSourceOffsets.find(Id); saved != Self->ProcessedSourceOffsets.end()) {
            savedOffset = saved->second;
        }
        if (!Self->ReplicationsWithSourceOffsetMismatch.contains(Id.ReplicationId()) || worker.DrainPending) {
            if (request.HasNextOffset() && (!savedOffset || request.GetNextOffset() > *savedOffset)) {
                savedOffset = request.GetNextOffset();
                Self->ProcessedSourceOffsets[Id] = *savedOffset;
                db.Table<Schema::ProcessedSourceOffsets>().Key(Id.ReplicationId(), Id.TargetId(), Id.WorkerId()).Update(
                    NIceDb::TUpdate<Schema::ProcessedSourceOffsets::NextOffset>(*savedOffset));
            }
            if (request.GetDrained() && Self->DrainStateByReplicationId.contains(Id.ReplicationId())) {
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
        Result->Record.MutableController()->CopyFrom(request.GetController());
        Result->Record.MutableWorker()->CopyFrom(request.GetWorker());
        Result->Record.SetWorkerInstance(request.GetWorkerInstance());
        Result->Record.SetRequestId(request.GetRequestId());
        if (savedOffset) {
            Result->Record.SetAuthorizedNextOffset(*savedOffset);
        }
        Result->Record.SetStop(Stopping);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Result) {
            return;
        }
        // Only this post-commit acknowledgement authorizes a source commit.
        ctx.Send(Event->Sender, Result.Release());
        if (Stopping) {
            Self->StopWorkersForMismatch(Id.ReplicationId(), false, ctx);
        }
    }

};

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
    for (const auto& [id, worker] : Workers) {
        if (id.ReplicationId() == replicationId && worker.HasSession()) {
            StopQueue.emplace(id, worker.GetSession());
        }
    }
    ScheduleProcessQueues();
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
        const auto it = Self->DrainStateByReplicationId.find(ReplicationId);
        if (it == Self->DrainStateByReplicationId.end()) {
            return true;
        }
        const auto& drain = it->second;
        const bool expired = ctx.Now() >= drain.Deadline;
        const auto pending = CountIf(Self->Workers, [this](const auto& item) {
            return item.first.ReplicationId() == ReplicationId && item.second.DrainPending;
        });
        if (!expired && (pending || Self->ProcessHeartbeatsInFlight || !Self->PendingHeartbeats.empty() || Self->CommittingTxId || drain.UnknownCommit
            || !Self->IndexCommits.empty() || !Self->SchemaTargetFlushes.empty())) {
            return true;
        }
        auto replication = Self->Find(ReplicationId);
        if (!replication) {
            Self->DrainStateByReplicationId.erase(it);
            return true;
        }
        auto issue = TStringBuilder() << replication->GetIssue();
        if (pending) {
            issue << " Drain timed out with " << pending << " unfinished workers.";
        }
        if (Self->CommittingTxId || drain.UnknownCommit) {
            issue << " GLOBAL commit " << drain.CommitTxId << " was unconfirmed when draining stopped.";
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
        Self->DrainStateByReplicationId.erase(it);
        Finished = true;
        return true;
    }

    void Complete(const TActorContext&) override {
        if (!Self->DrainStateByReplicationId.contains(ReplicationId) && !Finished) {
            return;
        }
        for (const auto& [id, worker] : Self->Workers) {
            if (id.ReplicationId() == ReplicationId && worker.HasSession()) {
                Self->StopQueue.emplace(id, worker.GetSession());
            }
        }
        Self->ScheduleProcessQueues();
    }
};

void TController::Handle(TEvService::TEvSourceOffsetReport::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSourceOffsetReport(this, ev), ctx);
}

void TController::Handle(TEvPrivate::TEvCheckDrain::TPtr& ev, const TActorContext& ctx) {
    if (!DrainStateByReplicationId.contains(ev->Get()->ReplicationId)) {
        return;
    }
    ctx.Schedule(DrainCheckInterval, new TEvPrivate::TEvCheckDrain(ev->Get()->ReplicationId));
    RunTxHeartbeat(ctx);
}

void TController::CheckDrains(const TActorContext& ctx) {
    // Finalize only after the latest heartbeat transaction completes.
    TVector<ui64> ids;
    for (const auto& [id, _] : DrainStateByReplicationId) {
        ids.push_back(id);
    }
    for (const auto id : ids) {
        Execute(new TTxFinishDrain(this, id), ctx);
    }
}

}
