#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

bool HasRelevantTransferConfigChanges(
        const NKikimrReplication::TReplicationConfig& current,
        const NKikimrReplication::TReplicationConfig& next)
{
    if (!current.HasTransferSpecific()) {
        return false;
    }

    const auto& currentSpecific = current.GetTransferSpecific();
    const auto& nextSpecific = next.GetTransferSpecific();
    return currentSpecific.GetTarget().GetTransformLambda() != nextSpecific.GetTarget().GetTransformLambda()
        || currentSpecific.GetTarget().GetDirectoryPath() != nextSpecific.GetTarget().GetDirectoryPath()
        || currentSpecific.GetBatching().GetBatchSizeBytes() != nextSpecific.GetBatching().GetBatchSizeBytes()
        || currentSpecific.GetBatching().GetFlushIntervalMilliSeconds()
            != nextSpecific.GetBatching().GetFlushIntervalMilliSeconds();
}

} // anonymous namespace

class TController::TTxAlterReplication: public TTxBase {
    TEvController::TEvAlterReplication::TPtr Ev;
    THolder<TEvController::TEvAlterReplicationResult> Result;
    TReplication::TPtr Replication;
    bool ResetFailedSchemaBarriers = false;
    TVector<std::pair<ui64, ui64>> AlterersToStop;
    THashSet<TWorkerId> BarrierWorkersToRestart;

    static bool IsFullyCompleted(const TSchemaBarrier& barrier) {
        return barrier.CompletedWorkers.size() == barrier.ExpectedWorkers.size();
    }

    static bool ShouldCancelBarrier(const TSchemaBarrier& barrier, bool targetUnavailable) {
        return barrier.Phase == ESchemaBarrierPhase::Error
            || (barrier.Phase == ESchemaBarrierPhase::Collecting && targetUnavailable);
    }

    static bool NeedsTargetRecovery(const TSchemaBarrier& barrier, bool targetWasError) {
        return targetWasError
            && (barrier.Phase == ESchemaBarrierPhase::Altering
                || (barrier.Phase == ESchemaBarrierPhase::Applied && !IsFullyCompleted(barrier)));
    }

    bool NeedsWorkerRestart(const TSchemaBarrier& barrier, const TWorkerId& id, bool targetWasError) const {
        if (barrier.CompletedWorkers.contains(id)) {
            return false;
        }

        const auto worker = Self->Workers.find(id);
        return targetWasError || worker == Self->Workers.end() || !worker->second.HasSession();
    }

    void RetainBarrierForRecovery(
            const std::pair<ui64, ui64>& key,
            TSchemaBarrier& barrier,
            TReplication::ITarget* target,
            NIceDb::TNiceDb& db)
    {
        const bool targetWasError = target && target->GetDstState() == TReplication::EDstState::Error;
        if (NeedsTargetRecovery(barrier, targetWasError)) {
            // The DDL may already be committed, so this barrier must survive
            // the lifecycle request. Let its unfinished workers recover.
            target->SetDstState(TReplication::EDstState::Ready);
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()));
        }

        // A failure in any target can stop workers across the replication.
        // Leave still-attached healthy workers running.
        for (const auto& workerId : barrier.ExpectedWorkers) {
            if (NeedsWorkerRestart(barrier, workerId, targetWasError)) {
                BarrierWorkersToRestart.insert(workerId);
            }
        }
    }

    void CancelBarrier(
            const std::pair<ui64, ui64>& key,
            const TSchemaBarrier& barrier,
            const TReplication::ITarget* target,
            NIceDb::TNiceDb& db)
    {
        for (const auto& workerId : barrier.ExpectedWorkers) {
            db.Table<Schema::SchemaBarrierWorkers>()
                .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Delete();
        }

        if (Self->SchemaChangeDstAlterers.contains(key)) {
            AlterersToStop.push_back(key);
        }

        if (target) {
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(0),
                NIceDb::TUpdate<Schema::Targets::SchemaBarrierChange>(TString()),
                NIceDb::TUpdate<Schema::Targets::DstAlterTxId>(0));
        }
    }

    void RecoverFailedSchemaBarriers(NIceDb::TNiceDb& db) {
        for (auto it = Self->SchemaBarriers.begin(); it != Self->SchemaBarriers.end();) {
            const auto key = it->first;
            if (key.first != Replication->GetId()) {
                ++it;
                continue;
            }

            auto* target = Replication->FindTarget(key.second);
            const bool targetUnavailable = !target || target->GetDstState() == TReplication::EDstState::Error;
            if (!ShouldCancelBarrier(it->second, targetUnavailable)) {
                RetainBarrierForRecovery(key, it->second, target, db);
                ++it;
                continue;
            }

            // No destination DDL has started in Collecting, so a stranded
            // barrier can be cancelled safely. Error barriers are reset by
            // the explicit lifecycle request as well.
            CancelBarrier(key, it->second, target, db);
            it = Self->SchemaBarriers.erase(it);
        }
    }

public:
    explicit TTxAlterReplication(TController* self, TEvController::TEvAlterReplication::TPtr& ev)
        : TTxBase("TxAlterReplication", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ALTER_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", Ev->Get()->ToString()});

        auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvAlterReplicationResult>();
        Result->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
        Result->Record.SetOrigin(Self->TabletID());

        const auto pathId = TPathId::FromProto(record.GetPathId());
        Replication = Self->Find(pathId);

        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Cannot alter unknown replication",
                {"pathId", pathId});

            Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::UNKNOWN);
            return true;
        }

        const auto& oldConfig = Replication->GetConfig();
        auto newConfig = std::move(*record.MutableConfig());
        bool alter = HasRelevantTransferConfigChanges(oldConfig, newConfig);

        auto desiredState = Replication->GetDesiredState();
        if (record.HasSwitchState()) {
            switch (record.GetSwitchState().GetStateCase()) {
                case NKikimrReplication::TReplicationState::kDone:
                    desiredState = TReplication::EState::Done;
                    alter = true;
                    break;
                case NKikimrReplication::TReplicationState::kPaused:
                    desiredState = TReplication::EState::Paused;
                    alter = true;
                    break;
                case NKikimrReplication::TReplicationState::kStandBy:
                    desiredState = TReplication::EState::Ready;
                    alter = true;
                    break;
                default:
                    Y_ABORT("Invalid state");
            }
        }

        if (alter && Replication->GetState() == TReplication::EState::Error) {
            Replication->SetState(TReplication::EState::Ready);
            ResetFailedSchemaBarriers = true;
            if (desiredState == TReplication::EState::Error) {
                desiredState = TReplication::EState::Ready;
            }
        }

        auto issue = Replication->GetIssue();
        if (alter) {
            Replication->SetDesiredState(desiredState);
            if (desiredState == TReplication::EState::Ready) {
                issue = "";
            }
        }

        Replication->SetConfig(std::move(newConfig));

        if (record.HasLocation()) {
            Replication->SetLocation(record.GetLocation());
        }

        NIceDb::TNiceDb db(txc.DB);
        if (ResetFailedSchemaBarriers) {
            RecoverFailedSchemaBarriers(db);
        }
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::Config>(Replication->GetConfig().SerializeAsString()),
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::DesiredState>(desiredState),
            NIceDb::TUpdate<Schema::Replications::Issue>(issue)
        );

        if (!alter) {
            if (!Self->DeferredAlters.contains(Replication->GetId())) {
                Replication->ResetCredentials(ctx);
            }
            Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
            return true;
        }

        Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

        const bool recoverReady = ResetFailedSchemaBarriers && desiredState == TReplication::EState::Ready;
        if (!recoverReady && Self->HasActiveSchemaBarrier(Replication->GetId())) {
            Self->DeferredAlters.insert(Replication->GetId());
            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::DeferredAlter>(true));
            YDB_LOG_NOTICE_CTX(ctx, "Defer replication alter until schema barriers complete",
                {"rid", Replication->GetId()});
            if (BarrierWorkersToRestart.empty()) {
                Replication.Reset();
            }
            return true;
        }

        Replication->ResetCredentials(ctx);
        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            target->Shutdown(ctx);
            target->SetDstState(TReplication::EDstState::Alter);
            if (target->GetStreamState() == TReplication::EStreamState::Error && desiredState == TReplication::EState::Ready) {
                target->SetStreamState(TReplication::EStreamState::Creating);
            }
            db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
            );

            alter = true;
        }

        if (alter) {
            YDB_LOG_NOTICE_CTX(ctx, "Alter replication",
                {"rid", Replication->GetId()},
                {"pathId", pathId});
        } else {
            Replication.Reset();
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        for (const auto& key : AlterersToStop) {
            Self->StopSchemaChangeDstAlter(key, ctx);
        }

        if (Result) {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Replication) {
            Replication->Progress(ctx);
        }

        for (const auto& id : BarrierWorkersToRestart) {
            const auto it = Self->Workers.find(id);
            if (it == Self->Workers.end()) {
                continue;
            }
            if (it->second.HasSession()) {
                Self->StopQueue.emplace(id, it->second.GetSession());
            } else if (it->second.HasCommand()) {
                Self->BootQueue.insert(id);
            }
        }

        if (BarrierWorkersToRestart) {
            Self->ScheduleProcessQueues();
        }
    }

}; // TTxAlterReplication

void TController::RunTxAlterReplication(TEvController::TEvAlterReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAlterReplication(this, ev), ctx);
}

class TController::TTxResumeDeferredAlter: public TTxBase {
    TEvPrivate::TEvResumeDeferredAlter::TPtr Event;
    TReplication::TPtr Replication;

public:
    TTxResumeDeferredAlter(TController* self, TEvPrivate::TEvResumeDeferredAlter::TPtr& ev)
        : TTxBase("TxResumeDeferredAlter", self)
        , Event(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_ALTER_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto replicationId = Event->Get()->ReplicationId;
        if (!Self->DeferredAlters.contains(replicationId) || Self->HasActiveSchemaBarrier(replicationId)) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->DeferredAlters.erase(replicationId);
        Replication = Self->Find(replicationId);
        if (!Replication) {
            return true;
        }

        Replication->ResetCredentials(ctx);
        db.Table<Schema::Replications>().Key(replicationId).Update(
            NIceDb::TUpdate<Schema::Replications::DeferredAlter>(false));

        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }
            target->Shutdown(ctx);
            target->SetDstState(TReplication::EDstState::Alter);
            if (target->GetStreamState() == TReplication::EStreamState::Error
                && Replication->GetDesiredState() == TReplication::EState::Ready)
            {
                target->SetStreamState(TReplication::EStreamState::Creating);
            }
            db.Table<Schema::Targets>().Key(replicationId, tid).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()));
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Replication) {
            Replication->Progress(ctx);
        }
    }
};

void TController::Handle(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx) {
    RunTxResumeDeferredAlter(ev, ctx);
}

void TController::RunTxResumeDeferredAlter(TEvPrivate::TEvResumeDeferredAlter::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResumeDeferredAlter(this, ev), ctx);
}

}
