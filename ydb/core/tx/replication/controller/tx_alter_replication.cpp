#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxAlterReplication: public TTxBase {
    TEvController::TEvAlterReplication::TPtr Ev;
    THolder<TEvController::TEvAlterReplicationResult> Result;
    TReplication::TPtr Replication;
    bool ResetSchemaBarriers = false;
    TVector<std::pair<ui64, ui64>> SchemaChangeDstAlterersToStop;

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

        bool alter = false;

        const auto& oldConfig = Replication->GetConfig();
        auto newConfig = std::move(*record.MutableConfig());

        if (oldConfig.HasTransferSpecific()) {
            auto& oldSpecific = oldConfig.GetTransferSpecific();
            auto& newSpecific = newConfig.GetTransferSpecific();

            alter = oldSpecific.GetTarget().GetTransformLambda() != newSpecific.GetTarget().GetTransformLambda()
                || oldSpecific.GetTarget().GetDirectoryPath() != newSpecific.GetTarget().GetDirectoryPath()
                || oldSpecific.GetBatching().GetBatchSizeBytes() != newSpecific.GetBatching().GetBatchSizeBytes()
                || oldSpecific.GetBatching().GetFlushIntervalMilliSeconds() != newSpecific.GetBatching().GetFlushIntervalMilliSeconds();
        }

        auto desiredState = Replication->GetState();
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
            // A failed schema DDL leaves workers parked at the record that
            // caused it.  Resetting replication is an explicit request to
            // retry that operation, so discard the failed barrier and let the
            // workers establish a fresh durable snapshot on their retries.
            ResetSchemaBarriers = true;
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
        Replication->ResetCredentials(ctx);

        if (record.HasLocation()) {
            Replication->SetLocation(record.GetLocation());
        }

        NIceDb::TNiceDb db(txc.DB);
        if (ResetSchemaBarriers) {
            for (auto it = Self->SchemaBarriers.begin(); it != Self->SchemaBarriers.end();) {
                // Keep nonfailed sibling barriers: some of their workers may
                // already have committed beyond the schema record and cannot
                // reconstruct a new collection barrier after restart.
                if (it->first.first != Replication->GetId()
                    || it->second.Phase != ESchemaBarrierPhase::Error) {
                    ++it;
                    continue;
                }

                const auto key = it->first;
                for (const auto& workerId : it->second.ExpectedWorkers) {
                    db.Table<Schema::SchemaBarrierWorkers>()
                        .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Delete();
                }
                for (const auto writeTxId : it->second.TargetFlushTxIds) {
                    db.Table<Schema::SchemaBarrierFlushes>().Key(key.first, key.second, writeTxId).Delete();
                    Self->SchemaTargetFlushes.erase(writeTxId);
                }
                if (Self->ActiveSchemaTargetFlush && *Self->ActiveSchemaTargetFlush == key) {
                    Self->ActiveSchemaTargetFlush.reset();
                }
                if (Self->SchemaChangeDstAlterers.contains(key)) {
                    SchemaChangeDstAlterersToStop.push_back(key);
                }
                db.Table<Schema::SchemaBarriers>().Key(key.first, key.second).Delete();
                it = Self->SchemaBarriers.erase(it);
            }
        }
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::Config>(Replication->GetConfig().SerializeAsString()),
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::DesiredState>(desiredState),
            NIceDb::TUpdate<Schema::Replications::Issue>(issue)
        );

        if (!alter) {
            Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
            return true;
        }

        Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

        // Do not tear down workers (or the destination alterer) while a
        // schema barrier is intentionally holding replication degraded.  The
        // updated configuration and desired state above are durable; this
        // separate marker makes the target lifecycle transition recoverable.
        // Only Error -> Ready may rebuild the failed target while a
        // nonfailed global sibling is verifying. Its fresh workers are
        // required to restore the sibling's all-worker quorum. Pause and
        // finalization still must wait, otherwise they would tear down those
        // workers before the persisted barrier can complete.
        const bool recoverReady = ResetSchemaBarriers && desiredState == TReplication::EState::Ready;
        if (!recoverReady && Self->HasActiveSchemaBarrier(Replication->GetId())) {
            Self->DeferredAlters.insert(Replication->GetId());
            db.Table<Schema::DeferredAlters>().Key(Replication->GetId()).Update();
            YDB_LOG_NOTICE_CTX(ctx, "Defer replication alter until schema barriers are applied",
                {"rid", Replication->GetId()},
                {"pathId", pathId});
            return true;
        }

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

        for (const auto& key : SchemaChangeDstAlterersToStop) {
            Self->StopSchemaChangeDstAlter(key, ctx);
        }

        if (Result) {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Replication) {
            Replication->Progress(ctx);
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
    {
    }

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
        db.Table<Schema::DeferredAlters>().Key(replicationId).Delete();

        Replication = Self->Find(replicationId);
        if (!Replication) {
            return true;
        }

        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            target->Shutdown(ctx);
            target->SetDstState(TReplication::EDstState::Alter);
            if (target->GetStreamState() == TReplication::EStreamState::Error
                && Replication->GetDesiredState() == TReplication::EState::Ready) {
                target->SetStreamState(TReplication::EStreamState::Creating);
            }
            db.Table<Schema::Targets>().Key(replicationId, tid).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()));
        }

        YDB_LOG_NOTICE_CTX(ctx, "Resume deferred replication alter",
            {"rid", replicationId});
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
