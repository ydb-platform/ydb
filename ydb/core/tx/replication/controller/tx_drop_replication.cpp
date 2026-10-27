#include "controller_impl.h"

#include <ydb/core/base/path.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxDropReplication: public TTxBase {
    TEvController::TEvDropReplication::TPtr PubEv;
    TEvPrivate::TEvDropReplication::TPtr PrivEv;
    THolder<IEventHandle> Result; // TEvController::TEvDropReplicationResult
    TReplication::TPtr Replication;
    TVector<std::pair<ui64, ui64>> AlterersToStop;

public:
    explicit TTxDropReplication(TController* self, TEvController::TEvDropReplication::TPtr& ev)
        : TTxBase("TxDropReplication", self)
        , PubEv(ev)
    {
    }

    explicit TTxDropReplication(TController* self, TEvPrivate::TEvDropReplication::TPtr& ev)
        : TTxBase("TxDropReplication", self)
        , PrivEv(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        if (PubEv) {
            return ExecutePub(txc, ctx);
        } else if (PrivEv) {
            return ExecutePriv(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    bool Reject(const TActorContext& ctx) {
        auto ev = MakeHolder<TEvController::TEvDropReplicationResult>();
        ev->Record.MutableOperationId()->CopyFrom(PubEv->Get()->Record.GetOperationId());
        ev->Record.SetOrigin(Self->TabletID());
        ev->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::INVALID_REQUEST);
        Result = MakeHolder<IEventHandle>(PubEv->Sender, ctx.SelfID, ev.Release());
        Replication.Reset(); // Invalid input cannot trigger Progress or source work.
        return true;
    }

    void ClearSchemaBarriers(NIceDb::TNiceDb& db) {
        Self->DeferredAlters.erase(Replication->GetId());
        for (auto it = Self->SchemaBarriers.begin(); it != Self->SchemaBarriers.end();) {
            if (it->first.first != Replication->GetId()) {
                ++it;
                continue;
            }
            const auto key = it->first;
            for (const auto& workerId : it->second.ExpectedWorkers) {
                db.Table<Schema::SchemaBarrierWorkers>()
                    .Key(workerId.ReplicationId(), workerId.TargetId(), workerId.WorkerId()).Delete();
            }
            Self->StopSchemaChangeTargetFlush(key);
            if (Self->SchemaChangeDstAlterers.contains(key)) {
                AlterersToStop.push_back(key);
            }
            if (Replication->FindTarget(key.second)) {
                db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                    NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(0),
                    NIceDb::TUpdate<Schema::Targets::SchemaBarrierChange>(TString()),
                    NIceDb::TUpdate<Schema::Targets::DstAlterTxId>(0),
                    NIceDb::TUpdate<Schema::Targets::SchemaBarrierFlushTxIds>(TString()));
            }
            it = Self->SchemaBarriers.erase(it);
        }
    }

    void BeginForceRemoval(NIceDb::TNiceDb& db, const TActorContext& ctx) {
        Y_ABORT_UNLESS(Replication->GetState() == TReplication::EState::Done);
        // Completed barriers outlive workers. Delete their membership in the
        // same transaction before any target identity can disappear.
        ClearSchemaBarriers(db);
        Replication->SetState(TReplication::EState::Removing);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()));
        const auto rid = Replication->GetId();
        for (auto* target : Replication->GetTargets()) {
            const auto tid = target->GetId();
            target->Shutdown(ctx);
            if (Replication->GetDropTargets() == TReplication::EDropTargets::Cascade) {
                target->SetStreamState(TReplication::EStreamState::Removed);
                target->SetDstState(TReplication::EDstState::Removing);
                db.Table<Schema::SrcStreams>().Key(rid, tid).Update(
                    NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()));
                db.Table<Schema::Targets>().Key(rid, tid).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()));
            } else {
                db.Table<Schema::Targets>().Key(rid, tid).Delete();
                db.Table<Schema::SrcStreams>().Key(rid, tid).Delete();
                for (const auto wid : target->GetWorkers()) {
                    db.Table<Schema::Workers>().Key(rid, tid, wid).Delete();
                }
                Self->CompleteWorkerSets.erase({rid, tid});
                Self->IndexBuilds.erase({rid, tid});
                for (auto it = Self->IndexBuildProgress.begin(); it != Self->IndexBuildProgress.end();) {
                    if (it->first.ReplicationId() == rid && it->first.TargetId() == tid) {
                        db.Table<Schema::IndexBuildWorkers>().Key(rid, tid, it->first.WorkerId()).Delete();
                        Self->IndexBuildProgress.erase(it++);
                    } else {
                        ++it;
                    }
                }
                Replication->RemoveTarget(tid);
            }
        }
    }

    bool ExecutePub(TTransactionContext& txc, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", PubEv->Get()->ToString()});

        const auto& record = PubEv->Get()->Record;
        const auto& unknownFields = record.GetReflection()->GetUnknownFields(record);
        for (int i = 0; i < unknownFields.field_count(); ++i) {
            if (unknownFields.field(i).number() == 4) {
                return Reject(ctx);
            }
        }
        if (!NKikimrReplication::EDropReplicationMode_IsValid(record.GetMode())) {
            return Reject(ctx);
        }
        const auto mode = static_cast<TReplication::EDropMode>(record.GetMode());
        const auto targets = record.GetCascade() ? TReplication::EDropTargets::Cascade : TReplication::EDropTargets::Retain;
        const auto pathId = TPathId::FromProto(record.GetPathId());
        const auto& opId = record.GetOperationId();
        Replication = Self->Find(pathId);

        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Cannot drop unknown replication",
                {"pathId", pathId});

            auto ev = MakeHolder<TEvController::TEvDropReplicationResult>();
            ev->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
            ev->Record.SetOrigin(Self->TabletID());
            ev->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::NOT_FOUND);
            Result = MakeHolder<IEventHandle>(PubEv->Sender, ctx.SelfID, ev.Release());

            return true;
        }

        if (!Replication->IsDropIntentSupported()) {
            return Reject(ctx);
        }

        if (Replication->GetState() == TReplication::EState::Removing
            || Replication->GetDropMode() == TReplication::EDropMode::ForceLocal)
        {
            // Accepted durable intent wins over all resends, including conflicts.
            if (mode != Replication->GetDropMode()
                || (Replication->GetDropTargets() != TReplication::EDropTargets::Legacy
                    && targets != Replication->GetDropTargets()))
            {
                return Reject(ctx);
            }
            Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
            return true;
        }

        if (mode == TReplication::EDropMode::ForceLocal
            && (Replication->GetDesiredState() != TReplication::EState::Done
                || Replication->GetConfig().HasTransferSpecific()))
        {
            return Reject(ctx);
        }

        NIceDb::TNiceDb db(txc.DB);
        Replication->SetDropIntent(mode, targets);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::DropMode>(mode),
            NIceDb::TUpdate<Schema::Replications::DropTargets>(targets));
        if (mode == TReplication::EDropMode::ForceLocal) {
            Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
            Replication->ResetCredentials(ctx);
            if (Replication->GetState() == TReplication::EState::Done) {
                BeginForceRemoval(db, ctx);
            } else {
                // Do not cancel submitted attachment/barrier DDL. Rebuild only
                // local detach actors so their failure policy follows the intent.
                bool recoverable = true;
                for (auto* target : Replication->GetTargets()) {
                    const auto state = target->GetDstState();
                    if (state == TReplication::EDstState::Error) {
                        if (!target->GetDstPathId() && !target->GetPendingDstPathId()) {
                            recoverable = false;
                            continue;
                        }
                        target->SetDstState(TReplication::EDstState::Alter);
                        target->SetIssue({});
                        db.Table<Schema::Targets>().Key(Replication->GetId(), target->GetId()).Update(
                            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
                    }
                    if (target->GetDstState() == TReplication::EDstState::Alter) {
                        target->Shutdown(ctx);
                    }
                }
                if (recoverable && Replication->GetState() == TReplication::EState::Error) {
                    Replication->SetState(TReplication::EState::Ready);
                    db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                        NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
                        NIceDb::TUpdate<Schema::Replications::Issue>(TString()));
                }
            }
            return true;
        }

        ClearSchemaBarriers(db);

        Replication->SetState(TReplication::EState::Removing);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::DeferredAlter>(false)
        );

        THashSet<TString> retainedTables;
        for (const auto* target : Replication->GetTargets()) {
            if (target->GetKind() != TReplication::ETargetKind::Table) {
                continue;
            }

            const bool cascadeWithoutDestination = record.GetCascade() && !target->GetDstPathId();
            const bool pendingAttachment = target->GetPendingDstPathId()
                && (record.GetCascade() || target->GetDstState() == TReplication::EDstState::Attaching);
            if (!cascadeWithoutDestination && !pendingAttachment) {
                continue;
            }

            retainedTables.insert(CanonizePath(target->GetDstPath()));
        }

        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            Self->CompleteWorkerSets.erase({Replication->GetId(), tid});
            db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                NIceDb::TUpdate<Schema::Targets::WorkerSetComplete>(false));

            bool detachIndex = false;
            if (target->GetKind() == TReplication::ETargetKind::IndexTable) {
                const TString indexPath(ExtractParent(target->GetDstPath()));
                const auto basePath = CanonizePath(TString(ExtractParent(indexPath)));
                if (retainedTables.contains(basePath)) {
                    // A base without a confirmed destination or with a pending
                    // attachment is preserved. Persist index cleanup ownership
                    // before the base target can disappear, including on restart.
                    if (target->GetDstPathId() && !target->GetPendingDstPathId()) {
                        target->SetPendingDstPathId(target->GetDstPathId());
                        db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                            NIceDb::TUpdate<Schema::Targets::PendingDstPathOwnerId>(target->GetPendingDstPathId().OwnerId),
                            NIceDb::TUpdate<Schema::Targets::PendingDstPathLocalId>(target->GetPendingDstPathId().LocalPathId)
                        );
                    }
                    detachIndex = bool(target->GetPendingDstPathId());
                }
            }

            const bool attaching = target->GetDstState() == TReplication::EDstState::Attaching;
            if (!attaching) {
                target->Shutdown(ctx);
            }

            target->SetStreamState(TReplication::EStreamState::Removing);
            db.Table<Schema::SrcStreams>().Key(Replication->GetId(), tid).Update(
                NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState())
            );

            // Keep the creator until its submitted ALTER has finished; the imported table is detached afterwards.
            if (!attaching && (record.GetCascade() || detachIndex)) {
                target->SetDstState(TReplication::EDstState::Removing);
                db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
                );
            }
        }

        YDB_LOG_NOTICE_CTX(ctx, "Drop replication",
            {"rid", Replication->GetId()},
            {"pathId", pathId});

        Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
        return true;
    }

    bool ExecutePriv(TTransactionContext& txc, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", PrivEv->Get()->ToString()});

        const auto rid = PrivEv->Get()->ReplicationId;
        Replication = Self->Find(rid);

        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Cannot drop unknown replication",
                {"rid", rid});
            return true;
        }

        if (Replication->GetDropMode() == TReplication::EDropMode::ForceLocal
            && Replication->GetState() == TReplication::EState::Done)
        {
            NIceDb::TNiceDb db(txc.DB);
            BeginForceRemoval(db, ctx);
            return true;
        }

        if (Replication->GetState() != TReplication::EState::Removing) {
            YDB_LOG_WARN_CTX(ctx, "Replication state mismatch",
                {"rid", rid},
                {"state", Replication->GetState()});
            return true;
        }

        if (!Replication->GetTargets().empty()) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Delete();

        if (const auto& op = Replication->GetDropOp()) {
            auto ev = MakeHolder<TEvController::TEvDropReplicationResult>();
            ev->Record.MutableOperationId()->SetTxId(op->OperationId.first);
            ev->Record.MutableOperationId()->SetPartId(op->OperationId.second);
            ev->Record.SetOrigin(Self->TabletID());
            ev->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::SUCCESS);
            Result = MakeHolder<IEventHandle>(op->Sender, ctx.SelfID, ev.Release());
        }

        Replication->Shutdown(ctx);
        Self->Remove(rid);
        Replication.Reset();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        for (const auto& key : AlterersToStop) {
            Self->StopSchemaChangeDstAlter(key, ctx);
        }

        if (Result) {
            ctx.Send(Result.Release());
        }

        if (Replication) {
            if (Replication->GetDropMode() == TReplication::EDropMode::ForceLocal) {
                // Stop source readers immediately, including siblings of a
                // target whose local DDL/flush still owns its completion.
                // Keep worker metadata until the existing removal path runs.
                for (const auto& [id, worker] : Self->Workers) {
                    if (id.ReplicationId() == Replication->GetId() && worker.HasSession()) {
                        Self->StopQueue.emplace(id, worker.GetSession());
                    }
                }
                Self->ScheduleProcessQueues();
            }
            Replication->Progress(ctx);
            if (Replication->GetDropMode() == TReplication::EDropMode::ForceLocal
                && Self->DeferredAlters.contains(Replication->GetId()))
            {
                Self->RunTxIndexBuild(ctx);
                if (!Self->HasPendingAlter(Replication->GetId())) {
                    ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(Replication->GetId()));
                }
            }
        }
    }

}; // TTxDropReplication

void TController::RunTxDropReplication(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropReplication(this, ev), ctx);
}

void TController::RunTxDropReplication(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropReplication(this, ev), ctx);
}

}
