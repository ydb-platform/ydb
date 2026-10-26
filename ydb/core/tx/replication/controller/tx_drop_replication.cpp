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

    bool ExecutePub(TTransactionContext& txc, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", PubEv->Get()->ToString()});

        const auto& record = PubEv->Get()->Record;
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

        if (Replication->GetState() == TReplication::EState::Removing) {
            Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

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

        if (Replication->GetState() != TReplication::EState::Removing) {
            YDB_LOG_WARN_CTX(ctx, "Replication state mismatch",
                {"rid", rid},
                {"state", Replication->GetState()});
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
            Replication->Progress(ctx);
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
