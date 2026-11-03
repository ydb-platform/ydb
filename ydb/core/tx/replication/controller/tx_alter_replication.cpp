#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

bool RetryPausedBootstrap(
        TReplication& replication,
        NIceDb::TNiceDb& db,
        const TActorContext& ctx)
{
    if (replication.GetConfig().GetInitialScan()
        || replication.GetDesiredState() != TReplication::EState::Paused
        || replication.GetState() != TReplication::EState::Error)
    {
        return false;
    }

    // Restrict config-only recovery to source preparation. An ordinary PAUSE
    // can retain attached destinations and unrelated errors that must survive.
    for (const auto* target : replication.GetTargets()) {
        if (target->GetDstState() != TReplication::EDstState::Creating
            || target->GetDstPathId()
            || target->GetPendingDstPathId()
            || !target->GetWorkers().empty())
        {
            return false;
        }
    }

    // A public config ALTER is an explicit retry even if the credential
    // reference is unchanged (its secret or source ACL may be fixed).
    // Persist the reset before Progress can start a replacement creator.
    replication.SetState(TReplication::EState::Paused);
    for (auto* target : replication.GetTargets()) {
        if (target->GetStreamState() != TReplication::EStreamState::Error) {
            continue;
        }

        target->Shutdown(ctx);
        target->SetStreamState(TReplication::EStreamState::Creating);
        target->SetIssue({});
        db.Table<TControllerSchema::SrcStreams>().Key(replication.GetId(), target->GetId()).Update(
            NIceDb::TUpdate<TControllerSchema::SrcStreams::State>(target->GetStreamState()));
        db.Table<TControllerSchema::Targets>().Key(replication.GetId(), target->GetId()).Update(
            NIceDb::TUpdate<TControllerSchema::Targets::Issue>(target->GetIssue()));
    }

    return true;
}

} // anonymous namespace

class TController::TTxAlterReplication: public TTxBase {
    TEvController::TEvAlterReplication::TPtr Ev;
    THolder<TEvController::TEvAlterReplicationResult> Result;
    TReplication::TPtr Replication;

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
        if (!alter && RetryPausedBootstrap(*Replication, db, ctx)) {
            issue.clear();
            desiredState = Replication->GetDesiredState();
        }
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Config>(Replication->GetConfig().SerializeAsString()),
            NIceDb::TUpdate<Schema::Replications::DesiredState>(desiredState),
            NIceDb::TUpdate<Schema::Replications::Issue>(issue)
        );

        if (!alter) {
            Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
            return true;
        }

        Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            // An attachment may already be submitted to SchemeShard; let it finish before DONE.
            // Progress notifies its creator to stop source revalidation and reconcile locally.
            if (target->GetDstState() != TReplication::EDstState::Attaching) {
                target->Shutdown(ctx);
                target->SetDstState(TReplication::EDstState::Alter);
            }
            if (target->GetStreamState() == TReplication::EStreamState::Error && desiredState == TReplication::EState::Ready) {
                target->SetStreamState(TReplication::EStreamState::Creating);
                db.Table<Schema::SrcStreams>().Key(Replication->GetId(), tid).Update(
                    NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()));
            }
            db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
            );

            alter = true;
        }

        if (Replication->CheckAlterDone()) {
            Replication->SetState(desiredState);
            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::State>(desiredState)
            );
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

}
