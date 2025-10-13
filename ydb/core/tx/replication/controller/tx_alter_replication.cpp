#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

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
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvAlterReplicationResult>();
        Result->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
        Result->Record.SetOrigin(Self->TabletID());

        const auto pathId = TPathId::FromProto(record.GetPathId());
        Replication = Self->Find(pathId);

        if (!Replication) {
            CLOG_W(ctx, "Cannot alter unknown replication"
                << ": pathId# " << pathId);

            Result->Record.SetStatus(NKikimrReplication::TEvAlterReplicationResult::UNKNOWN);
            return true;
        }

        bool alter = false;

        const auto& oldConfig = Replication->GetConfig();
        const auto& newConfig = record.GetConfig();

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

        Replication->SetConfig(std::move(*record.MutableConfig()));
        Replication->ResetCredentials(ctx);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::Config>(record.GetConfig().SerializeAsString()),
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
            CLOG_N(ctx, "Alter replication"
                << ": rid# " << Replication->GetId()
                << ", pathId# " << pathId);
        } else {
            Replication.Reset();
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

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
