#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxAlterDstResult: public TTxBase {
    TEvPrivate::TEvAlterDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxAlterDstResult(TController* self, TEvPrivate::TEvAlterDstResult::TPtr& ev)
        : TTxBase("TxAlterDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ALTER_DST_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "Dump logPrefix, execute",
            {"logPrefix", LogPrefix},
            {"execute", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"logPrefix", LogPrefix},
                {"rid", rid});
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_WARN_CTX(ctx, "Unknown target",
                {"logPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        if (target->GetDstState() != TReplication::EDstState::Alter) {
            YDB_LOG_WARN_CTX(ctx, "Dst state mismatch",
                {"logPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetDstState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstState(NextState(Replication->GetDesiredState()));
            target->UpdateConfig(Replication->GetConfig());

            YDB_LOG_NOTICE_CTX(ctx, "Target dst altered",
                {"logPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});

            if (Replication->CheckAlterDone()) {
                YDB_LOG_NOTICE_CTX(ctx, "Replication altered",
                    {"logPrefix", LogPrefix},
                    {"rid", rid},
                    {"state", Replication->GetDesiredState()});
                Replication->SetState(Replication->GetDesiredState());
            }
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Alter dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            YDB_LOG_ERROR_CTX(ctx, "Alter dst error",
                {"logPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"status", NKikimrScheme::EStatus_Name(Ev->Get()->Status)},
                {"error", Ev->Get()->Error});
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        if (Replication->GetState() != TReplication::EState::Ready) {
            Replication.Reset();
        }

        return true;
    }

    TReplication::EDstState NextState(TReplication::EState state) {
        switch (state) {
        case TReplication::EState::Done:
            return TReplication::EDstState::Done;
        case TReplication::EState::Ready:
            return TReplication::EDstState::Ready;
        case TReplication::EState::Error:
            return TReplication::EDstState::Error;
        case TReplication::EState::Removing:
            return TReplication::EDstState::Removing;
        case TReplication::EState::Paused:
            return TReplication::EDstState::Paused;
        }
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"logPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxAlterDstResult

void TController::RunTxAlterDstResult(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAlterDstResult(this, ev), ctx);
}

}

