#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

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
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"LogPrefix", LogPrefix},
            {"Execute", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_CTX_WARN(ctx, "Unknown replication",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_CTX_WARN(ctx, "Unknown target",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        if (target->GetDstState() != TReplication::EDstState::Alter) {
            YDB_LOG_CTX_WARN(ctx, "Dst state mismatch",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetDstState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstState(NextState(Replication->GetDesiredState()));
            target->UpdateConfig(Replication->GetConfig());

            YDB_LOG_CTX_NOTICE(ctx, "Target dst altered",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});

            if (Replication->CheckAlterDone()) {
                YDB_LOG_CTX_NOTICE(ctx, "Replication altered",
                    {"LogPrefix", LogPrefix},
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

            YDB_LOG_CTX_ERROR(ctx, "Alter dst error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"#_NKikimrScheme::EStatus_Name(Ev->Get()->Status)", NKikimrScheme::EStatus_Name(Ev->Get()->Status)},
                {"#_Ev->Get()->Error", Ev->Get()->Error});
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
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxAlterDstResult

void TController::RunTxAlterDstResult(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAlterDstResult(this, ev), ctx);
}

}

