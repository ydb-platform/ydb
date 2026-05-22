#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxCreateStreamResult: public TTxBase {
    TEvPrivate::TEvCreateStreamResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxCreateStreamResult(TController* self, TEvPrivate::TEvCreateStreamResult::TPtr& ev)
        : TTxBase("TxCreateStreamResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_STREAM_RESULT;
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

        if (target->GetStreamState() != TReplication::EStreamState::Creating) {
            YDB_LOG_CTX_WARN(ctx, "Stream state mismatch",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetStreamState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetStreamState(TReplication::EStreamState::Ready);

            YDB_LOG_CTX_NOTICE(ctx, "Stream created",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
        } else {
            const auto& status = Ev->Get()->Status;

            target->SetStreamState(TReplication::EStreamState::Error);
            target->SetIssue(TStringBuilder() << "Create stream error"
                << ": " << status.GetStatus()
                << ", " << status.GetIssues().ToOneLineString());

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            YDB_LOG_CTX_ERROR(ctx, "Create stream error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"status", status.GetStatus()},
                {"issue", status.GetIssues().ToOneLineString()});
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SrcStreams>().Key(rid, tid).Update<Schema::SrcStreams::State>(target->GetStreamState());
        db.Table<Schema::Targets>().Key(rid, tid).Update<Schema::Targets::Issue>(target->GetIssue());
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxCreateStreamResult

void TController::RunTxCreateStreamResult(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateStreamResult(this, ev), ctx);
}

}
