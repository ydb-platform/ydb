#include "controller_impl.h"

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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"rid", rid});
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_WARN_CTX(ctx, "Unknown target",
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        const auto build = Self->IndexBuilds.find({rid, tid});
        if (build != Self->IndexBuilds.end() && Self->IsCancelledIndexBuild(build->second)) {
            YDB_LOG_DEBUG_CTX(ctx, "Ignore stream creation result for cancelled index build",
                {"rid", rid}, {"tid", tid});
            Replication.Reset();
            return true;
        }

        const bool discoveringCapability = target->GetStreamState() == TReplication::EStreamState::Ready
            && target->GetKind() == TReplication::ETargetKind::Table
            && !target->GetStreamSchemaChanges().has_value();
        if (target->GetStreamState() != TReplication::EStreamState::Creating && !discoveringCapability) {
            YDB_LOG_WARN_CTX(ctx, "Stream state mismatch",
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetStreamState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetStreamState(TReplication::EStreamState::Ready);
            target->SetStreamSchemaChanges(Ev->Get()->SchemaChanges);

            YDB_LOG_NOTICE_CTX(ctx, "Stream ready",
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

            YDB_LOG_ERROR_CTX(ctx, "Create stream error",
                {"rid", rid},
                {"tid", tid},
                {"status", status.GetStatus()},
                {"issue", status.GetIssues().ToOneLineString()});
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SrcStreams>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()),
            NIceDb::TUpdate<Schema::SrcStreams::SchemaChanges>(target->GetStreamSchemaChanges().value_or(false))
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update<Schema::Targets::Issue>(target->GetIssue());
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxCreateStreamResult

void TController::RunTxCreateStreamResult(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateStreamResult(this, ev), ctx);
}

}
