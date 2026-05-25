#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxDropStreamResult: public TTxBase {
    TEvPrivate::TEvDropStreamResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxDropStreamResult(TController* self, TEvPrivate::TEvDropStreamResult::TPtr& ev)
        : TTxBase("TxDropStreamResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_STREAM_RESULT;
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

        if (target->GetStreamState() != TReplication::EStreamState::Removing) {
            YDB_LOG_CTX_WARN(ctx, "Stream state mismatch",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetStreamState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            YDB_LOG_CTX_NOTICE(ctx, "Stream dropped",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
        } else {
            const auto& status = Ev->Get()->Status;
            YDB_LOG_CTX_ERROR(ctx, "Drop stream error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"status", status.GetStatus()},
                {"issue", status.GetIssues().ToOneLineString()});
        }

        NIceDb::TNiceDb db(txc.DB);
        if (target->GetDstState() == TReplication::EDstState::Removing) {
            target->SetStreamState(TReplication::EStreamState::Removed);
            db.Table<Schema::SrcStreams>().Key(rid, tid).Update<Schema::SrcStreams::State>(target->GetStreamState());
        } else {
            db.Table<Schema::Targets>().Key(rid, tid).Delete();
            db.Table<Schema::SrcStreams>().Key(rid, tid).Delete();
            for (const auto wid : target->GetWorkers()) {
                db.Table<Schema::Workers>().Key(rid, tid, wid).Delete();
            }
            Replication->RemoveTarget(tid);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropStreamResult

void TController::RunTxDropStreamResult(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropStreamResult(this, ev), ctx);
}

}
