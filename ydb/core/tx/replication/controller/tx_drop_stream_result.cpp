#include "controller_impl.h"

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
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        if (target->GetStreamState() != TReplication::EStreamState::Removing) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Stream state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetStreamState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Stream dropped"
                << ": rid# " << rid
                << ", tid# " << tid);
        } else {
            const auto& status = Ev->Get()->Status;
            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Drop stream error"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", status# " << status.GetStatus()
                << ", issue# " << status.GetIssues().ToOneLineString());
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
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropStreamResult

void TController::RunTxDropStreamResult(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropStreamResult(this, ev), ctx);
}

}
