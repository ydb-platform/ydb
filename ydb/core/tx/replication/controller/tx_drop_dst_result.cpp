#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxDropDstResult: public TTxBase {
    TEvPrivate::TEvDropDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxDropDstResult(TController* self, TEvPrivate::TEvDropDstResult::TPtr& ev)
        : TTxBase("TxDropDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_DST_RESULT;
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

        if (target->GetDstState() != TReplication::EDstState::Removing) {
            YDB_LOG_CTX_WARN(ctx, "Dst state mismatch",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetDstState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            YDB_LOG_CTX_NOTICE(ctx, "Target dst dropped",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
        } else {
            YDB_LOG_CTX_ERROR(ctx, "Drop dst error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"#_NKikimrScheme::EStatus_Name(Ev->Get()->Status)", NKikimrScheme::EStatus_Name(Ev->Get()->Status)},
                {"#_Ev->Get()->Error", Ev->Get()->Error});
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Targets>().Key(rid, tid).Delete();
        db.Table<Schema::SrcStreams>().Key(rid, tid).Delete();
        for (const auto wid : target->GetWorkers()) {
            db.Table<Schema::Workers>().Key(rid, tid, wid).Delete();
        }
        Replication->RemoveTarget(tid);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropDstResult

void TController::RunTxDropDstResult(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropDstResult(this, ev), ctx);
}

}
