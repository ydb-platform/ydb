#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxCreateDstResult: public TTxBase {
    TEvPrivate::TEvCreateDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxCreateDstResult(TController* self, TEvPrivate::TEvCreateDstResult::TPtr& ev)
        : TTxBase("TxCreateDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_DST_RESULT;
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

        if (target->GetDstState() != TReplication::EDstState::Creating && target->GetDstState() != TReplication::EDstState::Alter) {
            YDB_LOG_CTX_WARN(ctx, "Dst state mismatch",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetDstState()});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstPathId(Ev->Get()->DstPathId);
            target->SetDstState(TReplication::EDstState::Ready);

            YDB_LOG_CTX_NOTICE(ctx, "Target dst created",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid},
                {"pathId", Ev->Get()->DstPathId});
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Create dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            YDB_LOG_CTX_ERROR(ctx, "Create dst error",
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
            NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(target->GetDstPathId().OwnerId),
            NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(target->GetDstPathId().LocalPathId),
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
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

}; // TTxCreateDstResult

void TController::RunTxCreateDstResult(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateDstResult(this, ev), ctx);
}

}
