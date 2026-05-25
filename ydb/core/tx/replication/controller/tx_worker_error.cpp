#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxWorkerError: public TTxBase {
    const TWorkerId WorkerId;
    const TString Error;

public:
    explicit TTxWorkerError(TController* self, const TWorkerId& id, const TString& error)
        : TTxBase("TxWorkerError", self)
        , WorkerId(id)
        , Error(error)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WORKER_ERROR;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Execute",
            {"LogPrefix", LogPrefix},
            {"workerId", WorkerId},
            {"error", Error});

        auto replication = Self->Find(WorkerId.ReplicationId());
        if (!replication) {
            YDB_LOG_CTX_WARN(ctx, "Unknown replication",
                {"LogPrefix", LogPrefix},
                {"rid", WorkerId.ReplicationId()});
            return true;
        }

        if (replication->GetState() == TReplication::EState::Removing) {
            YDB_LOG_CTX_WARN(ctx, "Replication is being removed",
                {"LogPrefix", LogPrefix},
                {"rid", WorkerId.ReplicationId()});
            return true;
        }

        auto* target = replication->FindTarget(WorkerId.TargetId());
        if (!target) {
            YDB_LOG_CTX_WARN(ctx, "Unknown target",
                {"LogPrefix", LogPrefix},
                {"rid", WorkerId.ReplicationId()},
                {"tid", WorkerId.TargetId()});
            return true;
        }

        YDB_LOG_CTX_ERROR(ctx, "Worker error",
            {"LogPrefix", LogPrefix},
            {"rid", WorkerId.ReplicationId()},
            {"tid", WorkerId.TargetId()},
            {"error", Error});

        target->SetDstState(TReplication::EDstState::Error);
        target->SetIssue(Error);

        replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
            << ": " << target->GetIssue());

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(WorkerId.ReplicationId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(WorkerId.ReplicationId(), WorkerId.TargetId()).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});
    }

}; // TTxWorkerError

void TController::RunTxWorkerError(const TWorkerId& id, const TString& error, const TActorContext& ctx) {
    Execute(new TTxWorkerError(this, id, error), ctx);
}

}
