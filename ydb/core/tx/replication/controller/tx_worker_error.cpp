#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxWorkerError: public TTxBase {
    const TWorkerId WorkerId;
    const TString Error;
    const bool SourceOffsetMismatch;
    const TMaybe<ui64> SourceCommittedOffset;
    TMaybe<ui32> RestartWorkerNode;
    bool StopWorkers = false;

public:
    explicit TTxWorkerError(TController* self, const TWorkerId& id, const TString& error, bool sourceOffsetMismatch, TMaybe<ui64> sourceCommittedOffset)
        : TTxBase("TxWorkerError", self)
        , WorkerId(id)
        , Error(error)
        , SourceOffsetMismatch(sourceOffsetMismatch)
        , SourceCommittedOffset(sourceCommittedOffset)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WORKER_ERROR;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"workerId", WorkerId},
            {"error", Error});

        auto replication = Self->Find(WorkerId.ReplicationId());
        if (!replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"rid", WorkerId.ReplicationId()});
            return true;
        }

        if (Self->ReplicationsWithSourceOffsetMismatch.contains(WorkerId.ReplicationId())
            || replication->GetState() == TReplication::EState::Removing)
        {
            YDB_LOG_WARN_CTX(ctx, "Replication is being removed",
                {"rid", WorkerId.ReplicationId()});
            return true;
        }

        auto* target = replication->FindTarget(WorkerId.TargetId());
        if (!target) {
            YDB_LOG_WARN_CTX(ctx, "Unknown target",
                {"rid", WorkerId.ReplicationId()},
                {"tid", WorkerId.TargetId()});
            return true;
        }

        if (SourceOffsetMismatch && Self->TracksSourceOffsets(WorkerId)) {
            const auto* savedOffset = Self->ProcessedSourceOffsets.FindPtr(WorkerId);
            if (!savedOffset || (SourceCommittedOffset && *SourceCommittedOffset <= savedOffset->NextOffset)) {
                if (const auto* worker = Self->Workers.FindPtr(WorkerId); worker && worker->HasSession()) {
                    RestartWorkerNode = worker->GetSession();
                }
                return true;
            }
            NIceDb::TNiceDb db(txc.DB);
            replication->SetState(TReplication::EState::Error, TStringBuilder() << Error << " Worker " << WorkerId << ".");
            Self->ReplicationsWithSourceOffsetMismatch.insert(WorkerId.ReplicationId());
            db.Table<Schema::Replications>().Key(WorkerId.ReplicationId()).Update(
                NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
                NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue()),
                NIceDb::TUpdate<Schema::Replications::SourceOffsetMismatch>(true));
            YDB_LOG_ERROR_CTX(ctx, "Destination source offset mismatch", {"worker", WorkerId}, {"issue", replication->GetIssue()});
            StopWorkers = true;
            return true;
        }

        YDB_LOG_ERROR_CTX(ctx, "Worker error",
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (RestartWorkerNode && Self->HasWorkerSession(WorkerId, *RestartWorkerNode)) {
            Self->StopQueue.emplace(WorkerId, *RestartWorkerNode);
            Self->ScheduleProcessQueues();
            return;
        }
        if (StopWorkers) {
            Self->StopWorkersForMismatch(WorkerId.ReplicationId(), ctx);
        }
    }

}; // TTxWorkerError

void TController::RunTxWorkerError(const TWorkerId& id, const TString& error, const TActorContext& ctx, bool sourceOffsetMismatch, TMaybe<ui64> sourceCommittedOffset) {
    Execute(new TTxWorkerError(this, id, error, sourceOffsetMismatch, sourceCommittedOffset), ctx);
}

}
