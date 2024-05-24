#include "controller_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NKikimr::NReplication::NController {

class TController::TTxDescribeReplication: public TTxBase {
    TEvController::TEvDescribeReplication::TPtr Ev;
    THolder<TEvController::TEvDescribeReplicationResult> Result;

public:
    explicit TTxDescribeReplication(TController* self, TEvController::TEvDescribeReplication::TPtr& ev)
        : TTxBase("TxDescribeReplication", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DESCRIBE_REPLICATION;
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();

        const auto pathId = PathIdFromPathId(record.GetPathId());
        auto replication = Self->Find(pathId);

        if (!replication) {
            Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
            return true;
        }

        Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        Result->Record.MutableConnectionParams()->CopyFrom(replication->GetConfig().GetSrcConnectionParams());

        for (ui64 tid = 0; tid < replication->GetNextTargetId(); ++tid) {
            auto* target = replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            auto& item = *Result->Record.AddTargets();
            item.SetSrcPath(target->GetSrcPath());
            item.SetDstPath(target->GetDstPath());
        }

        auto& state = *Result->Record.MutableState();
        switch (replication->GetState()) {
        case TReplication::EState::Ready:
        case TReplication::EState::Removing:
            state.MutableStandBy();
            break;
        case TReplication::EState::Error:
            if (auto issue = state.MutableError()->AddIssues()) {
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(replication->GetIssue());
            }
            break;
        // TODO: 'done' state
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Result) {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }
    }

}; // TTxDescribeReplication

void TController::RunTxDescribeReplication(TEvController::TEvDescribeReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDescribeReplication(this, ev), ctx);
}

}
