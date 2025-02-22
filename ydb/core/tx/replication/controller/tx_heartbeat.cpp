#include "controller_impl.h"

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NReplication::NController {

THolder<TEvTxUserProxy::TEvProposeTransaction> MakeCommitProposal(ui64 writeTxId, const TVector<TString>& tables) {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto& tx = *ev->Record.MutableTransaction()->MutableCommitWrites();

    tx.SetWriteTxId(writeTxId);
    for (const auto& path : tables) {
        tx.AddTables()->SetTablePath(path);
    }

    return ev;
}

class TController::TTxHeartbeat: public TTxBase {
    // TODO(ilnaz): configurable
    static constexpr ui32 MaxBatchSize = 1000;

    THolder<TEvTxUserProxy::TEvProposeTransaction> CommitProposal;

public:
    explicit TTxHeartbeat(TController* self)
        : TTxBase("TxHeartbeat", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_HEARTBEAT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute"
            << ": pending# " << Self->PendingHeartbeats.size());

        if (Self->Workers.empty()) {
            CLOG_W(ctx, "There are no workers");
            return true;
        }

        auto replication = Self->GetSingle();
        if (!replication) {
            CLOG_E(ctx, "Ambiguous replication instance");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        ui32 i = 0;
        while (!Self->PendingHeartbeats.empty() && i++ < MaxBatchSize) {
            auto it = Self->PendingHeartbeats.begin();
            const auto& id = it->first;
            const auto& version = it->second;

            if (!Self->Workers.contains(id)) {
                Self->PendingHeartbeats.erase(it);
                continue;
            }

            auto& worker = Self->Workers[id];
            if (worker.HasHeartbeat()) {
                const auto& prevVersion = worker.GetHeartbeat();
                if (version < prevVersion) {
                    Self->PendingHeartbeats.erase(it);
                    continue;
                }

                auto jt = Self->WorkersByHeartbeat.find(prevVersion);
                if (jt != Self->WorkersByHeartbeat.end()) {
                    jt->second.erase(id);
                    if (jt->second.empty()) {
                        Self->WorkersByHeartbeat.erase(jt);
                    }
                }
            }

            worker.SetHeartbeat(version);
            Self->WorkersWithHeartbeat.insert(id);
            Self->WorkersByHeartbeat[version].insert(id);

            db.Table<Schema::Workers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
                NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(version.Step),
                NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(version.TxId)
            );

            Self->PendingHeartbeats.erase(it);
        }

        if (Self->Workers.size() != Self->WorkersWithHeartbeat.size()) {
            return true; // no quorum
        }

        if (Self->CommittingTxId) {
            return true; // another commit in progress
        }

        if (Self->AssignedTxIds.empty()) {
            return true; // nothing to commit
        }

        Y_ABORT_UNLESS(!Self->WorkersByHeartbeat.empty());
        if (Self->WorkersByHeartbeat.begin()->first < Self->AssignedTxIds.begin()->first) {
            return true; // version has not been changed
        }

        Self->CommittingTxId = Self->AssignedTxIds.begin()->second;
        CommitProposal = MakeCommitProposal(Self->CommittingTxId, replication->GetTargetTablePaths());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete"
            << ": pending# " << Self->PendingHeartbeats.size());

        Self->TabletCounters->Simple()[COUNTER_WORKERS_WITH_HEARTBEAT] = Self->WorkersWithHeartbeat.size();
        Self->TabletCounters->Simple()[COUNTER_WORKERS_PENDING_HEARTBEAT] = Self->PendingHeartbeats.size();

        if (auto& ev = CommitProposal) {
            CLOG_N(ctx, "Propose commit"
                << ": writeTxId# " << Self->CommittingTxId);
            ctx.Send(MakeTxProxyID(), std::move(ev), 0, Self->CommittingTxId);
        }

        if (Self->PendingHeartbeats) {
            Self->Execute(new TTxHeartbeat(Self), ctx);
        } else {
            Self->ProcessHeartbeatsInFlight = false;
        }
    }

}; // TTxHeartbeat

void TController::RunTxHeartbeat(const TActorContext& ctx) {
    if (!ProcessHeartbeatsInFlight) {
        ProcessHeartbeatsInFlight = true;
        Execute(new TTxHeartbeat(this), ctx);
    }
}

class TController::TTxCommitChanges: public TTxBase {
    TEvTxUserProxy::TEvProposeTransactionStatus::TPtr Status;
    THolder<TEvTxUserProxy::TEvProposeTransaction> CommitProposal;

public:
    explicit TTxCommitChanges(TController* self, TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev)
        : TTxBase("TxCommitChanges", self)
        , Status(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_COMMIT_CHANGES;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute"
            << ": writeTxId# " << Self->CommittingTxId);

        auto replication = Self->GetSingle();
        if (!replication) {
            CLOG_E(ctx, "Ambiguous replication instance");
            return true;
        }

        auto it = Self->AssignedTxIds.begin();
        Y_ABORT_UNLESS(it != Self->AssignedTxIds.end());
        Y_ABORT_UNLESS(it->second == Self->CommittingTxId);

        const auto& record = Status->Get()->Record;
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
        if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            CLOG_W(ctx, "Error committing changes"
                << ": writeTxId# " << Self->CommittingTxId
                << ", issues# " << NYql::IssuesFromMessageAsString(record.GetIssues()));
            Self->TabletCounters->Cumulative()[COUNTER_ERROR_COMMITTING_CHANGES] += 1;

            CommitProposal = MakeCommitProposal(Self->CommittingTxId, replication->GetTargetTablePaths());
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        db.Table<Schema::TxIds>().Key(it->first.Step, it->first.TxId).Delete();
        it = Self->AssignedTxIds.erase(it);
        Self->CommittingTxId = 0;

        if (it == Self->AssignedTxIds.end() || Self->WorkersByHeartbeat.empty()) {
            return true;
        }

        if (Self->WorkersByHeartbeat.begin()->first < it->first) {
            return true;
        }

        Self->CommittingTxId = Self->AssignedTxIds.begin()->second;
        CommitProposal = MakeCommitProposal(Self->CommittingTxId, replication->GetTargetTablePaths());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->AssignedTxIds.size();

        if (auto& ev = CommitProposal) {
            CLOG_N(ctx, "Propose commit"
                << ": writeTxId# " << Self->CommittingTxId);
            ctx.Send(MakeTxProxyID(), std::move(ev), 0, Self->CommittingTxId);
        }
    }

}; // TTxCommitChanges

void TController::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    if (ev->Cookie != CommittingTxId) {
        CLOG_E(ctx, "Cookie mismatch"
            << ": expected# " << CommittingTxId
            << ", got# " << ev->Cookie);
        return;
    }

    Execute(new TTxCommitChanges(this, ev), ctx);
}

}
