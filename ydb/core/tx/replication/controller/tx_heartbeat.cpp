#include "controller_impl.h"

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

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

bool TController::HasFreshHeartbeatQuorum(const TSchemaBarrier& barrier) const {
    if (Workers.empty()) {
        return false;
    }

    const auto barrierVersion = TRowVersion::FromProto(barrier.Schema.GetVersion());
    return AllOf(Workers, [barrierVersion](const auto& item) {
        return item.second.HasHeartbeat() && item.second.GetHeartbeat() > barrierVersion;
    });
}

bool TController::HasPendingTargetFlushTxId(const TSchemaBarrier& barrier) const {
    return AnyOf(AssignedTxIds, [&barrier](const auto& assigned) {
        return ::Find(barrier.TargetFlushTxIds, assigned.second) != barrier.TargetFlushTxIds.end();
    });
}

bool TController::BlocksGlobalCommit(const TSchemaBarrier& barrier) const {
    switch (barrier.Phase) {
    case ESchemaBarrierPhase::FlushingTarget:
    case ESchemaBarrierPhase::Altering:
        return true;
    case ESchemaBarrierPhase::Verifying:
        return !HasFreshHeartbeatQuorum(barrier);
    case ESchemaBarrierPhase::Collecting:
    case ESchemaBarrierPhase::Applied:
    case ESchemaBarrierPhase::Error:
        return false;
    }
    Y_ABORT("Unexpected schema barrier phase");
}

void TController::AdvanceVerifyingSchemaBarriers(NIceDb::TNiceDb& db) {
    for (auto& [key, barrier] : SchemaBarriers) {
        if (barrier.Phase != ESchemaBarrierPhase::Verifying
            || !HasFreshHeartbeatQuorum(barrier)
            || HasPendingTargetFlushTxId(barrier))
        {
            continue;
        }

        barrier.Phase = ESchemaBarrierPhase::Applied;
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::SchemaBarrierPhase>(static_cast<ui8>(barrier.Phase)));
    }
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"pending", Self->PendingHeartbeats.size()});

        if (Self->Workers.empty()) {
            YDB_LOG_WARN_CTX(ctx, "There are no workers");
            return true;
        }

        auto replication = Self->GetSingle();
        if (!replication) {
            YDB_LOG_ERROR_CTX(ctx, "Ambiguous replication instance");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        ui32 i = 0;
        while (!Self->PendingHeartbeats.empty() && i++ < MaxBatchSize) {
            auto it = Self->PendingHeartbeats.begin();
            const auto& id = it->first;
            const auto& version = it->second;

            if (!Self->Workers.contains(id) || Self->RemoveQueue.contains(id)) {
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

        Self->AdvanceVerifyingSchemaBarriers(db);

        if (AnyOf(Self->SchemaBarriers, [this](const auto& item) {
            return Self->BlocksGlobalCommit(item.second);
        })) {
            return true; // global consistency is temporarily degraded
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"pending", Self->PendingHeartbeats.size()});

        Self->TabletCounters->Simple()[COUNTER_WORKERS_WITH_HEARTBEAT] = Self->WorkersWithHeartbeat.size();
        Self->TabletCounters->Simple()[COUNTER_WORKERS_PENDING_HEARTBEAT] = Self->PendingHeartbeats.size();

        if (auto& ev = CommitProposal) {
            YDB_LOG_NOTICE_CTX(ctx, "Propose commit",
                {"writeTxId", Self->CommittingTxId});
            ctx.Send(MakeTxProxyID(), std::move(ev), 0, Self->CommittingTxId);
        }

        for (const auto replicationId : Self->DeferredAlters) {
            if (!Self->HasActiveSchemaBarrier(replicationId)) {
                ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(replicationId));
            }
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"writeTxId", Self->CommittingTxId});

        auto replication = Self->GetSingle();
        if (!replication) {
            YDB_LOG_ERROR_CTX(ctx, "Ambiguous replication instance");
            return true;
        }

        auto it = Self->AssignedTxIds.begin();
        Y_ABORT_UNLESS(it != Self->AssignedTxIds.end());
        Y_ABORT_UNLESS(it->second == Self->CommittingTxId);

        const auto& record = Status->Get()->Record;
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
        if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            YDB_LOG_WARN_CTX(ctx, "Error committing changes",
                {"writeTxId", Self->CommittingTxId},
                {"issues", NYql::IssuesFromMessageAsString(record.GetIssues())});
            Self->TabletCounters->Cumulative()[COUNTER_ERROR_COMMITTING_CHANGES] += 1;

            CommitProposal = MakeCommitProposal(Self->CommittingTxId, replication->GetTargetTablePaths());
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        db.Table<Schema::TxIds>().Key(it->first.Step, it->first.TxId).Delete();
        it = Self->AssignedTxIds.erase(it);
        Self->CommittingTxId = 0;

        Self->AdvanceVerifyingSchemaBarriers(db);

        if (it == Self->AssignedTxIds.end() || Self->WorkersByHeartbeat.empty()) {
            return true;
        }

        if (AnyOf(Self->SchemaBarriers, [this](const auto& item) {
            return Self->BlocksGlobalCommit(item.second);
        })) {
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
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->AssignedTxIds.size();

        if (auto& ev = CommitProposal) {
            YDB_LOG_NOTICE_CTX(ctx, "Propose commit",
                {"writeTxId", Self->CommittingTxId});
            ctx.Send(MakeTxProxyID(), std::move(ev), 0, Self->CommittingTxId);
        }

        if (!Self->CommittingTxId) {
            for (const auto& [key, barrier] : Self->SchemaBarriers) {
                if (barrier.Phase == ESchemaBarrierPhase::FlushingTarget) {
                    Self->StartSchemaChangeTargetFlush(key, ctx);
                }
            }
        }

        for (const auto replicationId : Self->DeferredAlters) {
            if (!Self->HasActiveSchemaBarrier(replicationId)) {
                ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(replicationId));
            }
        }
    }

}; // TTxCommitChanges

void TController::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX(ctx, "Handle",
        {"ev", ev->Get()->ToString()});

    if (auto flush = SchemaTargetFlushes.find(ev->Cookie); flush != SchemaTargetFlushes.end()) {
        const auto key = flush->second;
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(
            ev->Get()->Record.GetStatus());
        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            SchemaTargetFlushes.erase(flush);
            const auto barrier = SchemaBarriers.find(key);
            if (barrier != SchemaBarriers.end()) {
                ++barrier->second.NextTargetFlushTxId;
                StartSchemaChangeTargetFlush(key, ctx);
            }
        } else {
            const auto barrier = SchemaBarriers.find(key);
            auto replication = Find(key.first);
            auto* target = FindTarget(TWorkerId(key.first, key.second, 0));
            if (barrier != SchemaBarriers.end()
                && replication
                && replication->GetState() != TReplication::EState::Removing
                && target)
            {
                ctx.Send(MakeTxProxyID(),
                    MakeCommitProposal(ev->Cookie, {target->GetDstPath()}).Release(), 0, ev->Cookie);
            }
        }
        return;
    }

    if (ev->Cookie != CommittingTxId) {
        YDB_LOG_ERROR_CTX(ctx, "Cookie mismatch",
            {"expected", CommittingTxId},
            {"got", ev->Cookie});
        return;
    }

    Execute(new TTxCommitChanges(this, ev), ctx);
}

}
