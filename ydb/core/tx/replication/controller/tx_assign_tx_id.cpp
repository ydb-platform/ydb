#include "controller_impl.h"
#include "global_write_txs_repository.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxAssignTxId: public TTxBase {
    THashMap<ui32, THolder<TEvService::TEvTxIdResult>> Result;
    bool TxIdsExhausted = false;

    TVector<TRowVersion> GetPendingVersions() const {
        TVector<TRowVersion> versions;
        versions.reserve(Self->PendingTxId.size());
        for (const auto& [version, _] : Self->PendingTxId) {
            versions.push_back(version);
        }
        return versions;
    }

    TVector<TRowVersion> GetIndexBuildJoinVersions() const {
        TVector<TRowVersion> versions;
        for (const auto& [_, build] : Self->IndexBuilds) {
            if (build.HasJoinVersion()) {
                versions.push_back(TRowVersion::FromProto(build.GetJoinVersion()));
            }
        }
        return versions;
    }

    void Reply(const TMap<TRowVersion, ui64>& pendingVersionToWriteTxId) {
        for (const auto& [version, writeTxId] : pendingVersionToWriteTxId) {
            const auto it = Self->PendingTxId.find(version);
            Y_ABORT_UNLESS(it != Self->PendingTxId.end());

            for (const auto nodeId : it->second) {
                auto& ev = Result[nodeId];
                if (!ev) {
                    ev.Reset(new TEvService::TEvTxIdResult(Self->TabletID(), Self->Executor()->Generation()));
                }

                auto& item = *ev->Record.AddVersionTxIds();
                version.ToProto(item.MutableVersion());
                item.SetTxId(writeTxId);
            }

            Self->PendingTxId.erase(it);
        }
    }

public:
    explicit TTxAssignTxId(TController* self)
        : TTxBase("TxAssignTxId", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ASSIGN_TX_ID;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"pending", Self->PendingTxId.size()},
            {"assigned", Self->GlobalWriteTxs.CountOpenWriteTxs()},
            {"allocated", Self->GlobalWriteTxs.CountFreeWriteTxIds()});

        const auto replication = Self->GetSingle();
        if (replication && Self->ReplicationsWithSourceOffsetMismatch.contains(replication->GetId())) {
            Self->PendingTxId.clear();
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        const auto assignment = Self->GlobalWriteTxs.AssignWriteTxs(
            GetPendingVersions(), GetIndexBuildJoinVersions(), !Self->IndexBuilds.empty());
        TGlobalWriteTxsRepository::SaveChanges(db, Self->GlobalWriteTxs);
        Reply(assignment.PendingVersionToWriteTxId);

        if (assignment.OpenWriteTxLimitReached) {
            Self->TabletCounters->Cumulative()[COUNTER_TOO_MANY_OPEN_TX_IDS] += 1;
        }
        if (assignment.FreeWriteTxIdsExhausted) {
            Self->TabletCounters->Cumulative()[COUNTER_TX_IDS_EXHAUSTED] += 1;
        }
        TxIdsExhausted = !Self->PendingTxId.empty();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"pending", Self->PendingTxId.size()},
            {"assigned", Self->GlobalWriteTxs.CountOpenWriteTxs()},
            {"allocated", Self->GlobalWriteTxs.CountFreeWriteTxIds()},
            {"exhausted", TxIdsExhausted});

        Self->TabletCounters->Simple()[COUNTER_PENDING_VERSIONS] = Self->PendingTxId.size();
        Self->TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = Self->GlobalWriteTxs.CountFreeWriteTxIds();
        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->GlobalWriteTxs.CountOpenWriteTxs();

        if (Self->ReplicationsWithSourceOffsetMismatch.empty()) {
            for (auto& [nodeId, ev] : Result) {
                ctx.Send(MakeReplicationServiceId(nodeId), std::move(ev));
            }
        }

        Self->AllocateTxIds(ctx);

        if (!TxIdsExhausted && Self->PendingTxId) {
            Self->Execute(new TTxAssignTxId(Self), ctx);
        } else {
            Self->AssignTxIdInFlight = false;
        }
    }

}; // TTxAssignTxId

void TController::AllocateTxIds(const TActorContext& ctx) {
    const auto replication = GetSingle();
    if (replication && ReplicationsWithSourceOffsetMismatch.contains(replication->GetId())) {
        return;
    }
    if (AllocateTxIdInFlight || GlobalWriteTxs.CountFreeWriteTxIds() >= MinAllocatedTxIds) {
        return;
    }

    AllocateTxIdInFlight = true;
    ctx.Send(TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(MaxOpenTxIds));
}

void TController::RunTxAssignTxId(const TActorContext& ctx) {
    if (!AssignTxIdInFlight) {
        AssignTxIdInFlight = true;
        Execute(new TTxAssignTxId(this), ctx);
    }
}

void TController::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX(ctx, "Handle",
        {"ev", ev->Get()->ToString()});

    GlobalWriteTxs.AddFreeWriteTxIds(ev->Get()->TxIds);
    AllocateTxIdInFlight = false;

    TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = GlobalWriteTxs.CountFreeWriteTxIds();
    RunTxAssignTxId(ctx);
    RunTxIndexBuild(ctx);
}

}
