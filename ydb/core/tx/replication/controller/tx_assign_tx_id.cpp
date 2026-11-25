#include "controller_impl.h"

#include <iterator>
#include <variant>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

void TController::SaveAssignment(NIceDb::TNiceDb& db, const TRowVersion& end, TAssignedTxId assignment) {
    AssignedTxIds[end] = assignment;
    db.Table<Schema::TxIds>().Key(end.Step, end.TxId).Update(
        NIceDb::TUpdate<Schema::TxIds::WriteTxId>(assignment.WriteTxId),
        NIceDb::TUpdate<Schema::TxIds::FirstEndStep>(assignment.FirstEndStep));
}

ui64 TController::ExtendLastAssignment(NIceDb::TNiceDb& db, const TRowVersion& end) {
    const auto last = std::prev(AssignedTxIds.end());
    const auto assignment = last->second;
    db.Table<Schema::TxIds>().Key(last->first.Step, last->first.TxId).Delete();
    AssignedTxIds.erase(last);
    SaveAssignment(db, end, assignment);
    return assignment.WriteTxId;
}

ui64 TController::CoverEarlierInterval(NIceDb::TNiceDb& db, const TRowVersion& end, const TRowVersion& requested) {
    auto& assignment = AssignedTxIds.at(end);
    if (requested.Step < assignment.FirstEndStep) {
        assignment.FirstEndStep = requested.Step;
        db.Table<Schema::TxIds>().Key(end.Step, end.TxId).Update(
            NIceDb::TUpdate<Schema::TxIds::FirstEndStep>(requested.Step));
    }
    return assignment.WriteTxId;
}

class TController::TTxAssignTxId: public TTxBase {
    THashMap<ui32, THolder<TEvService::TEvTxIdResult>> Result;
    bool TxIdsExhausted = false;

    struct TTxId {
        ui64 Value;

        TTxId() = default;
        TTxId(ui64 value)
            : Value(value)
        {}
    };

    enum class EError {
        TooManyOpenTxIds,
        TxIdsExhausted,
    };

    using TAssignResult = std::variant<TTxId, EError>;

    template <typename TAssignFunc>
    TAssignResult Process(TAssignFunc assignFunc) {
        TAssignResult result;

        for (auto it = Self->PendingTxId.begin(); it != Self->PendingTxId.end();) {
            result = assignFunc(it->first);
            ui64 txId = 0;

            if (const auto* x = std::get_if<TTxId>(&result)) {
                txId = x->Value;
            } else {
                return result;
            }

            for (const auto nodeId : it->second) {
                auto& ev = Result[nodeId];
                if (!ev) {
                    ev.Reset(new TEvService::TEvTxIdResult(Self->TabletID(), Self->Executor()->Generation()));
                }

                auto& item = *ev->Record.AddVersionTxIds();
                it->first.ToProto(item.MutableVersion());
                item.SetTxId(txId);
            }

            Self->PendingTxId.erase(it++);
        }

        return result;
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
            {"assigned", Self->AssignedTxIds.size()},
            {"allocated", Self->AllocatedTxIds.size()});

        const auto replication = Self->GetSingle();
        if (replication && Self->ReplicationsWithSourceOffsetMismatch.contains(replication->GetId())) {
            Self->PendingTxId.clear();
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto result = Process([&](const TRowVersion& version) -> TAssignResult {
            const auto it = Self->AssignedTxIds.lower_bound(version);
            if (it != Self->AssignedTxIds.end()
                    && (!Self->IndexBuilds.empty() || it->second.FirstEndStep <= version.Step)) {
                return Self->CoverEarlierInterval(db, it->first, version);
            }

            if (Self->AssignedTxIds.size() >= MaxOpenTxIds) {
                Self->TabletCounters->Cumulative()[COUNTER_TOO_MANY_OPEN_TX_IDS] += 1;
                return EError::TooManyOpenTxIds;
            }

            if (Self->AllocatedTxIds.empty()) {
                Self->TabletCounters->Cumulative()[COUNTER_TX_IDS_EXHAUSTED] += 1;
                return EError::TxIdsExhausted;
            }

            const ui64 txId = Self->AllocatedTxIds.front();
            Self->AllocatedTxIds.pop_front();

            Self->SaveAssignment(db, version, {txId, version.Step});

            return txId;
        });

        if (const auto* x = std::get_if<EError>(&result); x && *x == EError::TxIdsExhausted) {
            TxIdsExhausted = true;
            return true;
        }

        if (auto it = Self->PendingTxId.rbegin(); it != Self->PendingTxId.rend()
                && Self->AssignedTxIds.rbegin()->first < it->first) {
            Y_ABORT_UNLESS(std::holds_alternative<EError>(result));
            Y_ABORT_UNLESS(std::get<EError>(result) == EError::TooManyOpenTxIds);

            const auto previousEnd = Self->AssignedTxIds.rbegin()->first;
            bool crossesJoin = false;
            for (const auto& [_, build] : Self->IndexBuilds) {
                if (build.HasJoinVersion()) {
                    const auto join = TRowVersion::FromProto(build.GetJoinVersion());
                    crossesJoin |= previousEnd <= join && join < it->first;
                }
            }
            if (crossesJoin) {
                // C is a fixed boundary. At the open-transaction limit a
                // separate suffix is necessary instead of extending across C.
                if (Self->AllocatedTxIds.empty()) {
                    TxIdsExhausted = true;
                    return true;
                }

                const auto txId = Self->AllocatedTxIds.front();
                Self->AllocatedTxIds.pop_front();
                Self->SaveAssignment(db, it->first, {txId, it->first.Step});
            } else {
                Self->ExtendLastAssignment(db, it->first);
            }
        }

        result = Process([&](const TRowVersion& version) -> TAssignResult {
            auto it = Self->AssignedTxIds.lower_bound(version);
            Y_ABORT_UNLESS(it != Self->AssignedTxIds.end());
            return Self->CoverEarlierInterval(db, it->first, version);
        });

        Y_ABORT_UNLESS(std::holds_alternative<TTxId>(result));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete",
            {"pending", Self->PendingTxId.size()},
            {"assigned", Self->AssignedTxIds.size()},
            {"allocated", Self->AllocatedTxIds.size()},
            {"exhausted", TxIdsExhausted});

        Self->TabletCounters->Simple()[COUNTER_PENDING_VERSIONS] = Self->PendingTxId.size();
        Self->TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = Self->AllocatedTxIds.size();
        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->AssignedTxIds.size();

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
    if (AllocateTxIdInFlight || AllocatedTxIds.size() >= MinAllocatedTxIds) {
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

    std::copy(ev->Get()->TxIds.begin(), ev->Get()->TxIds.end(), std::back_inserter(AllocatedTxIds));
    AllocateTxIdInFlight = false;

    TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = AllocatedTxIds.size();
    RunTxAssignTxId(ctx);
    RunTxIndexBuild(ctx);
}

}
