#include "controller_impl.h"

#include <variant>

namespace NKikimr::NReplication::NController {

class TController::TTxAssignTxId: public TTxBase {
    // TODO(ilnaz): configurable
    static constexpr ui64 MaxOpenTxIds = 5;
    static constexpr ui64 MinAllocatedTxIds = 3;

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
        CLOG_D(ctx, "Execute"
            << ": pending# " << Self->PendingTxId.size()
            << ", assigned# " << Self->AssignedTxIds.size()
            << ", allocated# " << Self->AllocatedTxIds.size());

        NIceDb::TNiceDb db(txc.DB);

        auto result = Process([&](const TRowVersion& version) -> TAssignResult {
            auto it = Self->AssignedTxIds.find(version);
            if (it != Self->AssignedTxIds.end()) {
                return it->second;
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

            db.Table<Schema::TxIds>().Key(version.Step, version.TxId).Update(
                NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId)
            );
            it = Self->AssignedTxIds.emplace(version, txId).first;

            return txId;
        });

        if (const auto* x = std::get_if<EError>(&result); x && *x == EError::TxIdsExhausted) {
            TxIdsExhausted = true;
            return true;
        }

        if (auto it = Self->PendingTxId.rbegin(); it != Self->PendingTxId.rend()) {
            Y_ABORT_UNLESS(std::holds_alternative<EError>(result));
            Y_ABORT_UNLESS(std::get<EError>(result) == EError::TooManyOpenTxIds);
            Y_ABORT_UNLESS(Self->AssignedTxIds.lower_bound(it->first) == Self->AssignedTxIds.end());
            Y_ABORT_UNLESS(!Self->AssignedTxIds.empty());

            auto nh = Self->AssignedTxIds.extract(Self->AssignedTxIds.rbegin()->first);
            db.Table<Schema::TxIds>().Key(nh.key().Step, nh.key().TxId).Delete();

            nh.key() = it->first;
            db.Table<Schema::TxIds>().Key(nh.key().Step, nh.key().TxId).Update(
                NIceDb::TUpdate<Schema::TxIds::WriteTxId>(nh.mapped())
            );
            Self->AssignedTxIds.insert(std::move(nh));
        }

        result = Process([&](const TRowVersion&) -> TAssignResult {
            Y_ABORT_UNLESS(!Self->AssignedTxIds.empty());
            return Self->AssignedTxIds.rbegin()->second;
        });

        Y_ABORT_UNLESS(std::holds_alternative<TTxId>(result));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete"
            << ": pending# " << Self->PendingTxId.size()
            << ", assigned# " << Self->AssignedTxIds.size()
            << ", allocated# " << Self->AllocatedTxIds.size()
            << ", exhausted# " << TxIdsExhausted);

        Self->TabletCounters->Simple()[COUNTER_PENDING_VERSIONS] = Self->PendingTxId.size();
        Self->TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = Self->AllocatedTxIds.size();
        Self->TabletCounters->Simple()[COUNTER_ASSIGNED_TX_IDS] = Self->AssignedTxIds.size();

        for (auto& [nodeId, ev] : Result) {
            ctx.Send(MakeReplicationServiceId(nodeId), std::move(ev));
        }

        if (!Self->AllocateTxIdInFlight && Self->AllocatedTxIds.size() < MinAllocatedTxIds) {
            Self->AllocateTxIdInFlight = true;
            ctx.Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(MaxOpenTxIds));
        }

        if (!TxIdsExhausted && Self->PendingTxId) {
            Self->Execute(new TTxAssignTxId(Self), ctx);
        } else {
            Self->AssignTxIdInFlight = false;
        }
    }

}; // TTxAssignTxId

void TController::RunTxAssignTxId(const TActorContext& ctx) {
    if (!AssignTxIdInFlight) {
        AssignTxIdInFlight = true;
        Execute(new TTxAssignTxId(this), ctx);
    }
}

void TController::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    std::copy(ev->Get()->TxIds.begin(), ev->Get()->TxIds.end(), std::back_inserter(AllocatedTxIds));
    AllocateTxIdInFlight = false;

    TabletCounters->Simple()[COUNTER_ALLOCATED_TX_IDS] = AllocatedTxIds.size();
    RunTxAssignTxId(ctx);
}

}
