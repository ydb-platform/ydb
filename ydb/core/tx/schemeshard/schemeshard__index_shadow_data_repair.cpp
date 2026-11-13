#include "schemeshard__index_shadow_data_repair.h"
#include "schemeshard_impl.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/util/backoff.h>

#include <util/generic/queue.h>
#include <util/string/builder.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {
namespace {

// Shares SchemeShard's mailbox and repairs one table at a time. Busy candidates
// are retried later; an accepted repair must finish before another can start.
class TIndexShadowDataRepairer : public TActorBootstrapped<TIndexShadowDataRepairer> {
    struct TItem {
        TPathId PathId;
        TInstant NextAttempt;
        TBackoff Backoff{TDuration::Seconds(1), TDuration::Minutes(1)};

        bool operator<(const TItem& other) const {
            return NextAttempt > other.NextAttempt; // Earliest deadline first.
        }
    };

    struct TInFlight {
        TTxId TxId;
        TItem Item;
    };

    TSchemeShard* const SchemeShard;
    const TActorId SchemeShardId;
    TPriorityQueue<TItem> PendingItems;
    std::optional<TInFlight> InFlight;
    // Waiting for a wakeup and waiting for a repair result are mutually exclusive.
    bool WakeupScheduled = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_INDEX_SHADOW_DATA_REPAIRER;
    }

    TIndexShadowDataRepairer(TSchemeShard* schemeShard, TVector<TPathId>&& paths)
        : SchemeShard(schemeShard)
        , SchemeShardId(schemeShard->SelfId())
    {
        for (const auto& pathId : paths) {
            PendingItems.push(TItem{.PathId = pathId});
        }
    }

    void Bootstrap() {
        if (!TlsActivationContext->Mailbox.FindActor(SchemeShardId.LocalId())) {
            return PassAway();
        }
        YDB_LOG_NOTICE("IndexShadowDataRepairer: starting",
            {"schemeshard", SchemeShard->TabletID()}, {"pending", PendingItems.size()});
        Become(&TIndexShadowDataRepairer::StateWork);
        ProcessItems(TlsActivationContext->AsActorContext());
    }

    STFUNC(StateWork) {
        if (!TlsActivationContext->Mailbox.FindActor(SchemeShardId.LocalId())) {
            return PassAway();
        }
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                YDB_LOG_CRIT("IndexShadowDataRepairer: unexpected event",
                    {"eventType", ev->GetTypeRewrite()});
        }
    }

private:
    void WakeupAfter(TDuration delay, const TActorContext& ctx) {
        if (!WakeupScheduled) {
            WakeupScheduled = true;
            ctx.Schedule(delay, new TEvents::TEvWakeup());
        }
    }

    void HandleWakeup() {
        WakeupScheduled = false;
        ProcessItems(TlsActivationContext->AsActorContext());
    }

    void Defer(TItem&& item, const TString& reason, const TActorContext& ctx) {
        // Next() adds jitter after applying the configured backoff limit.
        const auto delay = Min(item.Backoff.Next(), TDuration::Minutes(1));
        item.NextAttempt = ctx.Now() + delay;
        YDB_LOG_INFO_CTX(ctx, "IndexShadowDataRepairer: will retry",
            {"pathId", item.PathId}, {"reason", reason}, {"delay", delay});
        PendingItems.push(std::move(item));
    }

    void ProcessItems(const TActorContext& ctx) {
        if (InFlight) {
            return;
        }

        const auto now = ctx.Now();
        // Bound work in SchemeShard's mailbox, even when every candidate is busy.
        for (size_t remaining = 64; remaining && !PendingItems.empty(); --remaining) {
            if (PendingItems.top().NextAttempt > now) {
                break;
            }
            auto item = PendingItems.PopValue();

            const auto check = CheckIndexShadowDataRepair(SchemeShard, item.PathId);
            switch (check.Status) {
            case EIndexShadowDataRepairStatus::Done:
            case EIndexShadowDataRepairStatus::Invalid:
                YDB_LOG_INFO_CTX(ctx, "IndexShadowDataRepairer: skipping",
                    {"pathId", item.PathId}, {"reason", check.Reason});
                continue;
            case EIndexShadowDataRepairStatus::Deferred:
                Defer(std::move(item), check.Reason, ctx);
                continue;
            case EIndexShadowDataRepairStatus::Ready:
                break;
            }

            // Refill replies must outlive this worker and return to SchemeShard.
            const auto txId = SchemeShard->GetCachedTxId(ctx.MakeFor(SchemeShardId));
            if (txId == InvalidTxId) {
                PendingItems.push(std::move(item));
                WakeupAfter(TDuration::MilliSeconds(100), ctx);
                return;
            }

            auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
            request->Record.SetTxId(ui64(txId));
            auto& tx = *request->Record.AddTransaction();
            tx.SetInternal(true);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpRepairIndexShadowData);
            item.PathId.ToProto(tx.MutableAlterTable()->MutablePathId());
            YDB_LOG_NOTICE_CTX(ctx, "IndexShadowDataRepairer: proposing repair",
                {"pathId", item.PathId}, {"txId", txId});
            InFlight.emplace(TInFlight{txId, std::move(item)});
            Send(SchemeShardId, request.Release());
            return;
        }

        if (PendingItems.empty()) {
            YDB_LOG_NOTICE_CTX(ctx, "IndexShadowDataRepairer: finished",
                {"schemeshard", SchemeShard->TabletID()});
            PassAway();
            return;
        }

        const auto nextAttempt = PendingItems.top().NextAttempt;
        WakeupAfter(nextAttempt > now ? nextAttempt - now : TDuration::Zero(), ctx);
    }

    void RecheckCompleted(const TActorContext& ctx) {
        auto item = std::move(InFlight->Item);
        InFlight.reset();
        const auto check = CheckIndexShadowDataRepair(SchemeShard, item.PathId);
        if (check.Status == EIndexShadowDataRepairStatus::Ready
            || check.Status == EIndexShadowDataRepairStatus::Deferred)
        {
            Defer(std::move(item), check.Reason, ctx);
        } else {
            YDB_LOG_NOTICE_CTX(ctx, "IndexShadowDataRepairer: completed",
                {"pathId", item.PathId}, {"reason", check.Reason});
        }
        ProcessItems(ctx);
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (!InFlight || InFlight->TxId != TTxId(record.GetTxId())) {
            return;
        }
        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            Send(SchemeShardId, new TEvSchemeShard::TEvNotifyTxCompletion(record.GetTxId()));
            break;
        case NKikimrScheme::StatusSuccess:
        case NKikimrScheme::StatusAlreadyExists:
            RecheckCompleted(ctx);
            break;
        default:
            // Configuration errors may clear without a SchemeShard restart.
            Defer(std::move(InFlight->Item),
                TStringBuilder() << NKikimrScheme::EStatus_Name(record.GetStatus()) << ": " << record.GetReason(), ctx);
            InFlight.reset();
            ProcessItems(ctx);
            break;
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        if (InFlight && InFlight->TxId == TTxId(ev->Get()->Record.GetTxId())) {
            RecheckCompleted(ctx);
        }
    }

};

} // namespace

void TSchemeShard::StartIndexShadowDataRepair(const TActorContext& ctx, TVector<TPathId>&& paths) {
    if (!paths.empty()) {
        IndexShadowDataRepairerId = ctx.RegisterWithSameMailbox(
            new TIndexShadowDataRepairer(this, std::move(paths)));
    }
}

} // namespace NKikimr::NSchemeShard

#undef YDB_LOG_THIS_FILE_COMPONENT
