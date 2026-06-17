#include "schemeshard__local_index_migration.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>

#include <ydb/core/tx/tx_proxy/proxy.h>


namespace NKikimr::NSchemeShard {

TString TLocalIndexMigrationItem::DebugString() const {
    return TStringBuilder() << "create local index '" << JoinPath({WorkingDir, IndexConfig.GetName()}) << "'";
}

namespace {

// Modeled on TSysViewsRosterUpdate: pre-allocated TxIds, operations processed
// sequentially (one at a time) to avoid concurrent ALTER operations on the same
// table, finishes when all items are processed.
class TLocalIndexMigrator : public TActorBootstrapped<TLocalIndexMigrator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_LOCAL_INDEX_MIGRATOR;
    }

    static constexpr TDuration RetryDelay = TDuration::MilliSeconds(100);

    TLocalIndexMigrator(TTabletId selfTabletId, TActorId selfActorId, TSchemeShard* schemeshard,
                        TVector<TLocalIndexMigrationItem>&& items)
        : SelfTabletId(selfTabletId)
        , SelfActorId(selfActorId)
        , SchemeShard(schemeshard)
        , PendingItems(std::move(items))
    {
        AwaitingRequests.reserve(PendingItems.size());
    }

    void Bootstrap() {
        LOG_NOTICE_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: Bootstrap with " << PendingItems.size() << " pending items");
        // Process items immediately
        ProcessItems(TlsActivationContext->AsActorContext());
        Become(&TLocalIndexMigrator::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(TEvTxAllocatorClient::TEvAllocateResult, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void HandleWakeup() {
        ProcessItems(TlsActivationContext->AsActorContext());
    }

    void ProcessItems(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: ProcessItems called, PendingItems=" << PendingItems.size()
            << ", AwaitingRequests=" << AwaitingRequests.size());

        if (PendingItems.empty()) {
            if (AwaitingRequests.empty()) {
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator: All items processed, passing away");
                PassAway();
            }
            return;
        }

        // Only process one item at a time to avoid concurrent ALTER operations on the same table
        if (!AwaitingRequests.empty()) {
            // Wait for current operation to complete before processing next item
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "LocalIndexMigrator: Waiting for " << AwaitingRequests.size() << " request(s) to complete");
            return;
        }

        TTxId txId = SchemeShard->GetCachedTxId(ctx);
        if (txId == InvalidTxId) {
            // No more TxIds available, wait and retry
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "LocalIndexMigrator: TxId cache exhausted, waiting for "
                << PendingItems.size() << " remaining item(s)");
            ctx.Schedule(RetryDelay, new TEvents::TEvWakeup());
            return;
        }

        // Get next item (without popping yet, so it stays in PendingItems if validation fails)
        auto& item = PendingItems.back();

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: Processing index '" << JoinPath({item.WorkingDir, item.IndexConfig.GetName()})
            << "' with TxId=" << txId << ", remaining=" << (PendingItems.size() - 1));

        // Create and send ModifySchemeTransaction
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(static_cast<ui64>(txId));

        // Extract table name from working directory
        auto pathParts = SplitPath(item.WorkingDir);
        if (pathParts.empty()) {
            SchemeShard->ReturnTxIdToCache(txId);
            if (item.Backoff.HasMore()) {
                auto delay = item.Backoff.Next();
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to split path '" << item.WorkingDir << "', will retry after delay " << delay);
                ctx.Schedule(delay, new TEvents::TEvWakeup());
            } else {
                LOG_CRIT_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to split path '" << item.WorkingDir << "' after max retries, dying");
                PassAway();
            }
            return;
        }

        TString tableName = pathParts.back();
        pathParts.pop_back();
        TString parentDir = JoinPath(pathParts);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(parentDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
        modifyScheme.SetInternal(true);
        modifyScheme.SetFailOnExist(false);

        auto& alterColumnTable = *modifyScheme.MutableAlterColumnTable();
        alterColumnTable.SetName(tableName);

        auto& alterSchema = *alterColumnTable.MutableAlterSchema();
        auto& upsertIndex = *alterSchema.AddUpsertIndexes();

        // Convert TIndexCreationConfig to TOlapIndexRequested
        if (!NOlap::ConvertCreationConfigToRequested(item.IndexConfig, upsertIndex)) {
            SchemeShard->ReturnTxIdToCache(txId);
            if (item.Backoff.HasMore()) {
                auto delay = item.Backoff.Next();
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to convert index config for '" << item.WorkingDir << "', will retry after delay " << delay);
                ctx.Schedule(delay, new TEvents::TEvWakeup());
            } else {
                LOG_CRIT_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to convert index config for '" << item.WorkingDir << "' after max retries, dying");
                PassAway();
            }
            return;
        }

        AwaitingRequests.emplace(txId, std::move(item));
        PendingItems.pop_back(); // Pop only after all validations succeed
        Send(SelfActorId, request.Release());
    }

    void Done(TTxId txId, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: Done for TxId=" << txId
            << ", AwaitingRequests=" << AwaitingRequests.size()
            << ", PendingItems=" << PendingItems.size());
        AwaitingRequests.erase(txId);
        if (AwaitingRequests.empty() && PendingItems.empty()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "LocalIndexMigrator: All items completed, passing away");
            PassAway();
        } else {
            // Process next item after current one completes
            ctx.Schedule(RetryDelay, new TEvents::TEvWakeup());
        }
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const TTxId txId(record.GetTxId());

        switch (record.GetStatus()) {
        case NKikimrScheme::StatusSuccess:
        case NKikimrScheme::StatusAlreadyExists:
            // StatusAlreadyExists: a concurrent operation already created the scheme object.
            Done(txId, ctx);
            break;
        case NKikimrScheme::StatusAccepted:
            Send(SelfActorId, new TEvSchemeShard::TEvNotifyTxCompletion(static_cast<ui64>(txId)));
            break;
        default:
            if (AwaitingRequests.at(txId).Backoff.HasMore()) {
                auto delay = AwaitingRequests.at(txId).Backoff.Next();
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to " << AwaitingRequests.at(txId).DebugString()
                    << ", reason: " << record.GetReason() << ", will retry after delay " << delay);
                PendingItems.push_back(std::move(AwaitingRequests.at(txId)));
                AwaitingRequests.erase(txId);
                ctx.Schedule(delay, new TEvents::TEvWakeup());
            } else {
                LOG_CRIT_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator at schemeshard: " << SelfTabletId
                    << ", failed to " << AwaitingRequests.at(txId).DebugString()
                    << " after max retries, reason: " << record.GetReason() << ", dying");
                PassAway();
            }
            break;
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        Done(TTxId(ev->Get()->Record.GetTxId()), ctx);
    }

    void Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
        for (ui64 txId : ev->Get()->TxIds) {
            SchemeShard->ReturnTxIdToCache(TTxId(txId));
        }
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: replenished TxId cache with " << ev->Get()->TxIds.size()
            << " TxId(s), PendingItems=" << PendingItems.size());
        ProcessItems(ctx);
    }

private:
    const TTabletId SelfTabletId;
    const TActorId SelfActorId;
    TSchemeShard* SchemeShard;
    TVector<TLocalIndexMigrationItem> PendingItems;
    THashMap<TTxId, TLocalIndexMigrationItem> AwaitingRequests;
};

} // anonymous namespace

THolder<IActor> CreateLocalIndexMigrator(TTabletId selfTabletId, TActorId selfActorId,
                                         TSchemeShard* schemeshard,
                                         TVector<TLocalIndexMigrationItem>&& items) {
    return MakeHolder<TLocalIndexMigrator>(selfTabletId, selfActorId, schemeshard, std::move(items));
}

} // namespace NKikimr::NSchemeShard
