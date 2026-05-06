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
        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TLocalIndexMigrator: Bootstrap with " << PendingItems.size() << " pending items");
        // Process items immediately
        ProcessItems(TlsActivationContext->AsActorContext());
        Become(&TLocalIndexMigrator::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TLocalIndexMigrator unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void HandleWakeup() {
        ProcessItems(TlsActivationContext->AsActorContext());
    }

    void ProcessItems(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TLocalIndexMigrator: ProcessItems called, PendingItems=" << PendingItems.size()
            << ", AwaitingRequests=" << AwaitingRequests.size());

        if (PendingItems.empty()) {
            if (AwaitingRequests.empty()) {
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TLocalIndexMigrator: All items processed, passing away");
                PassAway();
            }
            return;
        }

        // Only process one item at a time to avoid concurrent ALTER operations on the same table
        if (!AwaitingRequests.empty()) {
            // Wait for current operation to complete before processing next item
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator: Waiting for " << AwaitingRequests.size() << " request(s) to complete");
            return;
        }

        TTxId txId = SchemeShard->GetCachedTxId(ctx);
        if (txId == InvalidTxId) {
            // No more TxIds available, wait and retry
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator: TxId cache exhausted, waiting for "
                << PendingItems.size() << " remaining item(s)");
            ctx.Schedule(RetryDelay, new TEvents::TEvWakeup());
            return;
        }

        // Get next item
        auto item = std::move(PendingItems.back());
        PendingItems.pop_back();

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TLocalIndexMigrator: Processing index from table '" << item.WorkingDir
            << "' with TxId=" << txId << ", remaining=" << PendingItems.size());

        // Create and send ModifySchemeTransaction
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(static_cast<ui64>(txId));

        // Extract table name from working directory
        auto pathParts = SplitPath(item.WorkingDir);
        if (pathParts.empty()) {
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator: failed to split path '" << item.WorkingDir << "'");
            SchemeShard->ReturnTxIdToCache(txId);
            ctx.Schedule(TDuration::Zero(), new TEvents::TEvWakeup());
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
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator: failed to convert index config for '" << item.WorkingDir << "'");
            SchemeShard->ReturnTxIdToCache(txId);
            ctx.Schedule(TDuration::Zero(), new TEvents::TEvWakeup());
            return;
        }

        AwaitingRequests.emplace(txId, std::move(item));
        Send(SelfActorId, request.Release());
    }

    void Done(TTxId txId, const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TLocalIndexMigrator: Done for TxId=" << txId
            << ", AwaitingRequests=" << AwaitingRequests.size()
            << ", PendingItems=" << PendingItems.size());
        AwaitingRequests.erase(txId);
        if (AwaitingRequests.empty() && PendingItems.empty()) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator: All items completed, passing away");
            PassAway();
        } else {
            // Process next item after current one completes
            ProcessItems(ctx);
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
        case NKikimrScheme::StatusMultipleModifications:
        case NKikimrScheme::StatusNotAvailable:
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator at schemeshard: " << SelfTabletId
                << ", transient failure for " << AwaitingRequests.at(txId).DebugString()
                << ", reason: " << record.GetReason() << ", will retry");
            PendingItems.push_back(std::move(AwaitingRequests.at(txId)));
            AwaitingRequests.erase(txId);
            ctx.Schedule(RetryDelay, new TEvents::TEvWakeup());
            break;
        default:
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TLocalIndexMigrator at schemeshard: " << SelfTabletId
                << ", failed to " << AwaitingRequests.at(txId).DebugString()
                << ", reason: " << record.GetReason());
            Done(txId, ctx);
            break;
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        Done(TTxId(ev->Get()->Record.GetTxId()), ctx);
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
