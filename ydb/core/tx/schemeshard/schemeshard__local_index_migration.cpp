#include "schemeshard__local_index_migration.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"
#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>

#include <ydb/core/tx/tx_proxy/proxy.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD


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
        YDB_LOG_NOTICE("LocalIndexMigrator: Bootstrap with pending items",
            {"#_PendingItems.size", PendingItems.size()});
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
                YDB_LOG_CRIT_CTX(*TlsActivationContext, "LocalIndexMigrator unexpected event 0x%08x",
                    {"#_ev->GetTypeRewrite", ev->GetTypeRewrite()});
        }
    }

private:
    void HandleWakeup() {
        ProcessItems(TlsActivationContext->AsActorContext());
    }

    void ProcessItems(const TActorContext& ctx) {
        YDB_LOG_INFO_CTX(ctx, "LocalIndexMigrator: ProcessItems called,",
            {"pendingItems", PendingItems.size()},
            {"awaitingRequests", AwaitingRequests.size()});

        if (PendingItems.empty()) {
            if (AwaitingRequests.empty()) {
                YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: All items processed, passing away");
                PassAway();
            }
            return;
        }

        // Only process one item at a time to avoid concurrent ALTER operations on the same table
        if (!AwaitingRequests.empty()) {
            // Wait for current operation to complete before processing next item
            YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: Waiting for request(s) to complete",
                {"#_AwaitingRequests.size", AwaitingRequests.size()});
            return;
        }

        TTxId txId = SchemeShard->GetCachedTxId(ctx);
        if (txId == InvalidTxId) {
            // No more TxIds available, wait and retry
            YDB_LOG_INFO_CTX(ctx, "LocalIndexMigrator: TxId cache exhausted, waiting for remaining item(s)",
                {"#_PendingItems.size", PendingItems.size()});
            ctx.Schedule(RetryDelay, new TEvents::TEvWakeup());
            return;
        }

        // Get next item (without popping yet, so it stays in PendingItems if validation fails)
        auto& item = PendingItems.back();

        YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: Processing index with",
            {"#_JoinPath({item.WorkingDir, item.IndexConfig.GetName()})", JoinPath({item.WorkingDir, item.IndexConfig.GetName()})},
            {"txId", txId},
            {"remaining", (PendingItems.size() - 1)});

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
                YDB_LOG_ERROR_CTX(ctx, "LocalIndexMigrator at failed to split path will retry after delay",
                    {"schemeshard", SelfTabletId},
                    {"#_item.WorkingDir", item.WorkingDir},
                    {"delay", delay});
                ctx.Schedule(delay, new TEvents::TEvWakeup());
            } else {
                YDB_LOG_CRIT_CTX(ctx, "LocalIndexMigrator at failed to split path after max retries, dying",
                    {"schemeshard", SelfTabletId},
                    {"#_item.WorkingDir", item.WorkingDir});
                PassAway();
            }
            return;
        }

        TString tableName = pathParts.back();
        pathParts.pop_back();
        TString parentDir = JoinPath(pathParts);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(parentDir);
        modifyScheme.SetInternal(true);
        modifyScheme.SetFailOnExist(false);

        if (item.IsColumnTable) {
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);

            auto& alterColumnTable = *modifyScheme.MutableAlterColumnTable();
            alterColumnTable.SetName(tableName);

            auto& alterSchema = *alterColumnTable.MutableAlterSchema();
            auto& upsertIndex = *alterSchema.AddUpsertIndexes();

            // Convert TIndexCreationConfig to TOlapIndexRequested
            if (!NOlap::ConvertCreationConfigToRequested(item.IndexConfig, upsertIndex)) {
                SchemeShard->ReturnTxIdToCache(txId);
                if (item.Backoff.HasMore()) {
                    auto delay = item.Backoff.Next();
                    YDB_LOG_ERROR_CTX(ctx, "LocalIndexMigrator at failed to convert index config for will retry after delay",
                        {"schemeshard", SelfTabletId},
                        {"#_item.WorkingDir", item.WorkingDir},
                        {"delay", delay});
                    ctx.Schedule(delay, new TEvents::TEvWakeup());
                } else {
                    YDB_LOG_CRIT_CTX(ctx, "LocalIndexMigrator at failed to convert index config for after max retries, dying",
                        {"schemeshard", SelfTabletId},
                        {"#_item.WorkingDir", item.WorkingDir});
                    PassAway();
                }
                return;
            }
        } else {
            // Row table: register the scheme object only; ByKeyFilterPrefixes already exists.
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            auto& alterTable = *modifyScheme.MutableAlterTable();
            alterTable.SetName(tableName);
            const auto& cfg = item.IndexConfig;
            auto& indexDesc = *alterTable.AddTableIndexes();
            indexDesc.SetName(cfg.GetName());
            indexDesc.SetType(cfg.GetType());
            indexDesc.SetState(cfg.GetState());
            for (const auto& col : cfg.GetKeyColumnNames()) {
                indexDesc.AddKeyColumnNames(col);
            }
            if (cfg.HasBloomFilterDescription()) {
                *indexDesc.MutableBloomFilterDescription() = cfg.GetBloomFilterDescription();
            }
        }

        AwaitingRequests.emplace(txId, std::move(item));
        PendingItems.pop_back(); // Pop only after all validations succeed
        Send(SelfActorId, request.Release());
    }

    void Done(TTxId txId, const TActorContext& ctx) {
        YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: Done",
            {"txId", txId},
            {"awaitingRequests", AwaitingRequests.size()},
            {"pendingItems", PendingItems.size()});
        AwaitingRequests.erase(txId);
        if (AwaitingRequests.empty() && PendingItems.empty()) {
            YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: All items completed, passing away");
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
                YDB_LOG_ERROR_CTX(ctx, "LocalIndexMigrator at failed to will retry after delay",
                    {"schemeshard", SelfTabletId},
                    {"#_AwaitingRequests.at(txId).DebugString", AwaitingRequests.at(txId).DebugString()},
                    {"reason", record.GetReason()},
                    {"delay", delay});
                PendingItems.push_back(std::move(AwaitingRequests.at(txId)));
                AwaitingRequests.erase(txId);
                ctx.Schedule(delay, new TEvents::TEvWakeup());
            } else {
                YDB_LOG_CRIT_CTX(ctx, "LocalIndexMigrator at failed to after max retries, dying",
                    {"schemeshard", SelfTabletId},
                    {"#_AwaitingRequests.at(txId).DebugString", AwaitingRequests.at(txId).DebugString()},
                    {"reason", record.GetReason()});
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
        YDB_LOG_NOTICE_CTX(ctx, "LocalIndexMigrator: replenished TxId cache with TxId(s),",
            {"#_ev->Get()->TxIds.size", ev->Get()->TxIds.size()},
            {"pendingItems", PendingItems.size()});
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
