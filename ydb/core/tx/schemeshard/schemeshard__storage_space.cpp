#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

namespace {

// scheme cache watch keys of the resources domain space watch
constexpr ui64 ResourcesDomainWatchKey = 1; // the shared database's path in the root schemeshard
constexpr ui64 ResourcesDomainStateWatchKey = 2; // root path of the shared database's own schemeshard

} // anonymous

struct TSchemeShard::TTxUpdateStorageSpaceState : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    const std::vector<TPathId> Domains;
    const bool Exhausted;

    TTxUpdateStorageSpaceState(TSchemeShard *self, std::vector<TPathId> domains, bool exhausted)
        : TTransactionBase(self)
        , Domains(std::move(domains))
        , Exhausted(exhausted)
    {}

    TTxType GetTxType() const override { return TXTYPE_UPDATE_STORAGE_SPACE_STATE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        TDeque<TPathId> toPublish;
        for (const TPathId& pathId : Domains) {
            const auto it = Self->SubDomains.find(pathId);
            if (it == Self->SubDomains.end()) {
                continue; // the domain has gone meanwhile
            }
            TSubDomainInfo::TPtr subDomainInfo = it->second;
            if (subDomainInfo->ApplyStorageSpaceExhausted(Exhausted, Self)) {
                YDB_LOG_NOTICE_CTX(ctx, "Storage space state of the database has changed",
                    {"pathId", pathId},
                    {"exhausted", Exhausted},
                    {"schemeshard", Self->TabletID()},
                );
                Self->PersistSubDomainState(db, pathId, *subDomainInfo);
                toPublish.push_back(pathId);
            }
        }
        if (!toPublish.empty()) {
            // Publish is done in a separate transaction, so we may call this directly
            Self->PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TSchemeShard::UpdateDatabaseSpaceSubscriptions() {
    if (!DatabaseSpaceSubscriptionsActive) {
        return;
    }

    // Databases hosted by this schemeshard: its root domain and plain subdomains (external ones have their own
    // schemeshards). Which schemeshard this is matters:
    //  * the root schemeshard: the root domain (key is the root path; its pools usually have no ScopeId, so nothing
    //    matches unless it is set explicitly) and plain subdomains (key is the subdomain's path);
    //  * a dedicated or shared database's schemeshard: its root, the key is the database's path in the root
    //    schemeshard (ParentDomainId);
    //  * a serverless database's schemeshard: its root has no storage pools of its own, it runs on the shared
    //    database's ones, so it follows the state the shared database's schemeshard publishes instead.
    // The key is what console writes into ScopeId of the database's storage pools.
    std::map<TPathId, std::set<TPathId>> scopes;
    std::map<TPathId, std::set<TPathId>> resourcesDomains;
    for (const auto& [pathId, domainInfo] : SubDomains) {
        const auto it = PathsById.find(pathId);
        if (it == PathsById.end() || it->second->Dropped() || !it->second->IsSubDomainRoot()) {
            continue;
        }
        if (IsServerlessDomain(domainInfo)) {
            resourcesDomains[domainInfo->GetResourcesDomainId()].insert(pathId);
        } else {
            scopes[GetDomainKey(pathId)].insert(pathId);
        }
    }
    UpdateResourcesDomainSpaceWatch(std::move(resourcesDomains));

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();
    for (const auto& [domainKey, domains] : scopes) {
        if (!DatabaseSpaceScopes.contains(domainKey)) {
            domainKey.ToProto(ev->Record.AddSubscribe());
        }
    }
    for (const auto& [domainKey, domains] : DatabaseSpaceScopes) {
        if (!scopes.contains(domainKey)) {
            domainKey.ToProto(ev->Record.AddUnsubscribe());
        }
    }
    DatabaseSpaceScopes = std::move(scopes);

    if (ev->Record.SubscribeSize() || ev->Record.UnsubscribeSize()) {
        const auto& ctx = ActorContext();
        YDB_LOG_DEBUG_CTX(ctx, "Updating storage space state subscriptions",
            {"request", ev->Record.ShortDebugString()},
            {"schemeshard", TabletID()},
        );
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
    }
}

void TSchemeShard::UnsubscribeFromDatabaseSpace() {
    UpdateResourcesDomainSpaceWatch({});

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();
    for (const auto& [domainKey, domains] : std::exchange(DatabaseSpaceScopes, {})) {
        domainKey.ToProto(ev->Record.AddUnsubscribe());
    }
    DatabaseSpaceSubscriptionsActive = false;
    if (ev->Record.UnsubscribeSize()) {
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
    }
}

void TSchemeShard::Handle(TEvBlobStorage::TEvControllerDatabaseSpaceState::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    YDB_LOG_DEBUG_CTX(ctx, "Handle TEvControllerDatabaseSpaceState",
        {"record", record.ShortDebugString()},
        {"schemeshard", TabletID()},
    );

    const auto it = DatabaseSpaceScopes.find(TPathId::FromProto(record.GetScope()));
    if (it == DatabaseSpaceScopes.end()) {
        return; // not a database we are subscribed to
    }

    Execute(new TTxUpdateStorageSpaceState(this, {it->second.begin(), it->second.end()}, record.GetExhausted()), ctx);
}

void TSchemeShard::UpdateResourcesDomainSpaceWatch(std::map<TPathId, std::set<TPathId>> resourcesDomains) {
    // only the root of a serverless database's schemeshard runs on someone else's resources
    Y_DEBUG_ABORT_UNLESS(resourcesDomains.size() <= 1);
    auto it = resourcesDomains.begin();

    if (ResourcesDomainSpaceWatch && (it == resourcesDomains.end() ||
            ResourcesDomainSpaceWatch->ResourcesDomainId != it->first)) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(ResourcesDomainWatchKey));
        if (ResourcesDomainSpaceWatch->StatePathId) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(ResourcesDomainStateWatchKey));
        }
        ResourcesDomainSpaceWatch.reset();
    }

    if (it == resourcesDomains.end()) {
        return;
    }

    if (ResourcesDomainSpaceWatch) {
        ResourcesDomainSpaceWatch->Domains = std::move(it->second);
    } else {
        const auto& ctx = ActorContext();
        YDB_LOG_DEBUG_CTX(ctx, "Watching storage space state of the resources domain",
            {"resourcesDomainId", it->first},
            {"schemeshard", TabletID()},
        );
        ResourcesDomainSpaceWatch.emplace(TResourcesDomainSpaceWatch{
            .ResourcesDomainId = it->first,
            .Domains = std::move(it->second),
        });
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(it->first, ResourcesDomainWatchKey));
    }
}

void TSchemeShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    auto& watch = ResourcesDomainSpaceWatch;
    if (!watch || !msg->Result) {
        return;
    }
    const NKikimrScheme::TEvDescribeSchemeResult& record = *msg->Result;
    if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
        return;
    }
    const auto& domain = record.GetPathDescription().GetDomainDescription();

    if (msg->Key == ResourcesDomainWatchKey && msg->PathId == watch->ResourcesDomainId) {
        // the shared database's own schemeshard; it becomes known once the database is set up
        const ui64 schemeShardId = domain.GetProcessingParams().GetSchemeShard();
        const TPathId statePathId = schemeShardId ? TPathId(schemeShardId, NSchemeShard::RootPathId) : TPathId();
        if (statePathId == watch->StatePathId) {
            return;
        }
        YDB_LOG_DEBUG_CTX(ctx, "Resources domain schemeshard discovered",
            {"resourcesDomainId", watch->ResourcesDomainId},
            {"statePathId", statePathId},
            {"schemeshard", TabletID()},
        );
        if (watch->StatePathId) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(ResourcesDomainStateWatchKey));
        }
        watch->StatePathId = statePathId;
        if (statePathId) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(statePathId, ResourcesDomainStateWatchKey));
        }
    } else if (msg->Key == ResourcesDomainStateWatchKey && watch->StatePathId && msg->PathId == watch->StatePathId) {
        // only the physical storage state is taken; the shared database's own disk space quota is none of ours
        const bool exhausted = domain.GetDomainState().GetStorageSpaceExhausted();
        YDB_LOG_DEBUG_CTX(ctx, "Resources domain storage space state",
            {"statePathId", watch->StatePathId},
            {"exhausted", exhausted},
            {"schemeshard", TabletID()},
        );
        // Every state is submitted, even if equal to the applied one: transactions submitted earlier may not have been
        // executed yet, and they are executed in order, so the last one wins. The transaction itself skips domains that
        // already have this state.
        Execute(new TTxUpdateStorageSpaceState(this, {watch->Domains.begin(), watch->Domains.end()}, exhausted), ctx);
    }
}

void TSchemeShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    auto& watch = ResourcesDomainSpaceWatch;
    if (!watch) {
        return;
    }
    YDB_LOG_NOTICE_CTX(ctx, "Resources domain path is deleted",
        {"key", msg->Key},
        {"pathId", msg->PathId},
        {"schemeshard", TabletID()},
    );
    if (msg->Key == ResourcesDomainStateWatchKey && watch->StatePathId && msg->PathId == watch->StatePathId) {
        // there is no state to follow anymore (console doesn't remove a shared database hosting serverless ones)
        Execute(new TTxUpdateStorageSpaceState(this, {watch->Domains.begin(), watch->Domains.end()}, false), ctx);
    }
}

void TSchemeShard::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::TPtr& ev, const TActorContext& ctx) {
    // the last known state is kept until the path becomes available again
    const auto* msg = ev->Get();
    YDB_LOG_DEBUG_CTX(ctx, "Resources domain path is unavailable",
        {"key", msg->Key},
        {"pathId", msg->PathId},
        {"schemeshard", TabletID()},
    );
}

} // NKikimr::NSchemeShard
