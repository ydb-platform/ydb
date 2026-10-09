#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

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
    // schemeshards). The key is the database's domain key -- the same thing console writes into ScopeId of the
    // database's storage pools. Storage of a serverless database belongs to its shared database. For the root domain
    // of the root schemeshard it is the root path itself; its pools usually have no ScopeId, so nothing matches
    // unless it is set explicitly.
    std::map<TPathId, std::set<TPathId>> scopes;
    for (const auto& [pathId, domainInfo] : SubDomains) {
        const auto it = PathsById.find(pathId);
        if (it == PathsById.end() || it->second->Dropped() || !it->second->IsSubDomainRoot()) {
            continue;
        }
        const TPathId domainKey = IsServerlessDomain(domainInfo) ? domainInfo->GetResourcesDomainId() : GetDomainKey(pathId);
        scopes[domainKey].insert(pathId);
    }

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

} // NKikimr::NSchemeShard
