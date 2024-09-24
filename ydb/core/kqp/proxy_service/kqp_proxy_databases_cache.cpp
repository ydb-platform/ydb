#include "kqp_proxy_service_impl.h"

#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/events.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKikimr::NKqp {

namespace {


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvSubscribeOnDatabase = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPingDatabaseSubscription,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvSubscribeOnDatabase : public TEventLocal<TEvSubscribeOnDatabase, EvSubscribeOnDatabase> {
        explicit TEvSubscribeOnDatabase(const TString& database)
            : Database(database)
        {}

        TString Database;
    };

    struct TEvPingDatabaseSubscription : public TEventLocal<TEvSubscribeOnDatabase, EvPingDatabaseSubscription> {
        explicit TEvPingDatabaseSubscription(const TString& database)
            : Database(database)
        {}

        TString Database;
    };
};

class TDatabaseSubscriberActor : public TActor<TDatabaseSubscriberActor> {
    using TBase = TActor<TDatabaseSubscriberActor>;

    struct TDatabaseState {
        bool FetchRequestIsRunning = false;
        TPathId WatchPathId;

        TString DatabaseId;
        bool Serverless = false;
        std::unordered_set<TActorId> Subscribers;
    };

public:
    TDatabaseSubscriberActor()
        : TBase(&TDatabaseSubscriberActor::StateFunc)
    {}

    void Handle(TEvPrivate::TEvSubscribeOnDatabase::TPtr& ev) {
        const TString& database = CanonizePath(ev->Get()->Database);
        auto& databaseState = Subscriptions[database];

        if (databaseState.DatabaseId) {
            SendSubscriberInfo(database, ev->Sender, databaseState, Ydb::StatusIds::SUCCESS);
        } else if (!databaseState.FetchRequestIsRunning) {
            Register(NWorkload::CreateDatabaseFetcherActor(SelfId(), database));
            databaseState.FetchRequestIsRunning = true;
        }

        databaseState.Subscribers.insert(ev->Sender);
    }

    void Handle(NWorkload::TEvFetchDatabaseResponse::TPtr& ev) {
        const TString& database = CanonizePath(ev->Get()->Database);
        auto& databaseState = Subscriptions[database];

        UpdateDatabaseState(databaseState, database, ev->Get()->PathId, ev->Get()->Serverless);
        UpdateSubscribersInfo(database, databaseState, ev->Get()->Status, ev->Get()->Issues);

        databaseState.FetchRequestIsRunning = false;
        databaseState.WatchPathId = ev->Get()->PathId;

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            WatchKey++;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(databaseState.WatchPathId, WatchKey));
            WatchDatabases.insert({WatchKey, database});
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        auto it = WatchDatabases.find(ev->Get()->Key);
        if (it == WatchDatabases.end()) {
            return;
        }

        const auto& result = ev->Get()->Result;
        if (!result || result->GetStatus() != NKikimrScheme::StatusSuccess) {
            return;
        }

        if (result->GetPathDescription().HasDomainDescription()) {
            NSchemeCache::TDomainInfo description(result->GetPathDescription().GetDomainDescription());

            auto& databaseState = Subscriptions[it->second];
            UpdateDatabaseState(databaseState, it->second, description.DomainKey, description.IsServerless());
            UpdateSubscribersInfo(it->second, databaseState, Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        auto it = WatchDatabases.find(ev->Get()->Key);
        if (it == WatchDatabases.end()) {
            return;
        }

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(ev->Get()->Key));

        auto databaseStateIt = Subscriptions.find(it->second);
        if (databaseStateIt != Subscriptions.end()) {    
            UpdateSubscribersInfo(it->second, databaseStateIt->second, Ydb::StatusIds::NOT_FOUND, {NYql::TIssue{"Database was dropped"}});
            Subscriptions.erase(databaseStateIt);
        }

        WatchDatabases.erase(it);
    }

    void HandlePoison() {
        if (!WatchDatabases.empty()) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(0));
        }

        TBase::PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSubscribeOnDatabase, Handle);
        hFunc(NWorkload::TEvFetchDatabaseResponse, Handle);
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
        sFunc(TEvents::TEvPoison, HandlePoison);
    )

private:
    void UpdateDatabaseState(TDatabaseState& databaseState, const TString& database, TPathId pathId, bool serverless) {
        databaseState.DatabaseId = (serverless ? TStringBuilder() << pathId.OwnerId << ":" << pathId.LocalPathId << ":" : TStringBuilder()) << database;
        databaseState.Serverless = serverless;
    }

    void UpdateSubscribersInfo(const TString& database, const TDatabaseState& databaseState, Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        for (const auto& subscriber : databaseState.Subscribers) {
            SendSubscriberInfo(database, subscriber, databaseState, status, issues);
        }
    }

    void SendSubscriberInfo(const TString& database, TActorId subscriber, const TDatabaseState& databaseState, Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::UNSUPPORTED) {
            Send(subscriber, new TEvKqp::TEvUpdateDatabaseInfo(database, databaseState.DatabaseId, databaseState.Serverless));
        } else {
            NYql::TIssue rootIssue(TStringBuilder() << "Failed to describe database " << database);
            for (const auto& issue : issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
            Send(subscriber, new TEvKqp::TEvUpdateDatabaseInfo(database, status, {rootIssue}));
        }
    }

private:
    std::unordered_map<TString, TDatabaseState> Subscriptions;
    std::unordered_map<ui32, TString> WatchDatabases;
    ui32 WatchKey = 0;
};

}  // anonymous namespace

const TString& TDatabasesCache::GetTenantName() {
    if (!TenantName) {
        TenantName = CanonizePath(AppData()->TenantName);
    }
    return TenantName;
}

void TDatabasesCache::UpdateDatabaseInfo(TEvKqp::TEvUpdateDatabaseInfo::TPtr& event, TActorContext actorContext) {
    const auto& database = event->Get()->Database;
    auto it = DatabasesCache.find(database);
    if (it == DatabasesCache.end()) {
        it = DatabasesCache.insert({database, TDatabaseInfo{}}).first;
    }
    it->second.DatabaseId = event->Get()->DatabaseId;

    bool success = event->Get()->Status == Ydb::StatusIds::SUCCESS;
    for (auto& delayedEvent : it->second.DelayedEvents) {
        if (success) {
            actorContext.Send(std::move(delayedEvent.Event));
        } else {
            delayedEvent.ErrorHandler(event->Get()->Status, event->Get()->Issues);
        }
    }
    it->second.DelayedEvents.clear();

    if (!success) {
        DatabasesCache.erase(it);
    }
}

void TDatabasesCache::StopSubscriberActor(TActorContext actorContext) const {
    if (SubscriberActor) {
        actorContext.Send(SubscriberActor, new TEvents::TEvPoison());
    }
}

void TDatabasesCache::CreateDatabaseSubscriberActor(TActorContext actorContext) {
    SubscriberActor = actorContext.Register(new TDatabaseSubscriberActor());
}

}  // namespace NKikimr::NKqp
