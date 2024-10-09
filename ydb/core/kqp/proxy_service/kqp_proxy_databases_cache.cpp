#include "kqp_proxy_service_impl.h"

#include <ydb/core/kqp/workload_service/actors/actors.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKikimr::NKqp {

namespace {


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvSubscribeOnDatabase = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPingDatabaseSubscription,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvSubscribeOnDatabase : public TEventLocal<TEvSubscribeOnDatabase, EvSubscribeOnDatabase> {
        explicit TEvSubscribeOnDatabase(const TString& database)
            : Database(database)
        {}

        const TString Database;
    };

    struct TEvPingDatabaseSubscription : public TEventLocal<TEvPingDatabaseSubscription, EvPingDatabaseSubscription> {
        explicit TEvPingDatabaseSubscription(const TString& database)
            : Database(database)
        {}

        const TString Database;
    };
};

class TDatabaseSubscriberActor : public TActor<TDatabaseSubscriberActor> {
    struct TDatabaseState {
        TString Database;
        TString DatabaseId = "";
        bool Serverless = false;

        bool FetchRequestIsRunning = true;
        TInstant LastUpdateTime = TInstant::Now();
        ui32 WatchKey = 0;
    };

    using TBase = TActor<TDatabaseSubscriberActor>;

public:
    TDatabaseSubscriberActor(TDuration idleTimeout)
        : TBase(&TDatabaseSubscriberActor::StateFunc)
        , IdleTimeout(idleTimeout)
        , DatabaseStates(std::numeric_limits<size_t>::max())
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    void Handle(TEvPrivate::TEvSubscribeOnDatabase::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        auto databaseStateIt = DatabaseStates.Find(database);

        if (databaseStateIt == DatabaseStates.End()) {
            DatabaseStates.Insert({database, TDatabaseState{.Database = database}});
            Register(NWorkload::CreateDatabaseFetcherActor(SelfId(), database));
            StartIdleCheck();
            return;
        }

        databaseStateIt->LastUpdateTime = TInstant::Now();
        if (databaseStateIt->DatabaseId) {
            SendSubscriberInfo(*databaseStateIt, Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvPingDatabaseSubscription::TPtr& ev) {
        auto databaseStateIt = DatabaseStates.Find(ev->Get()->Database);
        if (databaseStateIt != DatabaseStates.End()) {
            databaseStateIt->LastUpdateTime = TInstant::Now();
        }
    }

    void Handle(NWorkload::TEvFetchDatabaseResponse::TPtr& ev) {
        auto databaseStateIt = DatabaseStates.Find(ev->Get()->Database);
        if (databaseStateIt == DatabaseStates.End()) {
            return;
        }

        databaseStateIt->FetchRequestIsRunning = false;
        databaseStateIt->LastUpdateTime = TInstant::Now();
        databaseStateIt->DatabaseId = ev->Get()->DatabaseId;
        databaseStateIt->Serverless = ev->Get()->Serverless;
        SendSubscriberInfo(*databaseStateIt, ev->Get()->Status, ev->Get()->Issues);

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            FreeWatchKey++;
            databaseStateIt->WatchKey = FreeWatchKey;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(ev->Get()->PathId, FreeWatchKey));
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        auto databaseStateIt = DatabaseStates.Find(ev->Get()->Path);
        if (databaseStateIt == DatabaseStates.End()) {
            return;
        }

        UnsubscribeFromSchemeCache(*databaseStateIt);
        SendSubscriberInfo(*databaseStateIt, Ydb::StatusIds::NOT_FOUND, {NYql::TIssue{"Database was dropped"}});
        DatabaseStates.Erase(databaseStateIt);
    }

    void HandlePoison() {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(0));
        TBase::PassAway();
    }

    void HandleWakeup() {
        IdleCheckStarted = false;
        const auto minimalTime = TInstant::Now() - IdleTimeout;
        while (!DatabaseStates.Empty()) {
            auto oldestIt = DatabaseStates.FindOldest();
            if (oldestIt->LastUpdateTime > minimalTime) {
                break;
            }

            UnsubscribeFromSchemeCache(*oldestIt);
            SendSubscriberInfo(*oldestIt, Ydb::StatusIds::ABORTED, {NYql::TIssue{"Database subscription was dropped by idle timeout"}});
            DatabaseStates.Erase(oldestIt);
        }

        if (!DatabaseStates.Empty()) {
            StartIdleCheck();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSubscribeOnDatabase, Handle);
        hFunc(TEvPrivate::TEvPingDatabaseSubscription, Handle);
        hFunc(NWorkload::TEvFetchDatabaseResponse, Handle);
        sFunc(TEvents::TEvPoison, HandlePoison);
        sFunc(TEvents::TEvWakeup, HandleWakeup);

        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated);
    )

private:
    void UnsubscribeFromSchemeCache(TDatabaseState& databaseState) const {
        if (databaseState.WatchKey) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(databaseState.WatchKey));
            databaseState.WatchKey = 0;
        }
    }

    void SendSubscriberInfo(const TDatabaseState& databaseState, Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::UNSUPPORTED) {
            Send(Owner, new TEvKqp::TEvUpdateDatabaseInfo(databaseState.Database, databaseState.DatabaseId, databaseState.Serverless));
        } else {
            NYql::TIssue rootIssue(TStringBuilder() << "Failed to describe database " << databaseState.Database);
            for (const auto& issue : issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
            Send(Owner, new TEvKqp::TEvUpdateDatabaseInfo(databaseState.Database, status, {rootIssue}));
        }
    }

    void StartIdleCheck() {
        if (!IdleCheckStarted) {
            IdleCheckStarted = true;
            Schedule(IdleTimeout, new TEvents::TEvWakeup());
        }
    }

private:
    const TDuration IdleTimeout;
    TActorId Owner;
    bool IdleCheckStarted = false;

    TLRUCache<TString, TDatabaseState> DatabaseStates;
    ui32 FreeWatchKey = 0;
};

}  // anonymous namespace

TDatabasesCache::TDatabasesCache(TDuration idleTimeout)
    : IdleTimeout(idleTimeout)
{}

void TDatabasesCache::UpdateDatabaseInfo(TEvKqp::TEvUpdateDatabaseInfo::TPtr& event, TActorContext actorContext) {
    auto it = DatabasesCache.find(event->Get()->Database);
    if (it == DatabasesCache.end()) {
        return;
    }
    it->second.DatabaseId = event->Get()->DatabaseId;

    const bool success = event->Get()->Status == Ydb::StatusIds::SUCCESS;
    for (auto& delayedEvent : it->second.DelayedEvents) {
        if (success) {
            actorContext.Send(std::move(delayedEvent.Event));
        } else {
            actorContext.Send(actorContext.SelfID, new TEvKqp::TEvDelayedRequestError(std::move(delayedEvent.Event), event->Get()->Status, event->Get()->Issues), 0, delayedEvent.RequestType);
        }
    }
    it->second.DelayedEvents.clear();

    if (!success) {
        DatabasesCache.erase(it);
    }
}

void TDatabasesCache::SubscribeOnDatabase(const TString& database, TActorContext actorContext) {
    if (!SubscriberActor) {
        SubscriberActor = actorContext.Register(new TDatabaseSubscriberActor(IdleTimeout));
    }
    actorContext.Send(SubscriberActor, new TEvPrivate::TEvSubscribeOnDatabase(database));
}

void TDatabasesCache::PingDatabaseSubscription(const TString& database, TActorContext actorContext) const {
    if (SubscriberActor) {
        actorContext.Send(SubscriberActor, new TEvPrivate::TEvPingDatabaseSubscription(database));
    }
}

void TDatabasesCache::StopSubscriberActor(TActorContext actorContext) const {
    if (SubscriberActor) {
        actorContext.Send(SubscriberActor, new TEvents::TEvPoison());
    }
}

}  // namespace NKikimr::NKqp
