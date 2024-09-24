#include "kqp_proxy_service_impl.h"

#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/events.h>

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
    using TDatabaseStatePtr = typename std::list<TDatabaseState>::iterator;

public:
    TDatabaseSubscriberActor(TDuration idleTimeout)
        : TBase(&TDatabaseSubscriberActor::StateFunc)
        , IdleTimeout(idleTimeout)
    {}

    void Registered(TActorSystem* sys, const TActorId& owner) {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    void Handle(TEvPrivate::TEvSubscribeOnDatabase::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const auto it = DatabasePathToState.find(database);

        if (it == DatabasePathToState.end()) {
            DatabaseStates.emplace_front(TDatabaseState{.Database = database});
            DatabasePathToState.insert({database, DatabaseStates.begin()});
            Register(NWorkload::CreateDatabaseFetcherActor(SelfId(), database));
            StartIdleCheck();
            return;
        }

        const auto databaseState = it->second;
        if (databaseState->DatabaseId) {
            SendSubscriberInfo(*databaseState, Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvPingDatabaseSubscription::TPtr& ev) {
        const auto it = DatabasePathToState.find(ev->Get()->Database);
        if (it == DatabasePathToState.end()) {
            return;
        }

        TDatabaseState databaseState = *it->second;
        databaseState.LastUpdateTime = TInstant::Now();

        DatabaseStates.erase(it->second);
        DatabaseStates.emplace_front(databaseState);
        it->second = DatabaseStates.begin();
    }

    void Handle(NWorkload::TEvFetchDatabaseResponse::TPtr& ev) {
        const auto it = DatabasePathToState.find(ev->Get()->Database);
        if (it == DatabasePathToState.end()) {
            return;
        }

        const auto databaseState = it->second;
        databaseState->FetchRequestIsRunning = false;
        UpdateDatabaseState(*databaseState, ev->Get()->PathId, ev->Get()->Serverless);
        SendSubscriberInfo(*databaseState, ev->Get()->Status, ev->Get()->Issues);

        if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
            FreeWatchKey++;
            databaseState->WatchKey = FreeWatchKey;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(ev->Get()->PathId, FreeWatchKey));
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        const auto it = DatabasePathToState.find(ev->Get()->Path);
        if (it == DatabasePathToState.end()) {
            return;
        }

        const auto databaseState = it->second;
        UnsubscribeFromSchemeCache(*databaseState);
        SendSubscriberInfo(*databaseState, Ydb::StatusIds::NOT_FOUND, {NYql::TIssue{"Database was dropped"}});
        DatabasePathToState.erase(it);
        DatabaseStates.erase(databaseState);
    }

    void HandlePoison() {
        for (auto& databaseState : DatabaseStates) {
            UnsubscribeFromSchemeCache(databaseState);
        }

        TBase::PassAway();
    }

    void HandleWakeup() {
        IdleCheckStarted = false;
        const auto minimalTime = TInstant::Now() - IdleTimeout;
        while (!DatabaseStates.empty() && DatabaseStates.back().LastUpdateTime <= minimalTime) {
            UnsubscribeFromSchemeCache(DatabaseStates.back());
            SendSubscriberInfo(DatabaseStates.back(), Ydb::StatusIds::ABORTED, {NYql::TIssue{"Database subscription was dropped by idle timeout"}});
            DatabasePathToState.erase(DatabaseStates.back().Database);
            DatabaseStates.pop_back();
        }

        if (!DatabaseStates.empty()) {
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
    static void UpdateDatabaseState(TDatabaseState& databaseState, TPathId pathId, bool serverless) {
        databaseState.DatabaseId = (serverless ? TStringBuilder() << pathId.OwnerId << ":" << pathId.LocalPathId << ":" : TStringBuilder()) << databaseState.Database;
        databaseState.Serverless = serverless;
    }

    void UnsubscribeFromSchemeCache(TDatabaseState& databaseState) const {
        if (databaseState.WatchKey) {
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(databaseState.WatchKey));
            databaseState.WatchKey = 0;
        }
    }

    void SendSubscriberInfo(TDatabaseState& databaseState, Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
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

    std::unordered_map<TString, TDatabaseStatePtr> DatabasePathToState;
    std::list<TDatabaseState> DatabaseStates;
    ui32 FreeWatchKey = 0;
};

}  // anonymous namespace

TDatabasesCache::TDatabasesCache(TDuration idleTimeout)
    : IdleTimeout(idleTimeout)
{}

const TString& TDatabasesCache::GetTenantName() {
    if (!TenantName) {
        TenantName = CanonizePath(AppData()->TenantName);
    }
    return TenantName;
}

void TDatabasesCache::UpdateDatabaseInfo(TEvKqp::TEvUpdateDatabaseInfo::TPtr& event, TActorContext actorContext) {
    const auto& database = event->Get()->Database;
    auto it = DatabasesCache.find(database);
    Y_ABORT_UNLESS(it != DatabasesCache.end());
    it->second.DatabaseId = event->Get()->DatabaseId;

    const bool success = event->Get()->Status == Ydb::StatusIds::SUCCESS;
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
