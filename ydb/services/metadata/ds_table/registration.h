#pragma once
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/initializer/fetcher.h>
#include <ydb/services/metadata/initializer/snapshot.h>

namespace NKikimr::NMetadata::NProvider {

class TBehavioursId {
private:
    YDB_READONLY_DEF(std::set<TString>, BehaviourIds);
public:
    TBehavioursId(const std::vector<IClassBehaviour::TPtr>& managers) {
        for (auto&& i : managers) {
            BehaviourIds.emplace(i->GetTypeId());
        }
    }

    bool IsEmpty() const {
        return BehaviourIds.empty();
    }

    bool RemoveId(const TString& id);

    bool operator<(const TBehavioursId& item) const;
};

class TWaitEvent {
private:
    TAutoPtr<IEventBase> Event;
    const TActorId Sender;
public:
    TWaitEvent(TAutoPtr<IEventBase> ev, const TActorId& sender)
        : Event(ev)
        , Sender(sender) {

    }

    void Resend(const TActorIdentity& receiver) {
        TActivationContext::Send(new IEventHandle(receiver, Sender, Event.Release()));
    }
};

class TEventsWaiter {
private:
    std::deque<TWaitEvent> Events;
public:
    void Add(TAutoPtr<IEventBase> ev, const TActorId& sender) {
        if (Events.size() > 10000) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "too many events for deferred sending (maybe service cannot start)";
            return;
        }
        Events.emplace_back(ev, sender);
    }

    template <class T>
    void Add(TEventHandle<T>& ev) {
        Add(ev.ReleaseBase(), ev.Sender);
    }

    bool ResendOne(const TActorIdentity& receiver) {
        if (Events.empty()) {
            return false;
        }
        Events.front().Resend(receiver);
        Events.pop_front();
        return true;
    }

    void ResendAll(const TActorIdentity& receiver) {
        while (ResendOne(receiver)) {
        }
    }

    bool IsEmpty() const {
        return Events.empty();
    }

    void Merge(TEventsWaiter&& source) {
        Merge(std::move(source.Events));
    }

    void Merge(std::deque<TWaitEvent>&& source) {
        for (auto&& i : source) {
            Events.emplace_back(std::move(i));
        }
    }
};

class TEventsCollector {
private:
    const TActorIdentity OwnerId;
    std::map<TBehavioursId, TEventsWaiter> Events;
public:
    TEventsCollector(const TActorIdentity& ownerId)
        : OwnerId(ownerId) {

    }

    const TActorIdentity& GetOwnerId() const {
        return OwnerId;
    }

    void Add(const TBehavioursId& id, TAutoPtr<IEventBase> ev, const TActorId& sender) {
        Events[id].Add(ev, sender);
    }

    void TryResendOne() {
        for (auto&& i : Events) {
            i.second.ResendOne(OwnerId);
        }
    }

    void Initialized(const TString& initId);
};

class TRegistrationData {
public:
    enum class EStage {
        Created,
        WaitInitializerInfo,
        Active
    };
private:
    YDB_READONLY(EStage, Stage, EStage::Created);
    YDB_READONLY_DEF(std::shared_ptr<NInitializer::TSnapshot>, InitializationSnapshot);
    YDB_READONLY_DEF(std::shared_ptr<NInitializer::TFetcher>, InitializationFetcher);
public:
    TRegistrationData();

    void StartInitialization();

    std::map<TString, IClassBehaviour::TPtr> InRegistration;
    std::map<TString, IClassBehaviour::TPtr> Registered;
    std::shared_ptr<TEventsCollector> EventsWaiting;

    void SetInitializationSnapshot(NFetcher::ISnapshot::TPtr s);
    void NoInitializationSnapshot();

    void InitializationFinished(const TString& initId);

};

}
