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
    bool Add(TAutoPtr<IEventBase> ev, const TActorId& sender) {
        if (Events.size() > 10000) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "too many events for deferred sending (maybe service cannot start)";
            return false;
        }
        Events.emplace_back(ev, sender);
        return true;
    }

    template <class T>
    bool Add(TEventHandle<T>& ev) {
        return Add(ev.ReleaseBase(), ev.Sender);
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

template <class TTag = TString>
class TTaggedEventsWaiter {
private:
    std::map<TString, TEventsWaiter> TaggedEvents;
    ui32 EventsCount = 0;
public:
    bool Add(TAutoPtr<IEventBase> ev, const TActorId& sender, const TString& tag) {
        if (EventsCount > 10000) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "too many events for deferred sending (maybe service cannot start)";
            return false;
        }
        TaggedEvents[tag].Add(ev, sender);
        ++EventsCount;
        return true;
    }

    template <class T>
    bool Add(TEventHandle<T>& ev, const TString& tag) {
        return Add(ev.ReleaseBase(), ev.Sender, tag);
    }

    bool ResendOne(const TActorIdentity& receiver, const TString& tag) {
        auto it = TaggedEvents.find(tag);
        if (it == TaggedEvents.end()) {
            return false;
        }
        if (it->second.ResendOne(receiver)) {
            --EventsCount;
            return true;
        } else {
            return false;
        }
    }

    void ResendAll(const TActorIdentity& receiver) {
        for (auto&& i : TaggedEvents) {
            i.second.ResendAll(receiver);
        }
        TaggedEvents.clear();
        EventsCount = 0;
    }

    bool IsEmpty() const {
        return EventsCount == 0;
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

class TRegistrationData;

class TInitializationSnapshotOwner {
private:
    mutable TMutex Mutex;
    std::shared_ptr<NInitializer::TSnapshot> InitializationSnapshot;
    friend class TRegistrationData;
    void SetInitializationSnapshot(NFetcher::ISnapshot::TPtr s) {
        auto snapshot = dynamic_pointer_cast<NInitializer::TSnapshot>(s);
        Y_ABORT_UNLESS(snapshot);
        TGuard<TMutex> g(Mutex);
        InitializationSnapshot = snapshot;
    }

    void NoInitializationSnapshot() {
        TGuard<TMutex> g(Mutex);
        InitializationSnapshot = std::make_shared<NInitializer::TSnapshot>(TInstant::Zero());
    }
public:
    bool HasInitializationSnapshot() const {
        TGuard<TMutex> g(Mutex);
        return !!InitializationSnapshot;
    }
    bool HasModification(const TString& componentId, const TString& modificationId) const {
        NInitializer::TDBInitializationKey key(componentId, modificationId);
        TGuard<TMutex> g(Mutex);
        return InitializationSnapshot->GetObjects().contains(key);
    }

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
    YDB_READONLY_DEF(std::shared_ptr<NInitializer::TFetcher>, InitializationFetcher);
    YDB_READONLY_DEF(std::shared_ptr<TInitializationSnapshotOwner>, SnapshotOwner);
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
