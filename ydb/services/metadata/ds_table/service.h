#pragma once
#include "accessor_subscribe.h"
#include "config.h"

#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/initializer/events.h>
#include <ydb/services/metadata/initializer/manager.h>
#include <ydb/services/metadata/initializer/snapshot.h>
#include <ydb/services/metadata/initializer/fetcher.h>

#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadataProvider {

class TServiceInternalController: public NMetadataInitializer::IInitializerOutput {
private:
    const NActors::TActorIdentity ActorId;
public:
    TServiceInternalController(const NActors::TActorIdentity& actorId)
        : ActorId(actorId)
    {

    }

    virtual void InitializationFinished(const TString& id) const override;
};

class TManagersId {
private:
    YDB_READONLY_DEF(std::set<TString>, ManagerIds);
public:
    TManagersId(const std::vector<NMetadata::IOperationsManager::TPtr>& managers) {
        for (auto&& i : managers) {
            ManagerIds.emplace(i->GetTypeId());
        }
    }

    bool IsEmpty() const {
        return ManagerIds.empty();
    }

    bool RemoveId(const TString& id) {
        auto it = ManagerIds.find(id);
        if (it == ManagerIds.end()) {
            return false;
        }
        ManagerIds.erase(it);
        return true;
    }

    bool operator<(const TManagersId& item) const {
        if (ManagerIds.size() < item.ManagerIds.size()) {
            return true;
        } else if (ManagerIds.size() > item.ManagerIds.size()) {
            return false;
        } else {
            auto itSelf = ManagerIds.begin();
            auto itItem = item.ManagerIds.begin();
            while (itSelf != ManagerIds.end()) {
                if (*itSelf < *itItem) {
                    return true;
                }
                ++itSelf;
                ++itItem;
            }
            return false;
        }
    }
};

class TWaitEvent {
private:
    TAutoPtr<IEventBase> Event;
    const TActorId Sender;
public:
    TWaitEvent(TAutoPtr<IEventBase> ev, const TActorId& sender)
        : Event(ev)
        , Sender(sender)
    {

    }

    void Resend(const TActorIdentity& receiver) {
        TActivationContext::Send(new IEventHandle(receiver, Sender, Event.Release()));
    }
};

class TWaitingWatcher {
private:
};

class TService: public NActors::TActorBootstrapped<TService> {
private:
    using TBase = NActors::TActor<TService>;
    bool ActiveFlag = false;
    bool PreparationFlag = false;
    std::map<TString, NActors::TActorId> Accessors;
    std::map<TManagersId, std::deque<TWaitEvent>> EventsWaiting;
    std::map<TString, NMetadata::IOperationsManager::TPtr> ManagersInRegistration;
    std::map<TString, NMetadata::IOperationsManager::TPtr> RegisteredManagers;

    std::shared_ptr<NMetadataInitializer::TFetcher> InitializationFetcher;
    std::shared_ptr<NMetadataInitializer::TSnapshot> InitializationSnapshot;
    std::shared_ptr<TServiceInternalController> InternalController;
    const TConfig Config;

    void Handle(NMetadataInitializer::TEvInitializationFinished::TPtr& ev);
    void Handle(TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvAskSnapshot::TPtr& ev);
    void Handle(TEvSubscribeExternal::TPtr& ev);
    void Handle(TEvUnsubscribeExternal::TPtr& ev);
    void Handle(TEvObjectsOperation::TPtr& ev);
    void PrepareManagers(std::vector<NMetadata::IOperationsManager::TPtr> manager, TAutoPtr<IEventBase> ev, const NActors::TActorId& sender);

    template <class TEventPtr, class TAction>
    void ProcessEventWithFetcher(TEventPtr& ev, TAction action) {
        std::vector<NMetadata::IOperationsManager::TPtr> needManagers;
        for (auto&& i : ev->Get()->GetFetcher()->GetManagers()) {
            if (!RegisteredManagers.contains(i->GetTypeId())) {
                needManagers.emplace_back(i);
            }
        }
        if (needManagers.empty()) {
            auto it = Accessors.find(ev->Get()->GetFetcher()->GetComponentId());
            if (it == Accessors.end()) {
                THolder<TExternalData> actor = MakeHolder<TExternalData>(Config, ev->Get()->GetFetcher());
                it = Accessors.emplace(ev->Get()->GetFetcher()->GetComponentId(), Register(actor.Release())).first;
            }
            action(it->second);
        } else {
            PrepareManagers(needManagers, ev->ReleaseBase(), ev->Sender);
        }
    }

public:

    void Bootstrap(const NActors::TActorContext& /*ctx*/);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvObjectsOperation, Handle);
            hFunc(TEvRefreshSubscriberData, Handle);
            hFunc(TEvAskSnapshot, Handle);
            hFunc(TEvSubscribeExternal, Handle);
            hFunc(TEvUnsubscribeExternal, Handle);
            hFunc(NMetadataInitializer::TEvInitializationFinished, Handle);
            default:
                Y_VERIFY(false);
        }
    }

    TService(const TConfig& config)
        : Config(config)
    {
        TServiceOperator::Register(Config);
    }
};

NActors::IActor* CreateService(const TConfig& config);

}
