#include "service.h"

#include "accessor_subscribe.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadataProvider {

IActor* CreateService(const TConfig& config) {
    return new TService(config);
}

void TService::PrepareManagers(std::vector<NMetadata::IOperationsManager::TPtr> managers, TAutoPtr<IEventBase> ev, const NActors::TActorId& sender) {
    TManagersId id(managers);
    const bool isInitialization = (managers.size() == 1) && managers.front()->GetTypeId() == NMetadataInitializer::TManager::GetTypeIdStatic();
    if (ActiveFlag || (PreparationFlag && isInitialization)) {
        for (auto&& manager : managers) {
            Y_VERIFY(!RegisteredManagers.contains(manager->GetTypeId()));
            if (!ManagersInRegistration.contains(manager->GetTypeId()) && !RegisteredManagers.contains(manager->GetTypeId())) {
                ManagersInRegistration.emplace(manager->GetTypeId(), manager);
                Register(new NMetadataInitializer::TDSAccessorInitialized(Config.GetRequestConfig(),
                    manager->GetTypeId(), manager->GetInitializationBehaviour(), InternalController, InitializationSnapshot));
            }
        }
    } else if (!PreparationFlag) {
        PreparationFlag = true;
        InitializationFetcher = std::make_shared<NMetadataInitializer::TFetcher>();
        Send(SelfId(), new TEvSubscribeExternal(InitializationFetcher));
    }
    EventsWaiting[id].emplace_back(ev, sender);
}

void TService::Handle(NMetadataInitializer::TEvInitializationFinished::TPtr& ev) {
    const TString& initId = ev->Get()->GetInitializationId();

    auto it = ManagersInRegistration.find(initId);
    Y_VERIFY(it != ManagersInRegistration.end());
    RegisteredManagers.emplace(initId, it->second);
    ManagersInRegistration.erase(it);

    std::map<TManagersId, std::deque<TWaitEvent>> movedEvents;
    for (auto it = EventsWaiting.begin(); it != EventsWaiting.end(); ) {
        auto m = it->first;
        if (!m.RemoveId(initId)) {
            ++it;
            continue;
        }
        if (m.IsEmpty()) {
            for (auto&& i : it->second) {
                i.Resend(SelfId());
            }
        } else {
            auto itNext = EventsWaiting.find(m);
            if (itNext == EventsWaiting.end()) {
                movedEvents.emplace(m, std::move(it->second));
            } else {
                for (auto&& i : it->second) {
                    itNext->second.emplace_back(std::move(i));
                }
            }
        }
        it = EventsWaiting.erase(it);
    }
    for (auto&& i : movedEvents) {
        EventsWaiting.emplace(i.first, std::move(i.second));
    }
}

void TService::Handle(TEvSubscribeExternal::TPtr& ev) {
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
        Send<TEvSubscribe>(it->second, ev->Sender);
    } else {
        PrepareManagers(needManagers, ev->ReleaseBase(), ev->Sender);
    }
}

void TService::Handle(TEvAskSnapshot::TPtr& ev) {
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
        Send<TEvAsk>(it->second, ev->Sender);
    } else {
        PrepareManagers(needManagers, ev->ReleaseBase(), ev->Sender);
    }
}

void TService::Handle(TEvAlterObjects::TPtr& ev) {
    auto it = RegisteredManagers.find(ev->Get()->GetCommand()->GetManager()->GetTypeId());
    if (it != RegisteredManagers.end()) {
        ev->Get()->GetCommand()->Execute();
    } else {
        auto m = ev->Get()->GetCommand()->GetManager();
        PrepareManagers({ m }, ev->ReleaseBase(), ev->Sender);
    }
}

void TService::Handle(TEvUnsubscribeExternal::TPtr& ev) {
    auto it = Accessors.find(ev->Get()->GetFetcher()->GetComponentId());
    if (it != Accessors.end()) {
        Send<TEvUnsubscribe>(it->second, ev->Sender);
    }
}

void TService::Handle(TEvRefreshSubscriberData::TPtr& ev) {
    auto s = ev->Get()->GetSnapshot();
    InitializationSnapshot = dynamic_pointer_cast<NMetadataInitializer::TSnapshot>(s);
    Y_VERIFY(InitializationSnapshot);
    if (!ActiveFlag) {
        ActiveFlag = true;
        for (auto&& i : EventsWaiting) {
            i.second.front().Resend(SelfId());
            i.second.pop_front();
        }
    }
}

void TService::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    Become(&TService::StateMain);
    InternalController = std::make_shared<TServiceInternalController>(SelfId());
}

void TServiceInternalController::InitializationFinished(const TString& id) const {
    ActorId.Send(ActorId, new NMetadataInitializer::TEvInitializationFinished(id));
}

}
