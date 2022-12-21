#include "service.h"

#include "accessor_subscribe.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NProvider {

IActor* CreateService(const TConfig& config) {
    return new TService(config);
}

void TService::PrepareManagers(std::vector<IClassBehaviour::TPtr> managers, TAutoPtr<IEventBase> ev, const NActors::TActorId& sender) {
    TManagersId id(managers);
    if (InitializationSnapshot) {
        const bool isInitialization = (managers.size() == 1) && managers.front()->GetTypeId() == NInitializer::TDBInitialization::GetTypeId();
        if (ActiveFlag || (PreparationFlag && isInitialization)) {
            for (auto&& manager : managers) {
                Y_VERIFY(!RegisteredManagers.contains(manager->GetTypeId()));
                if (!ManagersInRegistration.contains(manager->GetTypeId()) && !RegisteredManagers.contains(manager->GetTypeId())) {
                    ManagersInRegistration.emplace(manager->GetTypeId(), manager);
                    Register(new NInitializer::TDSAccessorInitialized(Config.GetRequestConfig(),
                        manager->GetTypeId(), manager->GetInitializer(), InternalController, InitializationSnapshot));
                }
            }
        } else if (!PreparationFlag) {
            PreparationFlag = true;
            Send(SelfId(), new TEvSubscribeExternal(InitializationFetcher));
        }
    }
    EventsWaiting[id].emplace_back(ev, sender);
}

void TService::Handle(TEvPrepareManager::TPtr& ev) {
    auto it = RegisteredManagers.find(ev->Get()->GetManager()->GetTypeId());
    if (it != RegisteredManagers.end()) {
        Send(ev->Sender, new TEvManagerPrepared(it->second));
    } else {
        auto m = ev->Get()->GetManager();
        PrepareManagers({ m }, ev->ReleaseBase(), ev->Sender);
    }
}

void TService::InitializationFinished(const TString& initId) {
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

void TService::Handle(TEvTableDescriptionSuccess::TPtr& ev) {
    const TString& initId = ev->Get()->GetRequestId();
    auto it = ManagersInRegistration.find(initId);
    Y_VERIFY(it != ManagersInRegistration.end());
    it->second->GetOperationsManager()->SetActualSchema(ev->Get()->GetSchema());
    InitializationFinished(initId);
}

void TService::Handle(TEvTableDescriptionFailed::TPtr& ev) {
    const TString& initId = ev->Get()->GetRequestId();
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "metadata service cannot receive table description for " << initId << Endl;
    Schedule(TDuration::Seconds(1), new NInitializer::TEvInitializationFinished(initId));
}

void TService::Handle(NInitializer::TEvInitializationFinished::TPtr& ev) {
    const TString& initId = ev->Get()->GetInitializationId();

    auto it = ManagersInRegistration.find(initId);
    Y_VERIFY(it != ManagersInRegistration.end());
    if (it->second->GetOperationsManager()) {
        Register(new TSchemeDescriptionActor(InternalController, initId, it->second->GetStorageTablePath()));
    } else {
        InitializationFinished(initId);
    }
}

void TService::Handle(TEvSubscribeExternal::TPtr& ev) {
    const TActorId senderId = ev->Sender;
    ProcessEventWithFetcher(*ev, ev->Get()->GetFetcher(), [this, senderId](const TActorId& actorId) {
        Send<TEvSubscribe>(actorId, senderId);
        });
}

void TService::Handle(TEvAskSnapshot::TPtr& ev) {
    const TActorId senderId = ev->Sender;
    ProcessEventWithFetcher(*ev, ev->Get()->GetFetcher(), [this, senderId](const TActorId& actorId) {
        Send<TEvAsk>(actorId, senderId);
        });
}

void TService::Handle(TEvObjectsOperation::TPtr& ev) {
    if (ev->Get()->GetCommand()->GetManager()->GetTypeId() == NInitializer::TDBInitialization::GetTypeId()) {
        ev->Get()->GetCommand()->SetManager(NInitializer::TDBInitialization::GetBehaviour());
        ev->Get()->GetCommand()->Execute();
    } else {
        auto it = RegisteredManagers.find(ev->Get()->GetCommand()->GetManager()->GetTypeId());
        if (it != RegisteredManagers.end()) {
            ev->Get()->GetCommand()->SetManager(it->second);
            ev->Get()->GetCommand()->Execute();
        } else {
            auto m = ev->Get()->GetCommand()->GetManager();
            PrepareManagers({ m }, ev->ReleaseBase(), ev->Sender);
        }
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
    InitializationSnapshot = dynamic_pointer_cast<NInitializer::TSnapshot>(s);
    Y_VERIFY(InitializationSnapshot);
    if (!ActiveFlag) {
        ActiveFlag = true;
        Activate();
    }
}

void TService::Activate() {
    for (auto&& i : EventsWaiting) {
        i.second.front().Resend(SelfId());
        i.second.pop_front();
    }
}

void TService::Handle(TDSAccessorSimple::TEvController::TEvResult::TPtr& ev) {
    InitializationSnapshot = dynamic_pointer_cast<NInitializer::TSnapshot>(ev->Get()->GetResult());
    Y_VERIFY(InitializationSnapshot);
    Activate();
}

void TService::Handle(TEvStartMetadataService::TPtr& /*ev*/) {
    Register(new TDSAccessorSimple(Config.GetRequestConfig(), InternalController, InitializationFetcher));
}

void TService::Handle(TDSAccessorSimple::TEvController::TEvError::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot receive initializer snapshot: " << ev->Get()->GetErrorMessage() << Endl;
    Schedule(TDuration::Seconds(1), new TEvStartMetadataService());
}

void TService::Handle(TDSAccessorSimple::TEvController::TEvTableAbsent::TPtr& /*ev*/) {
    InitializationSnapshot = std::make_shared<NInitializer::TSnapshot>(TInstant::Zero());
    Activate();
}

void TService::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "metadata service started" << Endl;
    Become(&TService::StateMain);
    InternalController = std::make_shared<TServiceInternalController>(SelfId());
    InitializationFetcher = std::make_shared<NInitializer::TFetcher>();
    Sender<TEvStartMetadataService>().SendTo(SelfId());
}

void TServiceInternalController::InitializationFinished(const TString& id) const {
    ActorId.Send(ActorId, new NInitializer::TEvInitializationFinished(id));
}

void TServiceInternalController::OnDescriptionFailed(const TString& errorMessage, const TString& requestId) const {
    ActorId.Send(ActorId, new TEvTableDescriptionFailed(errorMessage, requestId));
}

void TServiceInternalController::OnDescriptionSuccess(THashMap<ui32, TSysTables::TTableColumnInfo>&& result, const TString& requestId) const {
    ActorId.Send(ActorId, new TEvTableDescriptionSuccess(std::move(result), requestId));
}

}
