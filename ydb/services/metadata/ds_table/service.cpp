#include "service.h"

#include "accessor_subscribe.h"
#include "behaviour_registrator_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/initializer/behaviour.h>

namespace NKikimr::NMetadata::NProvider {

IActor* CreateService(const TConfig& config) {
    return new TService(config);
}

void TService::PrepareManagers(std::vector<IClassBehaviour::TPtr> managers, TAutoPtr<IEventBase> ev, const NActors::TActorId& sender) {
    TBehavioursId id(managers);
    if (RegistrationData->GetSnapshotOwner()->HasInitializationSnapshot()) {
        auto bInitializer = NInitializer::TDBObjectBehaviour::GetInstance();
        switch (RegistrationData->GetStage()) {
            case TRegistrationData::EStage::Created:
                RegistrationData->StartInitialization();
                Y_ABORT_UNLESS(RegistrationData->InRegistration.emplace(bInitializer->GetTypeId(), bInitializer).second);
                RegisterWithSameMailbox(new TBehaviourRegistrator(bInitializer, RegistrationData, Config.GetRequestConfig()));
                break;
            case TRegistrationData::EStage::WaitInitializerInfo:
                break;
            case TRegistrationData::EStage::Active:
                for (auto&& b : managers) {
                    Y_ABORT_UNLESS(!RegistrationData->Registered.contains(b->GetTypeId()));
                    if (!RegistrationData->InRegistration.contains(b->GetTypeId()) && !RegistrationData->Registered.contains(b->GetTypeId())) {
                        RegistrationData->InRegistration.emplace(b->GetTypeId(), b);
                        RegisterWithSameMailbox(new TBehaviourRegistrator(b, RegistrationData, Config.GetRequestConfig()));
                    }
                }
                break;
        }
    }
    RegistrationData->EventsWaiting->Add(id, ev, sender);
}

void TService::Handle(TEvPrepareManager::TPtr& ev) {
    auto it = RegistrationData->Registered.find(ev->Get()->GetManager()->GetTypeId());
    if (it != RegistrationData->Registered.end()) {
        Send(ev->Sender, new TEvManagerPrepared(it->second));
    } else {
        auto m = ev->Get()->GetManager();
        PrepareManagers({ m }, ev->ReleaseBase(), ev->Sender);
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
    auto command = ev->Get()->GetCommand();
    if (command->GetBehaviour()->GetTypeId() == NInitializer::TDBInitialization::GetTypeId()) {
        command->SetBehaviour(NInitializer::TDBInitialization::GetBehaviour());
        command->Execute();
    } else {
        auto it = RegistrationData->Registered.find(command->GetBehaviour()->GetTypeId());
        if (it != RegistrationData->Registered.end()) {
            command->Execute();
        } else {
            auto b = command->GetBehaviour();
            PrepareManagers({ b }, ev->ReleaseBase(), ev->Sender);
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
    RegistrationData->SetInitializationSnapshot(ev->Get()->GetSnapshot());
}

void TService::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    RegistrationData->EventsWaiting = std::make_shared<TEventsCollector>(SelfId());
    ALS_INFO(NKikimrServices::METADATA_PROVIDER) << "metadata service started" << Endl;
    Become(&TService::StateMain);
    Send(SelfId(), new TEvSubscribeExternal(RegistrationData->GetInitializationFetcher()));
}


}
