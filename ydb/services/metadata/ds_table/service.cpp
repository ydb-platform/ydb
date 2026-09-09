#include "service.h"

#include "accessor_subscribe.h"
#include "behaviour_registrator_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/initializer/behaviour.h>
#include <ydb/services/metadata/manager/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METADATA_PROVIDER

namespace NKikimr::NMetadata::NProvider {

namespace {

class TObjectTrackCommand final : public NModifications::IObjectModificationCommand {
    using TBase = NModifications::IObjectModificationCommand;

public:
    class TController final : public NModifications::IAlterController {
    public:
        TController(const TString& typeId, const TString& objectId, NModifications::IOperationsManager::TOperationTrackContext context)
            : TypeId(typeId)
            , ObjectId(objectId)
            , Context(std::move(context))
        {}

    private:
        void OnAlteringProblem(const TString& errorMessage) final {
            Y_UNUSED(errorMessage);
            OnAlteringFinished();
        }

        void OnAlteringFinished() final {
            const auto& externalContext = Context.GetExternalData();
            const auto* actorSystem = externalContext.GetActorSystem();
            Y_VALIDATE(actorSystem, "Missing actor system");

            auto ev = std::make_unique<TEvTrackOperationFinished>();
            ev->SetDatabaseId(externalContext.GetDatabaseId());
            ev->SetTypeId(TypeId);
            ev->SetObjectId(ObjectId);
            ev->SetPathId(Context.GetPathId());
            ev->SetRequestGeneration(Context.GetRequestGeneration());
            ev->SetObjectGeneration(Context.GetObjectGeneration());
            actorSystem->Send(MakeServiceId(actorSystem->NodeId), ev.release());
        }

        const TString TypeId;
        const TString ObjectId;
        const NModifications::IOperationsManager::TOperationTrackContext Context;
    };

    TObjectTrackCommand(const TString& objectId, IClassBehaviour::TPtr behaviour, TController::TPtr controller, const NModifications::IOperationsManager::TOperationTrackContext& context)
        : TBase(std::vector<NInternal::TTableRecord>{}, std::move(behaviour), controller, NModifications::IOperationsManager::TInternalModificationContext(context.GetExternalData()))
        , ObjectId(objectId)
        , Context(context)
    {}

private:
    void DoExecute() const final {
        GetBehaviour()->GetOperationsManager()->TrackObjectOperation(ObjectId, Context).Subscribe([controller = GetController()](const auto&) {
            controller->OnAlteringFinished();
        });
    }

    const TString ObjectId;
    const NModifications::IOperationsManager::TOperationTrackContext Context;
};

} // anonymous namespace

IActor* CreateService(const TConfig& config) {
    return new TService(config);
}

ui64 TService::TTrackOperationId::THash::operator()(const TTrackOperationId& id) const {
    ui64 result = 0;
    result = CombineHashes<ui64>(result, std::hash<TString>()(id.DatabaseId));
    result = CombineHashes<ui64>(result, std::hash<TString>()(id.TypeId));
    result = CombineHashes<ui64>(result, std::hash<TString>()(id.ObjectId));
    result = CombineHashes<ui64>(result, id.PathId.Hash());
    result = CombineHashes<ui64>(result, std::hash<ui64>()(id.RequestGeneration));
    result = CombineHashes<ui64>(result, std::hash<ui64>()(id.ObjectGeneration));
    return result;
}

bool TService::TTrackOperationId::operator==(const TTrackOperationId& other) const {
    return DatabaseId == other.DatabaseId && TypeId == other.TypeId && ObjectId == other.ObjectId
        && PathId == other.PathId && RequestGeneration == other.RequestGeneration && ObjectGeneration == other.ObjectGeneration;
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

void TService::Handle(TEvResetManagerRegistration::TPtr& ev) {
    const auto manager = ev->Get()->GetManager();
    const auto& typeId = manager->GetTypeId();
    if (const auto it = RegistrationData->Registered.find(typeId); it != RegistrationData->Registered.end()) {
        RegistrationData->Registered.erase(it);
    } else if (const auto it = RegistrationData->InRegistration.find(typeId); it != RegistrationData->InRegistration.end()) {
        PrepareManagers({manager}, ev->ReleaseBase(), ev->Sender);
    }
}

void TService::Handle(TEvTrackOperationCompletion::TPtr& ev) {
    const auto id = TTrackOperationId{
        .DatabaseId = ev->Get()->GetDatabaseId(),
        .TypeId = ev->Get()->GetTypeId(),
        .ObjectId = ev->Get()->GetObjectId(),
        .PathId = ev->Get()->GetPathId(),
        .RequestGeneration = ev->Get()->GetRequestGeneration(),
        .ObjectGeneration = ev->Get()->GetObjectGeneration(),
    };
    if (!InflightTrackOperations.emplace(id).second) {
        return;
    }

    // We should start initialization for this object before operation tracing begins
    IClassBehaviour::TPtr cBehaviour(IClassBehaviour::TFactory::Construct(id.TypeId));
    Y_VALIDATE(cBehaviour, "Unsupported object type: \"" << id.TypeId << "\"");

    NModifications::IOperationsManager::TExternalModificationContext externalData;
    externalData.SetUserToken(ev->Get()->GetUserToken());
    externalData.SetDatabase(ev->Get()->GetDatabase());
    externalData.SetDatabaseId(id.DatabaseId);
    externalData.SetActorSystem(TActivationContext::ActorSystem());

    NModifications::IOperationsManager::TOperationTrackContext context(std::move(externalData));
    context.SetPathId(id.PathId);
    context.SetRequestGeneration(id.RequestGeneration);
    context.SetObjectGeneration(id.ObjectGeneration);
    context.SetOperationOwner(ev->Get()->GetOperationOwner());

    auto controller = std::make_shared<TObjectTrackCommand::TController>(id.TypeId, id.ObjectId, context);
    auto command = std::make_shared<TObjectTrackCommand>(id.ObjectId, cBehaviour, std::move(controller), std::move(context));

    Send(SelfId(), new NProvider::TEvObjectsOperation(std::move(command)));
}

void TService::Handle(TEvTrackOperationFinished::TPtr& ev) {
    const auto id = TTrackOperationId{
        .DatabaseId = ev->Get()->GetDatabaseId(),
        .TypeId = ev->Get()->GetTypeId(),
        .ObjectId = ev->Get()->GetObjectId(),
        .PathId = ev->Get()->GetPathId(),
        .RequestGeneration = ev->Get()->GetRequestGeneration(),
        .ObjectGeneration = ev->Get()->GetObjectGeneration(),
    };
    Y_VALIDATE(InflightTrackOperations.erase(id) == 1, "Unexpected track operation finish");
}

void TService::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    RegistrationData->EventsWaiting = std::make_shared<TEventsCollector>(SelfId());
    YDB_LOG_INFO("Metadata service started");
    Become(&TService::StateMain);
    Send(SelfId(), new TEvSubscribeExternal(RegistrationData->GetInitializationFetcher()));
}

} // namespace NKikimr::NMetadata::NProvider
