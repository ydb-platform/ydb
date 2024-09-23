#include "accessor_init.h"
#include "manager.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/service.h>
#include <ydb/library/actors/core/invoke.h>

namespace NKikimr::NMetadata::NInitializer {

void TDSAccessorInitialized::DoNextModifier(const bool doPop) {
    if (InitializationSnapshotOwner->HasInitializationSnapshot() && !doPop) {
        while (Modifiers.size() && !doPop) {
            if (InitializationSnapshotOwner->HasModification(ComponentId, Modifiers.front()->GetModificationId())) {
                Modifiers.pop_front();
            } else {
                break;
            }
        }
    }
    if (doPop) {
        Modifiers.pop_front();
    }
    if (Modifiers.size()) {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
        Modifiers.front()->Execute(SelfPtr, Config);
    } else {
        ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "initialization finished";
        ExternalController->OnInitializationFinished(ComponentId);
        SelfPtr.reset();
    }
}

TDSAccessorInitialized::TDSAccessorInitialized(const NRequest::TConfig& config,
    const TString& componentId,
    IInitializationBehaviour::TPtr initializationBehaviour,
    IInitializerOutput::TPtr controller, const std::shared_ptr<NProvider::TInitializationSnapshotOwner>& snapshotOwner)
    : Config(config)
    , InitializationBehaviour(initializationBehaviour)
    , ExternalController(controller)
    , InitializationSnapshotOwner(snapshotOwner)
    , ComponentId(componentId)
{
}

void TDSAccessorInitialized::OnModificationFinished(const TString& modificationId) {
    ALS_INFO(NKikimrServices::METADATA_INITIALIZER) << "modifiers count: " << Modifiers.size();
    Y_ABORT_UNLESS(Modifiers.size());
    Y_ABORT_UNLESS(Modifiers.front()->GetModificationId() == modificationId);
    if (NProvider::TServiceOperator::IsEnabled() && InitializationSnapshotOwner->HasInitializationSnapshot()) {
        TDBInitialization dbInit(ComponentId, Modifiers.front()->GetModificationId());
        NModifications::IOperationsManager::TExternalModificationContext extContext;
        extContext.SetUserToken(NACLib::TSystemUsers::Metadata());
        auto alterCommand = std::make_shared<NModifications::TUpsertObjectCommand<TDBInitialization>>(
            dbInit.SerializeToRecord(), TDBInitialization::GetBehaviour(), SelfPtr,
            NModifications::IOperationsManager::TInternalModificationContext(extContext));

        TActorContext::AsActorContext().Send(NProvider::MakeServiceId(TActorContext::AsActorContext().SelfID.NodeId()),
            new NProvider::TEvObjectsOperation(alterCommand));
    } else {
        DoNextModifier(true);
    }
}

void TDSAccessorInitialized::OnPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) {
    for (auto&& i : modifiers) {
        TDBInitializationKey key(ComponentId, i->GetModificationId());
        Modifiers.emplace_back(i);
    }
    DoNextModifier(false);
}

void TDSAccessorInitialized::OnPreparationProblem(const TString& errorMessage) const {
    AFL_ERROR(NKikimrServices::METADATA_INITIALIZER)("event", "OnPreparationProblem")("error", errorMessage);
    NActors::ScheduleInvokeActivity([self = this->SelfPtr]() {self->InitializationBehaviour->Prepare(self); }, TDuration::Seconds(1));
}

void TDSAccessorInitialized::OnAlteringProblem(const TString& errorMessage) {
    AFL_ERROR(NKikimrServices::METADATA_INITIALIZER)("event", "OnAlteringProblem")("error", errorMessage);
    NActors::ScheduleInvokeActivity([self = this->SelfPtr]() {
        Y_ABORT_UNLESS(self->Modifiers.size());
        self->OnModificationFinished(self->Modifiers.front()->GetModificationId());
    }, TDuration::Seconds(1));
}

void TDSAccessorInitialized::OnModificationFailed(Ydb::StatusIds::StatusCode /*status*/, const TString& errorMessage, const TString& modificationId) {
    AFL_ERROR(NKikimrServices::METADATA_INITIALIZER)("event", "OnModificationFailed")("error", errorMessage)("modificationId", modificationId);
    NActors::ScheduleInvokeActivity([self = this->SelfPtr]() {
        Y_ABORT_UNLESS(self->Modifiers.size());
        self->DoNextModifier(false);
    }, TDuration::Seconds(1));
}

void TDSAccessorInitialized::OnAlteringFinished() {
    DoNextModifier(true);
}

void TDSAccessorInitialized::Execute(const NRequest::TConfig& config, const TString& componentId,
    IInitializationBehaviour::TPtr initializationBehaviour, IInitializerOutput::TPtr controller,
    const std::shared_ptr<NProvider::TInitializationSnapshotOwner>& snapshotOwner)
{
    AFL_VERIFY(snapshotOwner);
    std::shared_ptr<TDSAccessorInitialized> initializer(new TDSAccessorInitialized(config,
        componentId, initializationBehaviour, controller, snapshotOwner));
    initializer->SelfPtr = initializer;

    initializationBehaviour->Prepare(initializer);
}

}
