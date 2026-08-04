#include "behaviour.h"
#include "manager.h"

namespace NKikimr::NKqp {

TKeyValueVolumeBehaviour::TFactory::TRegistrator<TKeyValueVolumeBehaviour> TKeyValueVolumeBehaviour::Registrator(
    TKeyValueVolumeConfig::GetTypeId());

NMetadata::NModifications::IOperationsManager::TPtr TKeyValueVolumeBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TKeyValueVolumeManager>();
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TKeyValueVolumeBehaviour::ConstructInitializer() const {
    return nullptr;
}

TString TKeyValueVolumeBehaviour::GetInternalStorageTablePath() const {
    return TKeyValueVolumeConfig::GetTypeId();
}

TString TKeyValueVolumeBehaviour::GetTypeId() const {
    return TKeyValueVolumeConfig::GetTypeId();
}

}   // namespace NKikimr::NKqp
