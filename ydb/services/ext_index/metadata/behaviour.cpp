#include "behaviour.h"
#include "initializer.h"
#include "manager.h"

namespace NKikimr::NMetadata::NCSIndex {

TBehaviour::TFactory::TRegistrator<TBehaviour> TBehaviour::Registrator(TObject::GetTypeId());

TString TBehaviour::GetInternalStorageTablePath() const {
    return "cs_index/external";
}

NModifications::IOperationsManager::TPtr TBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TManager>();
}

NInitializer::IInitializationBehaviour::TPtr TBehaviour::ConstructInitializer() const {
    return std::make_shared<TInitializer>();
}

TString TBehaviour::GetTypeId() const {
    return TObject::GetTypeId();
}

IClassBehaviour::TPtr TBehaviour::GetInstance() {
    static std::shared_ptr<TBehaviour> result = std::make_shared<TBehaviour>();
    return result;
}

}
