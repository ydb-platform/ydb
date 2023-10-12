#include "behaviour.h"
#include <ydb/core/kqp/gateway/behaviour/tablestore/manager.h>

namespace NKikimr::NKqp {

TOlapTableBehaviour::TFactory::TRegistrator<TOlapTableBehaviour> TOlapTableBehaviour::Registrator(TOlapTableConfig::GetTypeId());

NMetadata::NModifications::IOperationsManager::TPtr TOlapTableBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TTableStoreManager>(true);
}

NMetadata::NInitializer::IInitializationBehaviour::TPtr TOlapTableBehaviour::ConstructInitializer() const {
    return nullptr;
}

TString TOlapTableBehaviour::GetInternalStorageTablePath() const {
    return TOlapTableConfig::GetTypeId();
}


TString TOlapTableBehaviour::GetTypeId() const {
    return TOlapTableConfig::GetTypeId();
}

}
