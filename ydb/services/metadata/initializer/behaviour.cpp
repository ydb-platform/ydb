#include "behaviour.h"
#include "object.h"
#include "initializer.h"
#include "manager.h"
#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NMetadata::NInitializer {

TString TDBObjectBehaviour::GetInternalStorageTablePath() const {
    return "initialization/migrations";
}

TString TDBObjectBehaviour::GetTypeId() const {
    return TDBInitialization::GetTypeId();
}

IInitializationBehaviour::TPtr TDBObjectBehaviour::ConstructInitializer() const {
    return std::make_shared<TInitializer>();
}

std::shared_ptr<NKikimr::NMetadata::NModifications::IOperationsManager> TDBObjectBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TManager>();
}

}
