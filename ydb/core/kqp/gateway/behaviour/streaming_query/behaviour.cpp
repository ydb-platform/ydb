#include "behaviour.h"
#include "initializer.h"
#include "manager.h"

#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

TStreamingQueryBehaviour::TFactory::TRegistrator<TStreamingQueryBehaviour> TStreamingQueryBehaviour::Registrator(TStreamingQueryConfig::GetTypeId());

NMetadata::NInitializer::IInitializationBehaviour::TPtr TStreamingQueryBehaviour::ConstructInitializer() const {
    return std::make_shared<TStreamingQueryInitializer>();
}

TString TStreamingQueryBehaviour::GetInternalStorageTablePath() const {
    return "streaming/queries";
}

NMetadata::NModifications::IOperationsManager::TPtr TStreamingQueryBehaviour::ConstructOperationsManager() const {
    return std::make_shared<TStreamingQueryManager>();
}

TString TStreamingQueryBehaviour::GetTypeId() const {
    return TStreamingQueryConfig::GetTypeId();
}

NMetadata::IClassBehaviour::TPtr TStreamingQueryBehaviour::GetInstance() {
    static std::shared_ptr<TStreamingQueryBehaviour> result = std::make_shared<TStreamingQueryBehaviour>();
    return result;
}

}  // namespace NKikimr::NKqp
