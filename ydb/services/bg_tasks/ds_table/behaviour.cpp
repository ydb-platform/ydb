#include "behaviour.h"
#include "initialization.h"

namespace NKikimr::NBackgroundTasks {

std::shared_ptr<NMetadata::NInitializer::IInitializationBehaviour> TBehaviour::ConstructInitializer() const {
    return std::make_shared<TBGTasksInitializer>(Config);
}

std::shared_ptr<NMetadata::NModifications::IOperationsManager> TBehaviour::GetOperationsManager() const {
    return nullptr;
}

}
