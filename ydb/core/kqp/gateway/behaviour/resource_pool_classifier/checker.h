#pragma once

#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>


namespace NKikimr::NKqp {

NActors::IActor* CreateResourcePoolClassifierPreparationActor(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext);

}  // namespace NKikimr::NKqp
