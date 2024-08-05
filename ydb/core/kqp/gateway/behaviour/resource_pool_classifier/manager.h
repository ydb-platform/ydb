#pragma once

#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierManager : public NMetadata::NModifications::TGenericOperationsManager<TResourcePoolClassifierConfig> {
protected:
    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const override;

    virtual void DoPrepareObjectsBeforeModification(std::vector<TResourcePoolClassifierConfig>&& patchedObjects, NMetadata::NModifications::IAlterPreparationController<TResourcePoolClassifierConfig>::TPtr controller, const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;
};

}  // namespace NKikimr::NKqp
