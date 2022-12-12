#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::NModifications::TGenericOperationsManager<TTierConfig> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TTierConfig>&& patchedObjects,
        NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TModificationContext& context) const override;

    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const NMetadata::NModifications::IOperationsManager::TModificationContext& /*context*/) const override;

    virtual NMetadata::NModifications::TTableSchema ConstructActualSchema() const override;
public:
};

}
