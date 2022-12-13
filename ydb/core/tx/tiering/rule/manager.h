#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesManager: public NMetadata::NModifications::TGenericOperationsManager<TTieringRule> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TTieringRule>&& objects,
        NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TModificationContext& context) const override;

    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const NMetadata::NModifications::IOperationsManager::TModificationContext& /*context*/) const override;
};

}
