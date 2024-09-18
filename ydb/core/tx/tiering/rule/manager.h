#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulesManager: public NMetadata::NModifications::TSchemeOperationsManager<TTieringRule> {
private:
    static TConclusionStatus ValidateOperation(const NYql::TObjectSettingsImpl& settings, IOperationsManager::TInternalModificationContext& context,
        NSchemeShard::TSchemeShard& ss);

protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TTieringRule>&& objects,
        NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        IOperationsManager::TInternalModificationContext& context, NSchemeShard::TSchemeShard& ss) const override;
};
}
