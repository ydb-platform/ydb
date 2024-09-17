#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::NModifications::TSchemeOperationsManager<TTierConfig> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TTierConfig>&& patchedObjects,
        NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        TInternalModificationContext& context, const NSchemeShard::TSchemeShard& ss) const override;
};
}
