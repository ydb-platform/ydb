#pragma once
#include "snapshot.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretManager: public NModifications::TGenericOperationsManager<TSecret> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TSecret>&& patchedObjects,
        NModifications::IAlterPreparationController<TSecret>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const override;
};

class TAccessManager: public NModifications::TGenericOperationsManager<TAccess> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TAccess>&& patchedObjects,
        NModifications::IAlterPreparationController<TAccess>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        TInternalModificationContext& context) const override;
};

}
