#pragma once
#include "object.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NCSIndex {

class TManager: public NModifications::TGenericOperationsManager<TObject> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        NModifications::IAlterPreparationController<TObject>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const override;

public:
};

}
