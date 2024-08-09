#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NInitializer {

class TManager: public NModifications::TGenericOperationsManager<TDBInitialization> {
private:
    using TBase = NModifications::TGenericOperationsManager<TDBInitialization>;
    using TInternalModificationContext = TBase::TInternalModificationContext;
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
        NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& /*settings*/,
        TInternalModificationContext& context) const override;

public:
};

}
