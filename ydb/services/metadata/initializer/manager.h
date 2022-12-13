#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NInitializer {

class TManager: public NModifications::TGenericOperationsManager<TDBInitialization> {
private:
    using TBase = NModifications::TGenericOperationsManager<TDBInitialization>;
    using TModificationContext = TBase::TModificationContext;
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TDBInitialization>&& objects,
        NModifications::IAlterPreparationController<TDBInitialization>::TPtr controller,
        const TModificationContext& /*context*/) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& /*settings*/,
        const TModificationContext& /*context*/) const override;

public:
};

}
