#pragma once
#include "snapshot.h"
#include "udf_meta.h"
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/manager/generic_manager.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NUdfStore {

class TUdfManager: public NModifications::TGenericOperationsManager<TUdfMeta> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TUdfMeta>&& patchedObjects,
        NModifications::IAlterPreparationController<TUdfMeta>::TPtr controller,
        const TInternalModificationContext& context, const NMetadata::NModifications::TAlterOperationContext& alterContext) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context) const override;
};

} // namespace NKikimr::NUdfStore
