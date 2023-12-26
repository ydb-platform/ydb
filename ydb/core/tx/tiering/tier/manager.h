#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::NModifications::TGenericOperationsManager<TTierConfig> {
protected:
    virtual void DoPrepareObjectsBeforeModification(std::vector<TTierConfig>&& patchedObjects,
        NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
        const TInternalModificationContext& context) const override;

    virtual NMetadata::NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        TInternalModificationContext& context) const override;

    virtual IOperationsManager::TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

    virtual NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;
public:
};

}
