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
        const TInternalModificationContext& context) const override;

    virtual NModifications::TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& /*settings*/,
        TInternalModificationContext& context) const override;

    virtual IOperationsManager::TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

    virtual NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;

public:
};

}
