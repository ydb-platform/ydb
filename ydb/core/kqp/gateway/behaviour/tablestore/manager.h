#pragma once

#include "behaviour.h"

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TTableStoreManager: public NMetadata::NModifications::IOperationsManager {
    using TBase = NMetadata::NModifications::IOperationsManager;
    bool IsStandalone = false;
protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    IOperationsManager::TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

    NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;
public:
    TTableStoreManager(bool isStandalone)
        : IsStandalone(isStandalone)
    {}
};

}
