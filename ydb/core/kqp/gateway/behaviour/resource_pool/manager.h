#pragma once

#include <ydb/services/metadata/manager/abstract.h>


namespace NKikimr::NKqp {

class TResourcePoolManager : public NMetadata::NModifications::IOperationsManager {
public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;

protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

public:

    NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;
};

}  // namespace NKikimr::NKqp
