#pragma once

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TExternalDataSourceManager: public NMetadata::NModifications::IOperationsManager {
    NThreading::TFuture<TYqlConclusionStatus> CreateExternalDataSource(const NYql::TCreateObjectSettings& settings,
                                                                       TInternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> DropExternalDataSource(const NYql::TDropObjectSettings& settings,
                                                                     TInternalModificationContext& context) const;

    void PrepareCreateExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings,
                                         TInternalModificationContext& context) const;

    void PrepareDropExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings,
                                       TInternalModificationContext& context) const;

protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings,
                                                       const ui32 nodeId,
                                                       const NMetadata::IClassBehaviour::TPtr& manager,
                                                       TInternalModificationContext& context) const override;

    IOperationsManager::TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

    NThreading::TFuture<IOperationsManager::TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;

public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
};

}
