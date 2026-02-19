#pragma once

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TExternalDataSourceManager: public NMetadata::NModifications::IOperationsManager {
public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;

protected:
    TAsyncStatus DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, IOperationsManager::TInternalModificationContext& context) const override;

public:
    NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const override;

private:
    TAsyncStatus CreateExternalDataSource(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const;
    TAsyncStatus DropExternalDataSource(const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const;

    [[nodiscard]] TYqlConclusionStatus PrepareCreateExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const;
    [[nodiscard]] TYqlConclusionStatus PrepareDropExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const;

    TAsyncStatus ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const;
};

}  // namespace NKikimr::NKqp
