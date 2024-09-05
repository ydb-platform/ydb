#pragma once

#include <ydb/services/metadata/manager/abstract.h>


namespace NKikimr::NKqp {

class TResourcePoolManager : public NMetadata::NModifications::IOperationsManager {
public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;

protected:
    TAsyncStatus DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

public:
    TAsyncStatus ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const override;

private:
    TAsyncStatus CreateResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const;
    TAsyncStatus AlterResourcePool(const NYql::TAlterObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const;
    TAsyncStatus DropResourcePool(const NYql::TDropObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const;

    void PrepareCreateResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const;
    void PrepareAlterResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TAlterObjectSettings& settings, TInternalModificationContext& context) const;
    void PrepareDropResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const;

    TAsyncStatus ChainFeatures(TAsyncStatus lastFeature, std::function<TAsyncStatus()> callback) const;
    TAsyncStatus ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, ui32 nodeId, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const;
};

}  // namespace NKikimr::NKqp
