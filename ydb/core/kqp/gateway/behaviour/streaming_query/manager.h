#pragma once

#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

class TStreamingQueryManager : public NMetadata::NModifications::IOperationsManager {
public:
    using IOperationsManager::TYqlConclusionStatus;
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
    [[nodiscard]] static TYqlConclusionStatus PrepareCreateStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context);
    [[nodiscard]] static TYqlConclusionStatus PrepareAlterStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context);
    [[nodiscard]] static TYqlConclusionStatus PrepareDropStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context);
};

}  // namespace NKikimr::NKqp
