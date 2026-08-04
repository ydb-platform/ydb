#pragma once

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TKeyValueVolumeManager: public NMetadata::NModifications::IOperationsManager {
public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;

private:
    TAsyncStatus DoModify(const NYql::TObjectSettingsImpl& settings,
                          const ui32 nodeId,
                          const NMetadata::IClassBehaviour::TPtr& manager,
                          TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation,
                                   const NYql::TObjectSettingsImpl& settings,
                                   const NMetadata::IClassBehaviour::TPtr& manager,
                                   TInternalModificationContext& context) const override;

    TAsyncStatus ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
                                 const ui32 nodeId,
                                 const NMetadata::IClassBehaviour::TPtr& manager,
                                 const TExternalModificationContext& context) const override;
};

}   // namespace NKikimr::NKqp
