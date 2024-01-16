#pragma once

#include <ydb/services/metadata/manager/generic_manager.h>

namespace NKikimr::NKqp {

class TViewManager: public NMetadata::NModifications::IOperationsManager {

    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings,
                                                       const ui32 nodeId,
                                                       const NMetadata::IClassBehaviour::TPtr& manager,
                                                       TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation,
                                   const NYql::TObjectSettingsImpl& settings,
                                   const NMetadata::IClassBehaviour::TPtr& manager,
                                   TInternalModificationContext& context) const override;

    NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
                                                              const ui32 nodeId,
                                                              const NMetadata::IClassBehaviour::TPtr& manager,
                                                              const TExternalModificationContext& context) const override;

public:
    using NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
};

}
