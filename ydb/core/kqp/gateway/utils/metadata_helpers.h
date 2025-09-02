#pragma once

#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> ChainFeatures(
    NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> lastFeature,
    std::function<NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus>()> callback);

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> SendSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}  // namespace NKikimr::NKqp
