#include "init_manager.h"

namespace NKikimr::NMetadata {

NThreading::TFuture<TObjectOperatorResult> TInitManagerBase::BuildProcessingError() const
{
    TObjectOperatorResult result("manager only for initialization. other operation aren't correct.");
    return NThreading::MakeFuture<TObjectOperatorResult>(result);
}

NThreading::TFuture<TObjectOperatorResult> TInitManagerBase::DoCreateObject(
    const NYql::TCreateObjectSettings& /*settings*/, const ui32 /*nodeId*/,
    NMetadata::IOperationsManager::TPtr /*manager*/, const TModificationContext& /*context*/) const
{
    return BuildProcessingError();
}

NThreading::TFuture<TObjectOperatorResult> TInitManagerBase::DoAlterObject(
    const NYql::TAlterObjectSettings& /*settings*/, const ui32 /*nodeId*/,
    NMetadata::IOperationsManager::TPtr /*manager*/, const TModificationContext& /*context*/) const
{
    return BuildProcessingError();
}

NThreading::TFuture<TObjectOperatorResult> TInitManagerBase::DoDropObject(
    const NYql::TDropObjectSettings& /*settings*/, const ui32 /*nodeId*/,
    NMetadata::IOperationsManager::TPtr /*manager*/, const TModificationContext& /*context*/) const
{
    return BuildProcessingError();
}

}
