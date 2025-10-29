#pragma once

#include "object.h"

namespace NKikimr::NKqp {

class IStreamingQueryOperationController : public NMetadata::NModifications::IAlterController {
public:
    using TPtr = std::shared_ptr<IStreamingQueryOperationController>;

    virtual void OnAlteringFinishedWithStatus(const TStreamingQueryConfig::TStatus& status) = 0;
};

void DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

void DoAlterStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

void DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}  // namespace NKikimr::NKqp
