#pragma once

#include "manager.h"

namespace NKikimr::NKqp::NExternalDataSource {

bool IsIamDelegationEnabled(NActors::TActorSystem* actorSystem);

TExternalDataSourceManager::TYqlConclusionStatus PrepareIamDelegation(
    NKikimrSchemeOp::TExternalDataSourceDescription& description,
    TStringBuf name);

TExternalDataSourceManager::TAsyncStatus ExecuteIamDelegationDdl(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase);

} // namespace NKikimr::NKqp::NExternalDataSource
