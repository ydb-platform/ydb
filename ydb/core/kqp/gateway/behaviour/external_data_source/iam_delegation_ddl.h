#pragma once

#include "manager.h"

#include <functional>

namespace NKikimr::NKqp::NExternalDataSource {

enum class EIamDelegationDdlRoute {
    Legacy,
    IamOperation,
    LegacyWithIamCleanup,
};

using TLegacyDdlExecutor = std::function<TExternalDataSourceManager::TAsyncStatus(
    const NKikimrSchemeOp::TModifyScheme&,
    const TExternalDataSourceManager::TExternalModificationContext&)>;

bool IsIamDelegationEnabled(NActors::TActorSystem* actorSystem);

EIamDelegationDdlRoute SelectIamDelegationDdlRoute(
    bool delegationEnabled,
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase);

TExternalDataSourceManager::TYqlConclusionStatus PrepareIamDelegation(
    NKikimrSchemeOp::TExternalDataSourceDescription& description,
    TStringBuf name);

TExternalDataSourceManager::TAsyncStatus ExecuteIamDelegationDdl(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase);

TExternalDataSourceManager::TAsyncStatus ExecuteLegacyDdlWithIamCleanup(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
    TLegacyDdlExecutor executeLegacyDdl);

} // namespace NKikimr::NKqp::NExternalDataSource
