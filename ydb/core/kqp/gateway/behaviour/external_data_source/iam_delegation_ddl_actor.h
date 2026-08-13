#pragma once

#include "iam_delegation_ddl.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NKqp::NExternalDataSource {

NActors::IActor* CreateIamDelegationDdlActor(
    NKikimrSchemeOp::TModifyScheme schemeTx,
    TExternalDataSourceManager::TExternalModificationContext context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
    NThreading::TPromise<TExternalDataSourceManager::TYqlConclusionStatus> promise);

NActors::IActor* CreateLegacyDdlWithIamCleanupActor(
    NKikimrSchemeOp::TModifyScheme schemeTx,
    TExternalDataSourceManager::TExternalModificationContext context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
    TLegacyDdlExecutor executeLegacyDdl,
    NThreading::TPromise<TExternalDataSourceManager::TYqlConclusionStatus> promise);

} // namespace NKikimr::NKqp::NExternalDataSource
