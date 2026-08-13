#pragma once

#include "iam_delegation.h"
#include "manager.h"

#include <ydb/library/actors/async/async.h>

namespace NKikimr::NKqp::NExternalDataSource {

// Compatibility adapters for gateway APIs that still complete through
// NThreading futures. The DDL lifecycle actor consumes these only as native
// actor coroutines, keeping all operation ordering in iam_delegation_ddl.cpp.
struct TIamObjectDescription {
    TExternalDataSourceManager::TYqlConclusionStatus Status =
        TExternalDataSourceManager::TYqlConclusionStatus::Success();
    bool NotFound = false;
    TIamDelegation Delegation;
};

struct TCloudIdDescription {
    TExternalDataSourceManager::TYqlConclusionStatus Status =
        TExternalDataSourceManager::TYqlConclusionStatus::Success();
    TString CloudId;
};

NActors::async<TCloudIdDescription> DescribeDatabaseCloudId(
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TExternalDataSourceManager::TYqlConclusionStatus>
ValidateExternalDatasourceSecrets(
    const NKikimrSchemeOp::TExternalDataSourceDescription& description,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TExternalDataSourceManager::TYqlConclusionStatus> ExecuteIamSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

} // namespace NKikimr::NKqp::NExternalDataSource
