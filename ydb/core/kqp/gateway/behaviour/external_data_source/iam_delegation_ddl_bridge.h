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
    ui64 SnapshotPathId = 0;
    ui64 SnapshotPathVersion = 0;
    TIamDelegation Delegation;
};

struct TCloudIdDescription {
    TExternalDataSourceManager::TYqlConclusionStatus Status =
        TExternalDataSourceManager::TYqlConclusionStatus::Success();
    TString CloudId;
};

struct TIamSchemeRequestResult {
    TExternalDataSourceManager::TYqlConclusionStatus Status =
        TExternalDataSourceManager::TYqlConclusionStatus::Success();
    bool AlreadyExists = false;
};

// Acquire the YDB system service-account token through the configured VM
// metadata endpoint. The future-based SDK adapter is kept behind this actor
// coroutine boundary so lifecycle policy remains a linear co_await sequence.
NActors::async<TIamTokenResult> AcquireSystemIamToken(
    const TIamDelegationSettings& settings,
    const NActors::TActorId& replyTo);

NActors::async<TCloudIdDescription> DescribeDatabaseCloudId(
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TIamSchemeRequestResult> ExecuteIamSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const NActors::TActorId& replyTo);

NActors::async<TExternalDataSourceManager::TYqlConclusionStatus> AwaitLegacyDdl(
    TExternalDataSourceManager::TAsyncStatus legacyDdl,
    const NActors::TActorId& replyTo);

} // namespace NKikimr::NKqp::NExternalDataSource
