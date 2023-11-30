#pragma once
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/retry/retry_policy.h>

namespace NFq {

using TYdbSdkRetryPolicy = IRetryPolicy<const NYdb::TStatus&>;

// Actor that creates table.
// Sends TEvSchemaCreated to parent (if any).
NActors::IActor* MakeCreateTableActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& tablePath,
    const NYdb::NTable::TTableDescription& tableDesc,
    TYdbSdkRetryPolicy::TPtr,
    ui64 cookie = 0);

// Actor that creates directory.
// Sends TEvSchemaCreated to parent (if any).
NActors::IActor* MakeCreateDirectoryActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& directoryPath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie = 0);

// Actor that creates coordination node.
// Sends TEvSchemaCreated to parent (if any).
NActors::IActor* MakeCreateCoordinationNodeActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie = 0);

// Actor that creates rate limiter resource.
// Sends TEvSchemaCreated to parent (if any).
NActors::IActor* MakeCreateRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    const std::vector<TMaybe<double>>& limits, // limits from the very parent resource to the leaf
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie = 0);

// Actor that deletes rate limiter resource.
// Sends TEvSchemaDeleted to parent (if any).
NActors::IActor* MakeDeleteRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie = 0);

// Actor that updates rate limiter resource.
// Sends TEvSchemaUpdated to parent (if any).
NActors::IActor* MakeUpdateRateLimiterResourceActor(
    NActors::TActorId parent,
    ui64 logComponent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TMaybe<double> limit,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie = 0);

bool IsPathDoesNotExistError(const NYdb::TStatus& status);

} // namespace NFq
