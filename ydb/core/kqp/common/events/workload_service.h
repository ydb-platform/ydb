#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr::NKqp::NWorkload {

struct TEvSubscribeOnPoolChanges : public NActors::TEventLocal<TEvSubscribeOnPoolChanges, TKqpWorkloadServiceEvents::EvSubscribeOnPoolChanges> {
    TEvSubscribeOnPoolChanges(const TString& databaseId, const TString& poolId)
        : DatabaseId(databaseId)
        , PoolId(poolId)
    {}

    const TString DatabaseId;
    const TString PoolId;
};

struct TEvPlaceRequestIntoPool : public NActors::TEventLocal<TEvPlaceRequestIntoPool, TKqpWorkloadServiceEvents::EvPlaceRequestIntoPool> {
    TEvPlaceRequestIntoPool(const TString& databaseId, const TString& sessionId, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : DatabaseId(databaseId)
        , SessionId(sessionId)
        , PoolId(poolId)
        , UserToken(userToken)
    {}

    const TString DatabaseId;
    const TString SessionId;
    TString PoolId;  // Can be changed to default pool id
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

struct TEvContinueRequest : public NActors::TEventLocal<TEvContinueRequest, TKqpWorkloadServiceEvents::EvContinueRequest> {
    TEvContinueRequest(Ydb::StatusIds::StatusCode status, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NYql::TIssues issues = {})
        : Status(status)
        , PoolId(poolId)
        , PoolConfig(poolConfig)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status;
    const TString PoolId;
    const NResourcePool::TPoolSettings PoolConfig;
    const NYql::TIssues Issues;
};

struct TEvCleanupRequest : public NActors::TEventLocal<TEvCleanupRequest, TKqpWorkloadServiceEvents::EvCleanupRequest> {
    TEvCleanupRequest(const TString& databaseId, const TString& sessionId, const TString& poolId, TDuration duration, TDuration cpuConsumed)
        : DatabaseId(databaseId)
        , SessionId(sessionId)
        , PoolId(poolId)
        , Duration(duration)
        , CpuConsumed(cpuConsumed)
    {}

    const TString DatabaseId;
    const TString SessionId;
    const TString PoolId;
    const TDuration Duration;
    const TDuration CpuConsumed;
};

struct TEvCleanupResponse : public NActors::TEventLocal<TEvCleanupResponse, TKqpWorkloadServiceEvents::EvCleanupResponse> {
    explicit TEvCleanupResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status;
    const NYql::TIssues Issues;
};

struct TEvUpdatePoolInfo : public NActors::TEventLocal<TEvUpdatePoolInfo, TKqpWorkloadServiceEvents::EvUpdatePoolInfo> {
    TEvUpdatePoolInfo(const TString& databaseId, const TString& poolId, const std::optional<NResourcePool::TPoolSettings>& config, const std::optional<NACLib::TSecurityObject>& securityObject)
        : DatabaseId(databaseId)
        , PoolId(poolId)
        , Config(config)
        , SecurityObject(securityObject)
    {}

    const TString DatabaseId;
    const TString PoolId;
    const std::optional<NResourcePool::TPoolSettings> Config;
    const std::optional<NACLib::TSecurityObject> SecurityObject;
};

struct TEvFetchDatabaseResponse : public NActors::TEventLocal<TEvFetchDatabaseResponse, TKqpWorkloadServiceEvents::EvFetchDatabaseResponse> {
    TEvFetchDatabaseResponse(Ydb::StatusIds::StatusCode status, const TString& database, const TString& databaseId, bool serverless, TPathId pathId, NYql::TIssues issues)
        : Status(status)
        , Database(database)
        , DatabaseId(databaseId)
        , Serverless(serverless)
        , PathId(pathId)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status;
    const TString Database;
    const TString DatabaseId;
    const bool Serverless;
    const TPathId PathId;
    const NYql::TIssues Issues;
};

}  // NKikimr::NKqp::NWorkload
