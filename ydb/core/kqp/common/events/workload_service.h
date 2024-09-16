#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr::NKqp::NWorkload {

struct TEvSubscribeOnPoolChanges : public NActors::TEventLocal<TEvSubscribeOnPoolChanges, TKqpWorkloadServiceEvents::EvSubscribeOnPoolChanges> {
    TEvSubscribeOnPoolChanges(const TString& database, const TString& poolId)
        : Database(database)
        , PoolId(poolId)
    {}

    const TString Database;
    const TString PoolId;
};

struct TEvPlaceRequestIntoPool : public NActors::TEventLocal<TEvPlaceRequestIntoPool, TKqpWorkloadServiceEvents::EvPlaceRequestIntoPool> {
    TEvPlaceRequestIntoPool(const TString& database, const TString& sessionId, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken)
        : Database(database)
        , SessionId(sessionId)
        , PoolId(poolId)
        , UserToken(userToken)
    {}

    const TString Database;
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
    TEvCleanupRequest(const TString& database, const TString& sessionId, const TString& poolId, TDuration duration, TDuration cpuConsumed)
        : Database(database)
        , SessionId(sessionId)
        , PoolId(poolId)
        , Duration(duration)
        , CpuConsumed(cpuConsumed)
    {}

    const TString Database;
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
    TEvUpdatePoolInfo(const TString& database, const TString& poolId, const std::optional<NResourcePool::TPoolSettings>& config, const std::optional<NACLib::TSecurityObject>& securityObject)
        : Database(database)
        , PoolId(poolId)
        , Config(config)
        , SecurityObject(securityObject)
    {}

    const TString Database;
    const TString PoolId;
    const std::optional<NResourcePool::TPoolSettings> Config;
    const std::optional<NACLib::TSecurityObject> SecurityObject;
};

struct TEvUpdateDatabaseInfo : public NActors::TEventLocal<TEvUpdateDatabaseInfo, TKqpWorkloadServiceEvents::EvUpdateDatabaseInfo> {
    TEvUpdateDatabaseInfo(const TString& database, bool serverless)
        : Database(database)
        , Serverless(serverless)
    {}

    const TString Database;
    const bool Serverless;
};

}  // NKikimr::NKqp::NWorkload
