#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include "session_updater.h"
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/core/issue/yql_issue.h>

#include <memory>

namespace NKikimr::NWorkloadManager {

struct TWorkloadManagerEvents {
    enum EEvents {
        EvPlaceRequestIntoPool = EventSpaceBegin(TKikimrEvents::ES_WORKLOAD_MANAGER),
        EvContinueRequest,
        EvCleanupRequest,
        EvCleanupResponse,
        EvUpdatePoolInfo,
        EvSubscribeOnPoolChanges,
        EvFetchDatabaseResponse,
        EvWmStateChanged,
        EvWarmupDatabaseInfo,
        EvSubscribeOnWorkloadManagerReady,
        EvWorkloadManagerReady,
        EvEnsurePoolSubscribed,
    };
};

// Local-only notification for ReportWmStateChanges: the requester must send to
// the session owner's KQP proxy on the same node. Cross-node proxy forwarding
// is not supported; forwarded requests do not receive notifications.
// The event cookie is the original query request's cookie. NONE after a queued
// state means admission ended without execution; the query response carries
// the final status and issues.
struct TEvWmStateChanged : public NActors::TEventLocal<TEvWmStateChanged, TWorkloadManagerEvents::EvWmStateChanged> {
    TEvWmStateChanged(ISessionUpdater::EState state, TString poolId, TString classifiedBy)
        : State(state)
        , PoolId(std::move(poolId))
        , ClassifiedBy(std::move(classifiedBy))
    {}

    const ISessionUpdater::EState State;
    const TString PoolId;
    const TString ClassifiedBy;
};


struct TEvSubscribeOnPoolChanges : public NActors::TEventLocal<TEvSubscribeOnPoolChanges, TWorkloadManagerEvents::EvSubscribeOnPoolChanges> {
    TEvSubscribeOnPoolChanges(const TString& databaseId, const TString& poolId)
        : DatabaseId(databaseId)
        , PoolId(poolId)
    {}

    const TString DatabaseId;
    const TString PoolId;
};

struct TEvEnsurePoolSubscribed : public NActors::TEventLocal<TEvEnsurePoolSubscribed, TWorkloadManagerEvents::EvEnsurePoolSubscribed> {
    TEvEnsurePoolSubscribed(const TString& databaseId, const TString& poolId)
        : DatabaseId(databaseId)
        , PoolId(poolId)
    {}

    const TString DatabaseId;
    const TString PoolId;
};

struct TEvPlaceRequestIntoPool : public NActors::TEventLocal<TEvPlaceRequestIntoPool, TWorkloadManagerEvents::EvPlaceRequestIntoPool> {
    TEvPlaceRequestIntoPool(ui64 queryId, const TString& databaseId, const TString& sessionId, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& requestText = "", std::shared_ptr<ISessionUpdater> wmSessionUpdater = nullptr)
        : QueryId(queryId)
        , DatabaseId(databaseId)
        , SessionId(sessionId)
        , PoolId(poolId)
        , UserToken(userToken)
        , RequestText(requestText)
        , WmSessionUpdater(wmSessionUpdater)
    {}

    const ui64 QueryId;
    const TString DatabaseId;
    const TString SessionId;
    TString PoolId;  // Can be changed to default pool id
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString RequestText;
    std::shared_ptr<ISessionUpdater> WmSessionUpdater;
};

struct TEvContinueRequest : public NActors::TEventLocal<TEvContinueRequest, TWorkloadManagerEvents::EvContinueRequest> {
    TEvContinueRequest(ui64 queryId, Ydb::StatusIds::StatusCode status, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NYql::TIssues issues = {})
        : QueryId(queryId)
        , Status(status)
        , PoolId(poolId)
        , PoolConfig(poolConfig)
        , Issues(std::move(issues))
    {}

    bool IsDiskFull() const {
        if (Issues.Empty() || Issues.Size() > 1) {
            return false;
        }

        const auto& issue = *Issues.begin();

        return issue.GetCode() == NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED ||
            issue.GetCode() == NYql::TIssuesIds::KIKIMR_DISK_GROUP_OUT_OF_SPACE;
    }

    enum class EAdmissionResult {
        ContinueInPool,
        ContinueWithoutPool,
        Reject,
    };

    EAdmissionResult GetAdmissionResult() const {
        if (Status == Ydb::StatusIds::UNSUPPORTED) {
            return EAdmissionResult::ContinueWithoutPool;
        }
        // A WM bookkeeping write can fail on disk quota while the user query
        // can still execute (for example, a read). Preserve its pool and issues.
        if (Status == Ydb::StatusIds::SUCCESS || IsDiskFull()) {
            return EAdmissionResult::ContinueInPool;
        }
        return EAdmissionResult::Reject;
    }

    const ui64 QueryId;
    const Ydb::StatusIds::StatusCode Status;
    const TString PoolId;
    const NResourcePool::TPoolSettings PoolConfig;
    const NYql::TIssues Issues;
};

struct TEvCleanupRequest : public NActors::TEventLocal<TEvCleanupRequest, TWorkloadManagerEvents::EvCleanupRequest> {
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

struct TEvCleanupResponse : public NActors::TEventLocal<TEvCleanupResponse, TWorkloadManagerEvents::EvCleanupResponse> {
    explicit TEvCleanupResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status;
    const NYql::TIssues Issues;
};

struct TEvUpdatePoolInfo : public NActors::TEventLocal<TEvUpdatePoolInfo, TWorkloadManagerEvents::EvUpdatePoolInfo> {
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

struct TEvWarmupDatabaseInfo : public NActors::TEventLocal<TEvWarmupDatabaseInfo, TWorkloadManagerEvents::EvWarmupDatabaseInfo> {
    explicit TEvWarmupDatabaseInfo(const TString& databasePath)
        : DatabasePath(databasePath)
    {}

    const TString DatabasePath;
};

struct TEvSubscribeOnWorkloadManagerReady : public NActors::TEventLocal<TEvSubscribeOnWorkloadManagerReady, TWorkloadManagerEvents::EvSubscribeOnWorkloadManagerReady> {
    TEvSubscribeOnWorkloadManagerReady(const TString& databaseId, NActors::TActorId subscriber, ui64 cookie)
        : DatabaseId(databaseId)
        , Subscriber(subscriber)
        , Cookie(cookie)
    {}

    const TString DatabaseId;
    const NActors::TActorId Subscriber;
    const ui64 Cookie;
};

struct TEvWorkloadManagerReady : public NActors::TEventLocal<TEvWorkloadManagerReady, TWorkloadManagerEvents::EvWorkloadManagerReady> {
    TEvWorkloadManagerReady(ui64 cookie, Ydb::StatusIds::StatusCode status, TString message = {})
        : Cookie(cookie)
        , Status(status)
        , Message(std::move(message))
    {}

    const ui64 Cookie;
    const Ydb::StatusIds::StatusCode Status;
    const TString Message;
};

struct TEvFetchDatabaseResponse : public NActors::TEventLocal<TEvFetchDatabaseResponse, TWorkloadManagerEvents::EvFetchDatabaseResponse> {
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

}  // NKikimr::NWorkloadManager
