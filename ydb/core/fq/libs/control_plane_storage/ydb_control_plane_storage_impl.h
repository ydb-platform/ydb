#pragma once

#include "config.h"
#include "control_plane_storage.h"
#include "control_plane_storage_counters.h"
#include "extractors.h"
#include "probes.h"
#include "request_validators.h"
#include "util.h"
#include "validators.h"
#include <ydb/core/fq/libs/control_plane_storage/internal/response_tasks.h>

#include <util/generic/guid.h>
#include <util/system/yassert.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/db_pool/db_pool.h>
#include <ydb/library/security/util.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/fq/libs/common/cache.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/exceptions/exceptions.h>
#include <ydb/core/fq/libs/metrics/status_code_counters.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

using namespace NActors;
using namespace NConfig;
using namespace NDbPool;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

inline static void PrepareAccessConditionImpl(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user, TPermissions::TPermission privatePermission, TPermissions::TPermission publicPermission) {
    if (permissions.Check(publicPermission) && permissions.Check(privatePermission)) {
        // any row
    } else if (permissions.Check(publicPermission)) {
        builder.AddString("user", user);
        builder.AddInt64("visibility_scope", FederatedQuery::Acl::SCOPE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_scope OR `" USER_COLUMN_NAME "` = $user)");
    } else if (permissions.Check(privatePermission)) {
        builder.AddInt64("visibility_private", FederatedQuery::Acl::PRIVATE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_private)");
    } else {
        builder.AddString("user", user);
        builder.AddInt64("visibility_private", FederatedQuery::Acl::PRIVATE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_private AND `" USER_COLUMN_NAME "` = $user)");
    }
}

inline static void PrepareViewAccessCondition(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user) {
    PrepareAccessConditionImpl(builder, permissions, user, TPermissions::VIEW_PRIVATE, TPermissions::VIEW_PUBLIC);
}

inline static void PrepareManageAccessCondition(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user) {
    PrepareAccessConditionImpl(builder, permissions, user, TPermissions::MANAGE_PRIVATE, TPermissions::MANAGE_PUBLIC);
}

inline static bool HasAccessImpl(TPermissions permissions, FederatedQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user, TPermissions::TPermission privatePermission, TPermissions::TPermission publicPermission) {
    return (permissions.Check(publicPermission) && permissions.Check(privatePermission))
        || (permissions.Check(publicPermission) && (entityVisibility == FederatedQuery::Acl::SCOPE || entityUser == user))
        || (permissions.Check(privatePermission) && entityVisibility == FederatedQuery::Acl::PRIVATE)
        || (entityVisibility == FederatedQuery::Acl::PRIVATE && entityUser == user);
}


inline static bool HasViewAccess(TPermissions permissions, FederatedQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user) {
    return HasAccessImpl(permissions, entityVisibility, entityUser, user, TPermissions::VIEW_PRIVATE, TPermissions::VIEW_PUBLIC);
}

inline static bool HasManageAccess(TPermissions permissions, FederatedQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user) {
    return HasAccessImpl(permissions, entityVisibility, entityUser, user, TPermissions::MANAGE_PRIVATE, TPermissions::MANAGE_PUBLIC);
}

LWTRACE_USING(YQ_CONTROL_PLANE_STORAGE_PROVIDER);

using TRequestScopeCountersPtr = TIntrusivePtr<TRequestScopeCounters>;
using TRequestCommonCountersPtr = TIntrusivePtr<TRequestCommonCounters>;

struct TRequestCounters {
    TRequestScopeCountersPtr Scope;
    TRequestCommonCountersPtr Common;

    void IncInFly() {
        if (Scope) {
            Scope->InFly->Inc();
        }
        if (Common) {
            Common->InFly->Inc();
        }
    }

    void DecInFly() {
        if (Scope) {
            Scope->InFly->Dec();
        }
        if (Common) {
            Common->InFly->Dec();
        }
    }

    void IncOk() {
        if (Scope) {
            Scope->Ok->Inc();
        }
        if (Common) {
            Common->Ok->Inc();
        }
    }

    void DecOk() {
        if (Scope) {
            Scope->Ok->Dec();
        }
        if (Common) {
            Common->Ok->Dec();
        }
    }

    void IncError() {
        if (Scope) {
            Scope->Error->Inc();
        }
        if (Common) {
            Common->Error->Inc();
        }
    }

    void DecError() {
        if (Scope) {
            Scope->Error->Dec();
        }
        if (Common) {
            Common->Error->Dec();
        }
    }

    void IncRetry() {
        if (Scope) {
            Scope->Retry->Inc();
        }
        if (Common) {
            Common->Retry->Inc();
        }
    }

    void DecRetry() {
        if (Scope) {
            Scope->Retry->Dec();
        }
        if (Common) {
            Common->Retry->Dec();
        }
    }
};

template<typename T>
THashMap<TString, T> GetEntitiesWithVisibilityPriority(const TResultSet& resultSet, const TString& columnName, bool ignorePrivateSources, const TRequestCommonCountersPtr& commonCounters)
{
    THashMap<TString, T> entities;
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        T entity;
        if (!entity.ParseFromString(*parser.ColumnParser(columnName).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for GetEntitiesWithVisibilityPriority. Please contact internal support";
        }
        const auto visibility = entity.content().acl().visibility();
        if (ignorePrivateSources && visibility == FederatedQuery::Acl::PRIVATE) {
            continue;
        }
        const TString name = entity.content().name();
        if (auto it = entities.find(name); it != entities.end()) {
            if (visibility == FederatedQuery::Acl::PRIVATE) {
                it->second = std::move(entity);
            }
        } else {
            entities[name] = std::move(entity);
        }
    }

    return entities;
}

template<typename T>
TVector<T> GetEntities(const TResultSet& resultSet, const TString& columnName, bool ignorePrivateSources, const TRequestCommonCountersPtr& commonCounters)
{
    TVector<T> entities;
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        T entity;
        if (!entity.ParseFromString(*parser.ColumnParser(columnName).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for GetEntities. Please contact internal support";
        }
        const auto visibility = entity.content().acl().visibility();
        if (ignorePrivateSources && visibility == FederatedQuery::Acl::PRIVATE) {
            continue;
        }
        entities.emplace_back(std::move(entity));
    }
    return entities;
}

void InsertIdempotencyKey(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey, const TString& response, const TInstant& expireAt);

void ReadIdempotencyKeyQuery(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey);

template <bool auditable, class ResponseEvent, class Result>
struct TPrepareResponseResultTypeImpl;

template <class ResponseEvent, class Result>
struct TPrepareResponseResultTypeImpl<false, ResponseEvent, Result> {
    using Type = Result;
    using TResponseAuditDetails = void*;
};

template <class ResponseEvent, class Result>
struct TPrepareResponseResultTypeImpl<true, ResponseEvent, Result> {
    using Type = std::pair<Result, TAuditDetails<typename ResponseEvent::TAuditMessage>>;
    using TResponseAuditDetails = TAuditDetails<typename ResponseEvent::TAuditMessage>;
};

template <class ResponseEvent, class Result>
struct TPrepareResponseResultType : TPrepareResponseResultTypeImpl<ResponseEvent::Auditable, ResponseEvent, Result> {};

class TDbRequester {
protected:
    explicit TDbRequester(TDbPool::TPtr pool = nullptr, TYdbConnectionPtr ydbConnection = nullptr)
        : DbPool(std::move(pool))
        , YdbConnection(std::move(ydbConnection))
    {
    }

    std::pair<TAsyncStatus, std::shared_ptr<TVector<NYdb::TResultSet>>> Read(
        const TString& query,
        const NYdb::TParams& params,
        const TRequestCounters& requestCounters,
        TDebugInfoPtr debugInfo,
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryOnTli = true);

    TAsyncStatus Validate(
        NActors::TActorSystem* actorSystem,
        std::shared_ptr<TMaybe<TTransaction>> transaction,
        size_t item, const TVector<TValidationQuery>& validators,
        TSession session,
        std::shared_ptr<bool> successFinish,
        TDebugInfoPtr debugInfo,
        TTxSettings transactionMode = TTxSettings::SerializableRW());

    TAsyncStatus Write(
        const TString& query,
        const NYdb::TParams& params,
        const TRequestCounters& requestCounters,
        TDebugInfoPtr debugInfo,
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryTli = true);

    TAsyncStatus ReadModifyWrite(
        const TString& readQuery,
        const NYdb::TParams& readParams,
        const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>& prepare,
        const TRequestCounters& requestCounters,
        TDebugInfoPtr debugInfo = {},
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryOnTli = true);

protected:
    TDbPool::TPtr DbPool;
    TYdbConnectionPtr YdbConnection;
};

class TControlPlaneStorageUtils {
protected:
    TControlPlaneStorageUtils(
        const NConfig::TControlPlaneStorageConfig& config,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& common,
        const NConfig::TComputeConfig& computeConfig)
    : Config(std::make_shared<::NFq::TControlPlaneStorageConfig>(config, s3Config, common, computeConfig))
    {
    }

    explicit TControlPlaneStorageUtils(const std::shared_ptr<::NFq::TControlPlaneStorageConfig>& config)
    : Config(config)
    {
    }

    /*
    * Utility
    */
    bool IsSuperUser(const TString& user);

    template<typename T>
    NYql::TIssues ValidateConnection(T& ev, bool passwordRequired = true)
    {
        return ::NFq::ValidateConnection<T>(ev, Config->Proto.GetMaxRequestSize(),
                                  Config->AvailableConnections, Config->Proto.GetDisableCurrentIam(),
                                  passwordRequired);
    }

    template<typename T>
     NYql::TIssues ValidateBinding(T& ev)
    {
        return ::NFq::ValidateBinding<T>(ev, Config->Proto.GetMaxRequestSize(), Config->AvailableBindings, Config->GeneratorPathsLimit);
    }

    template<typename T>
    NYql::TIssues ValidateQuery(const T& ev)
    {
        return ::NFq::ValidateQuery<T>(ev, Config->Proto.GetMaxRequestSize());
    }

    template<class P>
    NYql::TIssues ValidateEvent(const P& ev)
    {
        return ::NFq::ValidateEvent<P>(ev, Config->Proto.GetMaxRequestSize());
    }

    static TString MakeLogPrefix(const TString& scope, const TString& user, const TString& id = "") {
        return "[" + scope + ", " + user + (id ? ", " + id : "") + "] ";
    }

    static TString MakeUserInfo(const TString& user, const TString& token = "") {
        return "[" + user + (token ? ", " + NKikimr::MaskTicket(token) : "") + "] ";
    }

    static TString TrimForLogs(const TString& s, ui64 maxLength = 4096) {
        return s.size() > maxLength ? (s.substr(0, maxLength - 3) + "...") : s;
    }

    static FederatedQuery::CommonMeta CreateCommonMeta(const TString& id, const TString& user, const TInstant& startTime, int64_t revision) {
        FederatedQuery::CommonMeta common;
        common.set_id(id);
        common.set_created_by(user);
        common.set_modified_by(user);
        auto now = NProtoInterop::CastToProto(startTime);
        *common.mutable_created_at() = now;
        *common.mutable_modified_at() = now;
        common.set_revision(revision);
        return common;
    }

protected:
    std::shared_ptr<::NFq::TControlPlaneStorageConfig> Config;

    static constexpr int64_t InitialRevision = 1;
};

class TYdbControlPlaneStorageActor : public NActors::TActorBootstrapped<TYdbControlPlaneStorageActor>,
                                     public TDbRequester,
                                     public TControlPlaneStorageUtils
{
    enum ERequestTypeScope {
        RTS_CREATE_QUERY,
        RTS_LIST_QUERIES,
        RTS_DESCRIBE_QUERY,
        RTS_GET_QUERY_STATUS,
        RTS_MODIFY_QUERY,
        RTS_DELETE_QUERY,
        RTS_CONTROL_QUERY,
        RTS_GET_RESULT_DATA,
        RTS_LIST_JOBS_DATA,
        RTS_DESCRIBE_JOB,
        RTS_CREATE_CONNECTION,
        RTS_LIST_CONNECTIONS,
        RTS_DESCRIBE_CONNECTION,
        RTS_MODIFY_CONNECTION,
        RTS_DELETE_CONNECTION,
        RTS_CREATE_BINDING,
        RTS_LIST_BINDINGS,
        RTS_DESCRIBE_BINDING,
        RTS_MODIFY_BINDING,
        RTS_DELETE_BINDING,
        RTS_PING_TASK,
        RTS_CREATE_DATABASE,
        RTS_DESCRIBE_DATABASE,
        RTS_MODIFY_DATABASE,
        RTS_MAX,
    };

    static constexpr std::string_view RequestTypeScopeNames[ERequestTypeScope::RTS_MAX] = {
        "CreateQuery",
        "ListQueries",
        "DescribeQuery",
        "GetQueryStatus",
        "ModifyQuery",
        "DeleteQuery",
        "ControlQuery",
        "GetResultData",
        "ListJobs",
        "DescribeJob",
        "CreateConnection",
        "ListConnections",
        "DescribeConnection",
        "ModifyConnection",
        "DeleteConnection",
        "CreateBinding",
        "ListBindings",
        "DescribeBinding",
        "ModifyBinding",
        "DeleteBinding",
        "PingTask",
        "CreateDatabase",
        "DescribeDatabase",
        "ModifyDatabase"
    };

    enum ERequestTypeCommon {
        RTC_WRITE_RESULT_DATA,
        RTC_GET_TASK,
        RTC_NODES_HEALTH_CHECK,
        RTS_QUOTA_USAGE,
        RTC_CREATE_RATE_LIMITER_RESOURCE,
        RTC_DELETE_RATE_LIMITER_RESOURCE,
        RTC_CREATE_QUERY,
        RTC_LIST_QUERIES,
        RTC_DESCRIBE_QUERY,
        RTC_GET_QUERY_STATUS,
        RTC_MODIFY_QUERY,
        RTC_DELETE_QUERY,
        RTC_CONTROL_QUERY,
        RTC_GET_RESULT_DATA,
        RTC_LIST_JOBS_DATA,
        RTC_DESCRIBE_JOB,
        RTC_CREATE_CONNECTION,
        RTC_LIST_CONNECTIONS,
        RTC_DESCRIBE_CONNECTION,
        RTC_MODIFY_CONNECTION,
        RTC_DELETE_CONNECTION,
        RTC_CREATE_BINDING,
        RTC_LIST_BINDINGS,
        RTC_DESCRIBE_BINDING,
        RTC_MODIFY_BINDING,
        RTC_DELETE_BINDING,
        RTC_PING_TASK,
        RTC_CREATE_DATABASE,
        RTC_DESCRIBE_DATABASE,
        RTC_MODIFY_DATABASE,
        RTC_MAX,
    };

    class TCounters: public virtual TThrRefBase {
        struct TMetricsScope {
            TString CloudId;
            TString Scope;

            TMetricsScope() = default;

            TMetricsScope(const TString& cloudId, const TString& scope)
                : CloudId(cloudId), Scope(scope)
            {}

            bool operator<(const TMetricsScope& right) const {
                return std::make_pair(CloudId, Scope) < std::make_pair(right.CloudId, right.Scope);
            }
        };

        using TScopeCounters = std::array<TRequestScopeCountersPtr, RTS_MAX>;
        using TScopeCountersPtr = std::shared_ptr<TScopeCounters>;
        using TFinalStatusCountersPtr = TIntrusivePtr<TFinalStatusCounters>;

        std::array<TRequestCommonCountersPtr, RTC_MAX> CommonRequests = CreateArray<RTC_MAX, TRequestCommonCountersPtr>({
            { MakeIntrusive<TRequestCommonCounters>("WriteResultData") },
            { MakeIntrusive<TRequestCommonCounters>("GetTask") },
            { MakeIntrusive<TRequestCommonCounters>("NodesHealthCheck") },
            { MakeIntrusive<TRequestCommonCounters>("GetQuotaUsage") },
            { MakeIntrusive<TRequestCommonCounters>("CreateRateLimiterResource") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteRateLimiterResource") },
            { MakeIntrusive<TRequestCommonCounters>("CreateQuery") },
            { MakeIntrusive<TRequestCommonCounters>("ListQueries") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeQuery") },
            { MakeIntrusive<TRequestCommonCounters>("GetQueryStatus") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyQuery") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteQuery") },
            { MakeIntrusive<TRequestCommonCounters>("ControlQuery") },
            { MakeIntrusive<TRequestCommonCounters>("GetResultData") },
            { MakeIntrusive<TRequestCommonCounters>("ListJobs") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeJob") },
            { MakeIntrusive<TRequestCommonCounters>("CreateConnection") },
            { MakeIntrusive<TRequestCommonCounters>("ListConnections") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeConnection") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyConnection") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteConnection") },
            { MakeIntrusive<TRequestCommonCounters>("CreateBinding") },
            { MakeIntrusive<TRequestCommonCounters>("ListBindings") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeBinding") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyBinding") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteBinding") },
            { MakeIntrusive<TRequestCommonCounters>("PingTask") },
            { MakeIntrusive<TRequestCommonCounters>("CreateDatabase") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeDatabase") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyDatabase") }
        });

        TTtlCache<TMetricsScope, TScopeCountersPtr, TMap> ScopeCounters{TTtlCacheSettings{}.SetTtl(TDuration::Days(1))};
        TTtlCache<TMetricsScope, TFinalStatusCountersPtr, TMap> FinalStatusCounters{TTtlCacheSettings{}.SetTtl(TDuration::Days(1))};

    public:
        ::NMonitoring::TDynamicCounterPtr Counters;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const ::NFq::TControlPlaneStorageConfig& config)
            : ScopeCounters{TTtlCacheSettings{}.SetTtl(config.MetricsTtl)}
            , FinalStatusCounters{TTtlCacheSettings{}.SetTtl(config.MetricsTtl)}
            , Counters(counters)
        {
            for (auto& request: CommonRequests) {
                request->Register(Counters);
            }
        }

        TRequestCounters GetCounters(const TString& cloudId, const TString& scope, ERequestTypeScope scopeType, ERequestTypeCommon commonType) {
            return {GetScopeCounters(cloudId, scope, scopeType), GetCommonCounters(commonType)};
        }

        TRequestCommonCountersPtr GetCommonCounters(ERequestTypeCommon type) {
            return CommonRequests[type];
        }

        TFinalStatusCountersPtr GetFinalStatusCounters(const TString& cloudId, const TString& scope) {
            TMetricsScope key{cloudId, scope};
            TMaybe<TFinalStatusCountersPtr> cacheVal;
            FinalStatusCounters.Get(key, &cacheVal);
            if (cacheVal) {
                return *cacheVal;
            }

            auto scopeCounters = (cloudId ? Counters->GetSubgroup("cloud_id", cloudId) : Counters)
                                    ->GetSubgroup("scope", scope);

            auto finalStatusCounters = MakeIntrusive<TFinalStatusCounters>(scopeCounters);
            cacheVal = finalStatusCounters;
            FinalStatusCounters.Put(key, cacheVal);
            return finalStatusCounters;
        }

        TRequestScopeCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, ERequestTypeScope type) {
            TMetricsScope key{cloudId, scope};
            TMaybe<TScopeCountersPtr> cacheVal;
            ScopeCounters.Get(key, &cacheVal);

            if (!cacheVal) {
                cacheVal = std::make_shared<TScopeCounters>();
                ScopeCounters.Put(key, cacheVal);
            }

            auto scopeRequests = *cacheVal;

            if (!(*scopeRequests)[type]) {
                auto scopeCounters = (cloudId ? Counters->GetSubgroup("cloud_id", cloudId) : Counters)->GetSubgroup("scope", scope);
                auto requestScoupeCounters = MakeIntrusive<TRequestScopeCounters>(std::string(RequestTypeScopeNames[type]));
                requestScoupeCounters->Register(scopeCounters);
                (*scopeRequests)[type] = requestScoupeCounters;
            }

            return (*scopeRequests)[type];
        }
    };

    TCounters Counters;
    TStatusCodeByScopeCounters::TPtr FailedStatusCodeCounters;

    ::NFq::TYqSharedResources::TPtr YqSharedResources;

    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TString TenantName;

    // Query Quota
    THashMap<TString, ui32> QueryQuotas;
    THashMap<TString, TEvQuotaService::TQuotaUsageRequest::TPtr> QueryQuotaRequests;
    TInstant QuotasUpdatedAt = TInstant::Zero();
    bool QuotasUpdating = false;

    TString TablePathPrefix;

public:
    TYdbControlPlaneStorageActor(
        const NConfig::TControlPlaneStorageConfig& config,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& common,
        const NConfig::TComputeConfig& computeConfig,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NFq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const TString& tenantName)
        : TControlPlaneStorageUtils(config, s3Config, common, computeConfig)
        , Counters(counters, *Config)
        , FailedStatusCodeCounters(MakeIntrusive<TStatusCodeByScopeCounters>("FinalFailedStatusCode", counters->GetSubgroup("component", "QueryDiagnostic")))
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credProviderFactory)
        , TenantName(tenantName)
    {
    }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_STORAGE";

    void Bootstrap();

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvCreateQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListQueriesRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetQueryStatusRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvControlQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListJobsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeJobRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListConnectionsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListBindingsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvWriteResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvPingTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvNodesHealthCheckRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest, Handle);
        hFunc(NMon::TEvHttpInfo, Handle);
        hFunc(TEvQuotaService::TQuotaUsageRequest, Handle);
        hFunc(TEvQuotaService::TQuotaLimitChangeRequest, Handle);
        hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
        hFunc(TEvents::TEvSchemaCreated, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvFinalStatusReport, Handle);
    )

    void Handle(TEvControlPlaneStorage::TEvCreateQueryRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvListQueriesRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDescribeQueryRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvGetQueryStatusRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvControlQueryRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvGetResultDataRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvListJobsRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvCreateConnectionRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvListConnectionsRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDescribeConnectionRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvListBindingsRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvWriteResultDataRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvGetTaskRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvPingTaskRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvNodesHealthCheckRequest::TPtr& ev);

    void Handle(TEvents::TEvSchemaCreated::TPtr& ev);

    void Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev);
    void Handle(TEvQuotaService::TQuotaLimitChangeRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvCreateDatabaseRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvDescribeDatabaseRequest::TPtr& ev);
    void Handle(TEvControlPlaneStorage::TEvModifyDatabaseRequest::TPtr& ev);

    void Handle(TEvControlPlaneStorage::TEvFinalStatusReport::TPtr& ev);

    template <class TEventPtr, class TRequestActor, ERequestTypeCommon requestType>
    void HandleRateLimiterImpl(TEventPtr& ev);

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config->Proto.DebugString() << Endl;
                str << Endl;
            }
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    /*
    * Creating tables
    */
    void CreateDirectory();
    void CreateQueriesTable();
    void CreatePendingSmallTable();
    void CreateConnectionsTable();
    void CreateJobsTable();
    void CreateNodesTable();
    void CreateBindingsTable();
    void CreateIdempotencyKeysTable();
    void CreateResultSetsTable();
    void CreateQuotasTable();
    void CreateTenantsTable();
    void CreateTenantAcksTable();
    void CreateMappingsTable();
    void CreateComputeDatabasesTable();

    void RunCreateTableActor(const TString& path, NYdb::NTable::TTableDescription desc);
    void AfterTablesCreated();

private:
    template<class ResponseEvent, class Result, class RequestEventPtr>
    TFuture<bool> SendResponse(const TString& name,
        NActors::TActorSystem* actorSystem,
        const TAsyncStatus& status,
        TActorId self,
        const RequestEventPtr& ev,
        const TInstant& startTime,
        const TRequestCounters& requestCounters,
        const std::function<typename TPrepareResponseResultType<ResponseEvent, Result>::Type()>& prepare,
        TDebugInfoPtr debugInfo)
    {
        return status.Apply([=, requestCounters=requestCounters](const auto& future) mutable {
            NYql::TIssues internalIssues;
            NYql::TIssues issues;
            Result result;
            typename TPrepareResponseResultType<ResponseEvent, Result>::TResponseAuditDetails auditDetails; // void* for nonauditable events

            try {
                TStatus status = future.GetValue();
                if (status.IsSuccess()) {
                    if constexpr (ResponseEvent::Auditable) {
                        auto p = prepare();
                        result = std::move(p.first);
                        auditDetails = std::move(p.second);
                    } else {
                        result = prepare();
                    }
                } else {
                    issues.AddIssues(status.GetIssues());
                    internalIssues.AddIssues(status.GetIssues());
                }
            } catch (const TCodeLineException& exception) {
                NYql::TIssue issue = MakeErrorIssue(exception.Code, exception.GetRawMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(exception.Code, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (const std::exception& exception) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, exception.what());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (...) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            }

            const auto& request = ev->Get()->Request;
            size_t responseByteSize = 0;
            if (issues) {
                CPS_LOG_AS_W(*actorSystem, name << ": {" << TrimForLogs(request.DebugString()) << "} ERROR: " << internalIssues.ToOneLineString());
                auto event = std::make_unique<ResponseEvent>(issues);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters.IncError();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters.Common->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": {" << TrimForLogs(result.DebugString()) << "} SUCCESS");
                std::unique_ptr<ResponseEvent> event;
                if constexpr (ResponseEvent::Auditable) {
                    event = std::make_unique<ResponseEvent>(result, auditDetails);
                } else {
                    event = std::make_unique<ResponseEvent>(result);
                }
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters.IncOk();
            }
            requestCounters.DecInFly();
            requestCounters.Common->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            return MakeFuture(!issues);
        });
    }

    template<class ResponseEvent, class Result, class RequestEventPtr>
    TFuture<bool> SendResponseTuple(const TString& name,
        NActors::TActorSystem* actorSystem,
        const TAsyncStatus& status,
        TActorId self,
        const RequestEventPtr& ev,
        const TInstant& startTime,
        const TRequestCounters& requestCounters,
        const std::function<Result()>& prepare,
        TDebugInfoPtr debugInfo)
    {
        return status.Apply([=, requestCounters=requestCounters](const auto& future) mutable {
            NYql::TIssues internalIssues;
            NYql::TIssues issues;
            Result result;

            try {
                TStatus status = future.GetValue();
                if (status.IsSuccess()) {
                    result = prepare();
                } else {
                    issues.AddIssues(status.GetIssues());
                }
            } catch (const TCodeLineException& exception) {
                NYql::TIssue issue = MakeErrorIssue(exception.Code, exception.GetRawMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(exception.Code, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (const std::exception& exception) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, exception.what());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (...) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            }

            size_t responseByteSize = 0;
            if (issues) {
                CPS_LOG_AS_W(*actorSystem, name << " ERROR: " << internalIssues.ToOneLineString());
                std::unique_ptr<ResponseEvent> event(new ResponseEvent(issues));
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters.IncError();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters.Common->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": SUCCESS");
                std::unique_ptr<ResponseEvent> event(new ResponseEvent(std::make_from_tuple<ResponseEvent>(result)));
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters.IncOk();
            }
            requestCounters.DecInFly();
            requestCounters.Common->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            return MakeFuture(!issues);
        });
    }

    template<typename T>
    void SendResponseIssues(const TActorId sender,
                            const NYql::TIssues& issues,
                            ui64 cookie,
                            const TDuration& delta,
                            TRequestCounters requestCounters) {
        std::unique_ptr<T> event(new T{issues});
        requestCounters.Common->ResponseBytes->Add(event->GetByteSize());
        Send(sender, event.release(), 0, cookie);
        requestCounters.DecInFly();
        requestCounters.IncError();
        requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
    }

    struct TPickTaskParams {
        TString ReadQuery;
        TParams ReadParams;
        std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)> PrepareParams;
        TString QueryId;
        bool RetryOnTli = false;
    };

    NThreading::TFuture<void> PickTask(
        const TPickTaskParams& taskParams,
        const TRequestCounters& requestCounters,
        TDebugInfoPtr debugInfo,
        std::shared_ptr<TResponseTasks> responseTasks,
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW());

    ui64 GetExecutionLimitMills(
        FederatedQuery::QueryContent_QueryType queryType,
        const TMaybe<TQuotaMap>& quotas);
};

}
