#pragma once

#include "config.h"
#include "control_plane_storage.h"
#include "control_plane_storage_counters.h"
#include "exceptions.h"
#include "extractors.h"
#include "probes.h"
#include "request_validators.h"
#include "util.h"
#include "validators.h"
#include <ydb/core/yq/libs/control_plane_storage/internal/response_tasks.h>

#include <util/generic/guid.h>
#include <util/system/yassert.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/security/util.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/yq/libs/config/yq_issue.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>
#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/quota_manager/events/events.h>
#include <ydb/core/yq/libs/ydb/util.h>
#include <ydb/core/yq/libs/ydb/ydb.h>


namespace NYq {

using namespace NActors;
using namespace NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

inline static void PrepareAccessConditionImpl(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user, TPermissions::TPermission privatePermission, TPermissions::TPermission publicPermission) {
    if (permissions.Check(publicPermission) && permissions.Check(privatePermission)) {
        // any row
    } else if (permissions.Check(publicPermission)) {
        builder.AddString("user", user);
        builder.AddInt64("visibility_scope", YandexQuery::Acl::SCOPE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_scope OR `" USER_COLUMN_NAME "` = $user)");
    } else if (permissions.Check(privatePermission)) {
        builder.AddInt64("visibility_private", YandexQuery::Acl::PRIVATE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_private)");
    } else {
        builder.AddString("user", user);
        builder.AddInt64("visibility_private", YandexQuery::Acl::PRIVATE);
        builder.AddText(" AND (`" VISIBILITY_COLUMN_NAME "` = $visibility_private AND `" USER_COLUMN_NAME "` = $user)");
    }
}

inline static void PrepareViewAccessCondition(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user) {
    PrepareAccessConditionImpl(builder, permissions, user, TPermissions::VIEW_PRIVATE, TPermissions::VIEW_PUBLIC);
}

inline static void PrepareManageAccessCondition(TSqlQueryBuilder& builder, TPermissions permissions, const TString& user) {
    PrepareAccessConditionImpl(builder, permissions, user, TPermissions::MANAGE_PRIVATE, TPermissions::MANAGE_PUBLIC);
}

inline static bool HasAccessImpl(TPermissions permissions, YandexQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user, TPermissions::TPermission privatePermission, TPermissions::TPermission publicPermission) {
    return (permissions.Check(publicPermission) && permissions.Check(privatePermission))
        || (permissions.Check(publicPermission) && (entityVisibility == YandexQuery::Acl::SCOPE || entityUser == user))
        || (permissions.Check(privatePermission) && entityVisibility == YandexQuery::Acl::PRIVATE)
        || (entityVisibility == YandexQuery::Acl::PRIVATE && entityUser == user);
}


inline static bool HasViewAccess(TPermissions permissions, YandexQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user) {
    return HasAccessImpl(permissions, entityVisibility, entityUser, user, TPermissions::VIEW_PRIVATE, TPermissions::VIEW_PUBLIC);
}

inline static bool HasManageAccess(TPermissions permissions, YandexQuery::Acl::Visibility entityVisibility, const TString& entityUser, const TString& user) {
    return HasAccessImpl(permissions, entityVisibility, entityUser, user, TPermissions::MANAGE_PRIVATE, TPermissions::MANAGE_PUBLIC);
}

TAsyncStatus ExecDbRequest(TDbPool::TPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler);

LWTRACE_USING(YQ_CONTROL_PLANE_STORAGE_PROVIDER);

using TRequestCountersPtr = TIntrusivePtr<TRequestCounters>;

    template<typename T>
    THashMap<TString, T> GetEntitiesWithVisibilityPriority(const TResultSet& resultSet, const TString& columnName)
    {
        THashMap<TString, T> entities;
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            T entity;
            Y_VERIFY(entity.ParseFromString(*parser.ColumnParser(columnName).GetOptionalString()));
            const TString name = entity.content().name();
            if (auto it = entities.find(name); it != entities.end()) {
                const auto visibility = entity.content().acl().visibility();
                if (visibility == YandexQuery::Acl::PRIVATE) {
                    entities[name] = std::move(entity);
                }
            } else {
                entities[name] = std::move(entity);
            }
        }

        return entities;
    }

    template<typename T>
    TVector<T> GetEntities(const TResultSet& resultSet, const TString& columnName)
    {
        TVector<T> entities;
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            Y_VERIFY(entities.emplace_back().ParseFromString(*parser.ColumnParser(columnName).GetOptionalString()));
        }
        return entities;
    }

void InsertIdempotencyKey(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey, const TString& response, const TInstant& expireAt);

void ReadIdempotencyKeyQuery(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey);

class TRequestCountersScope {
    TRequestCountersPtr Counters;
public:
    TRequestCountersScope(TRequestCountersPtr counters, ui64 requestSize) : Counters(counters) {
        StartTime = TInstant::Now();
        Counters->InFly->Inc();
        Counters->RequestBytes->Add(requestSize);
    }

    void Reply(const NYql::TIssues& issues, ui64 resultSize) {
        Delta = TInstant::Now() - StartTime;
        Counters->ResponseBytes->Add(resultSize);
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect(Delta.MilliSeconds());
        if (issues) {
            Counters->Error->Inc();
            for (const auto& issue : issues) {
                NYql::WalkThroughIssues(issue, true, [&counters=Counters](const NYql::TIssue& err, ui16 level) {
                    Y_UNUSED(level);
                    counters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                });
            }
        } else {
            Counters->Ok->Inc();
        }
    }
    TInstant StartTime;
    TDuration Delta;
};

class TYdbControlPlaneStorageActor : public NActors::TActorBootstrapped<TYdbControlPlaneStorageActor> {
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
        RTS_MAX,
    };

    enum ERequestTypeCommon {
        RTC_WRITE_RESULT_DATA,
        RTC_GET_TASK,
        RTC_NODES_HEALTH_CHECK,
        RTS_QUOTA_USAGE,
        RTC_MAX,
    };

    class TCounters: public virtual TThrRefBase {
        struct TMetricsScope {
            TString CloudId;
            TString Scope;

            bool operator<(const TMetricsScope& right) const {
                return std::make_pair(CloudId, Scope) < std::make_pair(right.CloudId, right.Scope);
            }
        };

        using TScopeCounters = std::array<TRequestCountersPtr, RTS_MAX>;
        using TScopeCountersPtr = std::shared_ptr<TScopeCounters>;
        using TFinalStatusCountersPtr = TIntrusivePtr<TFinalStatusCounters>;

        std::array<TRequestCountersPtr, RTC_MAX> CommonRequests = CreateArray<RTC_MAX, TRequestCountersPtr>({
            { MakeIntrusive<TRequestCounters>("WriteResultData") },
            { MakeIntrusive<TRequestCounters>("GetTask") },
            { MakeIntrusive<TRequestCounters>("NodesHealthCheck") },
            { MakeIntrusive<TRequestCounters>("GetQuotaUsage") },
        });

        TMap<TMetricsScope, TScopeCountersPtr> ScopeCounters;
        TMap<TMetricsScope, TFinalStatusCountersPtr> FinalStatusCounters;

    public:
        ::NMonitoring::TDynamicCounterPtr Counters;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: CommonRequests) {
                request->Register(Counters);
            }
        }

        TRequestCountersPtr GetCommonCounters(ERequestTypeCommon type) {
            return CommonRequests[type];
        }

        TFinalStatusCountersPtr GetFinalStatusCounters(const TString& cloudId, const TString& scope) {
            TMetricsScope key{cloudId, scope};
            auto it = FinalStatusCounters.find(key);
            if (it != FinalStatusCounters.end()) {
                return it->second;
            }

            auto scopeCounters = (cloudId ? Counters->GetSubgroup("cloud_id", cloudId) : Counters)
                                    ->GetSubgroup("scope", scope);

            auto finalStatusCounters = MakeIntrusive<TFinalStatusCounters>(scopeCounters);

            FinalStatusCounters[key] = finalStatusCounters;
            return finalStatusCounters;
        }

        TRequestCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, ERequestTypeScope type) {
            TMetricsScope key{cloudId, scope};
            auto it = ScopeCounters.find(key);
            if (it != ScopeCounters.end()) {
                return (*it->second)[type];
            }

            auto scopeRequests = std::make_shared<TScopeCounters>(CreateArray<RTS_MAX, TRequestCountersPtr>({
                { MakeIntrusive<TRequestCounters>("CreateQuery") },
                { MakeIntrusive<TRequestCounters>("ListQueries") },
                { MakeIntrusive<TRequestCounters>("DescribeQuery") },
                { MakeIntrusive<TRequestCounters>("GetQueryStatus") },
                { MakeIntrusive<TRequestCounters>("ModifyQuery") },
                { MakeIntrusive<TRequestCounters>("DeleteQuery") },
                { MakeIntrusive<TRequestCounters>("ControlQuery") },
                { MakeIntrusive<TRequestCounters>("GetResultData") },
                { MakeIntrusive<TRequestCounters>("ListJobs") },
                { MakeIntrusive<TRequestCounters>("DescribeJob") },
                { MakeIntrusive<TRequestCounters>("CreateConnection") },
                { MakeIntrusive<TRequestCounters>("ListConnections") },
                { MakeIntrusive<TRequestCounters>("DescribeConnection") },
                { MakeIntrusive<TRequestCounters>("ModifyConnection") },
                { MakeIntrusive<TRequestCounters>("DeleteConnection") },
                { MakeIntrusive<TRequestCounters>("CreateBinding") },
                { MakeIntrusive<TRequestCounters>("ListBindings") },
                { MakeIntrusive<TRequestCounters>("DescribeBinding") },
                { MakeIntrusive<TRequestCounters>("ModifyBinding") },
                { MakeIntrusive<TRequestCounters>("DeleteBinding") },
                { MakeIntrusive<TRequestCounters>("PingTask") },
            }));

            auto scopeCounters = (cloudId ? Counters->GetSubgroup("cloud_id", cloudId) : Counters)
                                    ->GetSubgroup("scope", scope);

            for (auto& request: *scopeRequests) {
                request->Register(scopeCounters);
            }

            ScopeCounters[key] = scopeRequests;
            return (*scopeRequests)[type];
        }
    };

    TCounters Counters;

    ::NYq::TControlPlaneStorageConfig Config;

    TYdbConnectionPtr YdbConnection;

    ::NYq::TYqSharedResources::TPtr YqSharedResources;
    TDbPool::TPtr DbPool;

    static constexpr int64_t InitialRevision = 1;

    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TString TenantName;

    // Query Quota
    THashMap<TString, ui32> QueryQuotas;
    THashMap<TString, TEvQuotaService::TQuotaUsageRequest::TPtr> QueryQuotaRequests;
    TInstant QuotasUpdatedAt = TInstant::Zero();
    bool QuotasUpdating = false;

public:
    TYdbControlPlaneStorageActor(
        const NConfig::TControlPlaneStorageConfig& config,
        const NConfig::TCommonConfig& common,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const TString& tenantName)
        : Counters(counters)
        , Config(config, common)
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
        hFunc(NMon::TEvHttpInfo, Handle);
        hFunc(TEvQuotaService::TQuotaUsageRequest, Handle);
        hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
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

    void Handle(TEvControlPlaneStorage::TEvNodesHealthCheckRequest::TPtr& ev);

    void Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev);

    template<typename T>
    NYql::TIssues ValidateConnection(T& ev, bool clickHousePasswordRequire = true)
    {
        return ::NYq::ValidateConnection<T>(ev, Config.Proto.GetMaxRequestSize(),
                                  Config.AvailableConnections, Config.Proto.GetDisableCurrentIam(),
                                  clickHousePasswordRequire);
    }

    template<typename T>
     NYql::TIssues ValidateBinding(T& ev)
    {
        return ::NYq::ValidateBinding<T>(ev, Config.Proto.GetMaxRequestSize(), Config.AvailableBindings);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.Proto.DebugString() << Endl;
                str << Endl;
            }
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    template<typename T>
    NYql::TIssues ValidateQuery(T& ev)
    {
        return ::NYq::ValidateQuery<T>(ev, Config.Proto.GetMaxRequestSize());
    }

    template<class P>
    NYql::TIssues ValidateEvent(P& ev)
    {
        return ::NYq::ValidateEvent<P>(ev, Config.Proto.GetMaxRequestSize());
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

    void RunCreateTableActor(const TString& path, NYdb::NTable::TTableDescription desc);

private:
    /*
    * Utility
    */
    bool IsSuperUser(const TString& user);

    std::pair<TAsyncStatus, std::shared_ptr<TVector<NYdb::TResultSet>>> Read(
        const TString& query,
        const NYdb::TParams& params,
        const TRequestCountersPtr& requestCounters,
        TDebugInfoPtr debugInfo,
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryOnTli = true);

    TAsyncStatus Validate(
        std::shared_ptr<TMaybe<TTransaction>> transaction,
        size_t item, const TVector<TValidationQuery>& validators,
        TSession session,
        std::shared_ptr<bool> successFinish,
        TDebugInfoPtr debugInfo,
        TTxSettings transactionMode = TTxSettings::SerializableRW());

    TAsyncStatus Write(
        NActors::TActorSystem* actorSystem,
        const TString& query,
        const NYdb::TParams& params,
        const TRequestCountersPtr& requestCounters,
        TDebugInfoPtr debugInfo,
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryTli = true);

    TAsyncStatus ReadModifyWrite(
        NActors::TActorSystem* actorSystem,
        const TString& readQuery,
        const NYdb::TParams& readParams,
        const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>& prepare,
        const TRequestCountersPtr& requestCounters,
        TDebugInfoPtr debugInfo = {},
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW(),
        bool retryOnTli = true);

    template<class ResponseEvent, class Result, class RequestEventPtr>
    TFuture<bool> SendResponse(const TString& name,
        NActors::TActorSystem* actorSystem,
        const TAsyncStatus& status,
        TActorId self,
        const RequestEventPtr& ev,
        const TInstant& startTime,
        const TRequestCountersPtr& requestCounters,
        const std::function<Result()>& prepare,
        TDebugInfoPtr debugInfo)
    {
        return status.Apply([=](const auto& future) {
            NYql::TIssues internalIssues;
            NYql::TIssues issues;
            Result result;

            try {
                TStatus status = future.GetValue();
                if (status.IsSuccess()) {
                    result = prepare();
                } else {
                    issues.AddIssues(status.GetIssues());
                    internalIssues.AddIssues(status.GetIssues());
                }
            } catch (const TControlPlaneStorageException& exception) {
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
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": {" << TrimForLogs(result.DebugString()) << "} SUCCESS");
                auto event = std::make_unique<ResponseEvent>(result);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
            requestCounters->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            return MakeFuture(!issues);
        });
    }

    template<class ResponseEvent, class Result, class AuditDetails, class RequestEventPtr>
    TFuture<bool> SendAuditResponse(const TString& name,
        NActors::TActorSystem* actorSystem,
        const TAsyncStatus& status,
        TActorId self,
        const RequestEventPtr& ev,
        const TInstant& startTime,
        const TRequestCountersPtr& requestCounters,
        const std::function<std::pair<Result, AuditDetails>()>& prepare,
        TDebugInfoPtr debugInfo)
    {
        return status.Apply([=](const auto& future) {
            NYql::TIssues internalIssues;
            NYql::TIssues issues;
            Result result;
            AuditDetails auditDetails;

            try {
                TStatus status = future.GetValue();
                if (status.IsSuccess()) {
                    auto p = prepare();
                    result = std::move(p.first);
                    auditDetails = std::move(p.second);
                } else {
                    issues.AddIssues(status.GetIssues());
                    internalIssues.AddIssues(status.GetIssues());
                }
            } catch (const TControlPlaneStorageException& exception) {
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
                CPS_LOG_AS_W(*actorSystem, name << ": {" << request.DebugString() << "} ERROR: " << internalIssues.ToOneLineString());
                auto event = std::make_unique<ResponseEvent>(issues);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": {" << result.DebugString() << "} SUCCESS");
                auto event = std::make_unique<ResponseEvent>(result, auditDetails);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
            requestCounters->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
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
        const TRequestCountersPtr& requestCounters,
        const std::function<Result()>& prepare,
        TDebugInfoPtr debugInfo)
    {
        return status.Apply([=](const auto& future) {
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
            } catch (const TControlPlaneStorageException& exception) {
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
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": SUCCESS");
                std::unique_ptr<ResponseEvent> event(new ResponseEvent(std::make_from_tuple<ResponseEvent>(result)));
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
            requestCounters->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            return MakeFuture(!issues);
        });
    }

    template<typename T>
    void SendResponseIssues(const TActorId sender,
                            const NYql::TIssues& issues,
                            ui64 cookie,
                            const TDuration& delta,
                            const TRequestCountersPtr& requestCounters) {
        std::unique_ptr<T> event(new T{issues});
        requestCounters->ResponseBytes->Add(event->GetByteSize());
        Send(sender, event.release(), 0, cookie);
        requestCounters->InFly->Dec();
        requestCounters->Error->Inc();
        requestCounters->LatencyMs->Collect(delta.MilliSeconds());
    }

    static YandexQuery::CommonMeta CreateCommonMeta(const TString& id, const TString& user, const TInstant& startTime, int64_t revision) {
        YandexQuery::CommonMeta common;
        common.set_id(id);
        common.set_created_by(user);
        common.set_modified_by(user);
        auto now = NProtoInterop::CastToProto(startTime);
        *common.mutable_created_at() = now;
        *common.mutable_modified_at() = now;
        common.set_revision(revision);
        return common;
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

    struct TPickTaskParams {
        TString ReadQuery;
        TParams ReadParams;
        std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)> PrepareParams;
        TString QueryId;
        bool RetryOnTli = false;
    };

    NThreading::TFuture<void> PickTask(
        const TPickTaskParams& taskParams,
        const TRequestCountersPtr& requestCounters,
        TDebugInfoPtr debugInfo,
        std::shared_ptr<TResponseTasks> responseTasks,
        const TVector<TValidationQuery>& validators = {},
        TTxSettings transactionMode = TTxSettings::SerializableRW());

    TString AssignTenantName(const TString& cloudId, const TString& scope);
};

}
