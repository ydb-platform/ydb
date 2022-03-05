#pragma once

#include "control_plane_storage.h"
#include "control_plane_storage_counters.h"
#include "exceptions.h"
#include "extractors.h"
#include <ydb/core/yq/libs/control_plane_storage/internal/response_tasks.h>
#include "probes.h"
#include "util.h"
#include "validators.h"

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

LWTRACE_USING(YQ_CONTROL_PLANE_STORAGE_PROVIDER);

using TRequestCountersPtr = TIntrusivePtr<TRequestCounters>;

class TYdbControlPlaneStorageActor : public NActors::TActorBootstrapped<TYdbControlPlaneStorageActor> {
    enum ERequestType {
        RT_CREATE_QUERY,
        RT_LIST_QUERIES,
        RT_DESCRIBE_QUERY,
        RT_GET_QUERY_STATUS,
        RT_MODIFY_QUERY,
        RT_DELETE_QUERY,
        RT_CONTROL_QUERY,
        RT_GET_RESULT_DATA,
        RT_LIST_JOBS_DATA,
        RT_DESCRIBE_JOB,
        RT_CREATE_CONNECTION,
        RT_LIST_CONNECTIONS,
        RT_DESCRIBE_CONNECTION,
        RT_MODIFY_CONNECTION,
        RT_DELETE_CONNECTION,
        RT_CREATE_BINDING,
        RT_LIST_BINDINGS,
        RT_DESCRIBE_BINDING,
        RT_MODIFY_BINDING,
        RT_DELETE_BINDING,
        RT_WRITE_RESULT_DATA,
        RT_GET_TASK,
        RT_PING_TASK,
        RT_NODES_HEALTH_CHECK,
        RT_MAX,
    };

    struct TCounters: public virtual TThrRefBase {
        std::array<TRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TRequestCountersPtr>({
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
            { MakeIntrusive<TRequestCounters>("WriteResultData") },
            { MakeIntrusive<TRequestCounters>("GetTask") },
            { MakeIntrusive<TRequestCounters>("PingTask") },
            { MakeIntrusive<TRequestCounters>("NodesHealthCheck") },
        });

        NMonitoring::TDynamicCounterPtr Counters;

        explicit TCounters(const NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }
    };

    struct TConfig {
        NConfig::TControlPlaneStorageConfig Proto;
        TString IdsPrefix;
        TDuration IdempotencyKeyTtl;
        TDuration AutomaticQueriesTtl;
        TDuration ResultSetsTtl;
        TDuration AnalyticsRetryCounterUpdateTime;
        TDuration StreamingRetryCounterUpdateTime;
        TDuration TaskLeaseTtl;
        TSet<YandexQuery::ConnectionSetting::ConnectionCase> AvailableConnections;
        TSet<YandexQuery::BindingSetting::BindingCase> AvailableBindings;

        TConfig(const NConfig::TControlPlaneStorageConfig& config, const NConfig::TCommonConfig& common);
    };

    TCounters Counters;
    TFinalStatusCounters FinalStatusCounters;

    TConfig Config;

    TYdbConnectionPtr YdbConnection;

    ::NYq::TYqSharedResources::TPtr YqSharedResources;
    TDbPool::TPtr DbPool;

    static constexpr int64_t InitialRevision = 1;

    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TString TenantName;

public:
    TYdbControlPlaneStorageActor(
        const NConfig::TControlPlaneStorageConfig& config,
        const NConfig::TCommonConfig& common,
        const NMonitoring::TDynamicCounterPtr& counters,
        const ::NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credProviderFactory,
        const TString& tenantName)
        : Counters(counters)
        , FinalStatusCounters(counters)
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

    template<typename T>
    NYql::TIssues ValidateConnection(T& ev, bool clickHousePasswordRequire = true)
    {
        const auto& request = ev->Get()->Request;
        NYql::TIssues issues = ValidateEvent(ev);

        if (!request.has_content()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content field is not specified"));
        }

        const YandexQuery::ConnectionContent& content = request.content();
        if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.acl.visibility field is not specified"));
        }

        if (content.name() != to_lower(content.name())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Incorrect connection name: " + content.name() + ". Please use only lower case"));
        }

        if (!content.has_setting()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting field is not specified"));
        }

        const YandexQuery::ConnectionSetting& setting = content.setting();
        if (!Config.AvailableConnections.contains(setting.connection_case())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection of the specified type is disabled"));
        }

        switch (setting.connection_case()) {
        case YandexQuery::ConnectionSetting::kYdbDatabase: {
            const YandexQuery::YdbDatabase database = setting.ydb_database();
            if (!database.has_auth() || database.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.auth field is not specified"));
            }

            if (database.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
            }

            if (!database.database_id() && !(database.endpoint() && database.database())) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.ydb_database.{database_id||database,endpoint} field is not specified"));
            }
            break;
        }
        case YandexQuery::ConnectionSetting::kClickhouseCluster: {
            const YandexQuery::ClickHouseCluster ch = setting.clickhouse_cluster();
            if (!ch.has_auth() || ch.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.auth field is not specified"));
            }

            if (ch.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
            }

            if (!ch.database_id() && !(ch.host() && ch.port())) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.{database_id||host,port} field is not specified"));
            }

            if (!ch.login()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.login field is not specified"));
            }

            if (!ch.password() && clickHousePasswordRequire) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.clickhouse_cluster.password field is not specified"));
            }
            break;
        }
        case YandexQuery::ConnectionSetting::kObjectStorage: {
            const YandexQuery::ObjectStorageConnection objectStorage = setting.object_storage();
            if (!objectStorage.has_auth() || objectStorage.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.auth field is not specified"));
            }

            if (objectStorage.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
            }

            if (!objectStorage.bucket()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.object_storage.bucket field is not specified"));
            }
            break;
        }
        case YandexQuery::ConnectionSetting::kDataStreams: {
            const YandexQuery::DataStreams dataStreams = setting.data_streams();
            if (!dataStreams.has_auth() || dataStreams.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.auth field is not specified"));
            }

            if (dataStreams.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
            }

            if (!dataStreams.database_id() && !(dataStreams.endpoint() && dataStreams.database())) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.data_streams.{database_id||database,endpoint} field is not specified"));
            }
            break;
        }
        case YandexQuery::ConnectionSetting::kMonitoring: {
            const YandexQuery::Monitoring monitoring = setting.monitoring();
            if (!monitoring.has_auth() || monitoring.auth().identity_case() == YandexQuery::IamAuth::IDENTITY_NOT_SET) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.auth field is not specified"));
            }

            if (monitoring.auth().identity_case() == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "current iam authorization is disabled"));
            }

            if (!monitoring.project()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.project field is not specified"));
            }

            if (!monitoring.cluster()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "content.setting.monitoring.cluster field is not specified"));
            }
            break;
        }
        case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET: {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "connection is not set"));
            break;
        }
        // Do not add default. Adding a new connection should cause a compilation error
        }
        return issues;
    }

    template<typename T>
     NYql::TIssues ValidateBinding(T& ev)
    {
        const auto& request = ev->Get()->Request;
        NYql::TIssues issues = ValidateEvent(ev);

        if (request.has_content()) {
            const YandexQuery::BindingContent& content = request.content();
            if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.acl.visibility field is not specified"));
            }

            if (content.name() != to_lower(content.name())) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Incorrect binding name: " + content.name() + ". Please use only lower case"));
            }

            if (!content.has_setting()) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding.setting field is not specified"));
            }

            const YandexQuery::BindingSetting& setting = content.setting();
            if (!Config.AvailableBindings.contains(setting.binding_case())) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding of the specified type is disabled"));
            }

            switch (setting.binding_case()) {
            case YandexQuery::BindingSetting::kDataStreams: {
                const YandexQuery::DataStreamsBinding dataStreams = setting.data_streams();
                if (!dataStreams.has_schema()) {
                    issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "data streams with empty schema is forbidden"));
                }
                break;
            }
            case YandexQuery::BindingSetting::BINDING_NOT_SET: {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding is not set"));
                break;
            }
                // Do not replace with default. Adding a new binding should cause a compilation error
            case YandexQuery::BindingSetting::kObjectStorage:
                break;
            }
        } else {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "binding field is not specified"));
        }

        return issues;
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
        NYql::TIssues issues = ValidateEvent(ev);
        auto& request = ev->Get()->Request;
        const auto& content = request.content();

        if (request.execute_mode() == YandexQuery::ExecuteMode::EXECUTE_MODE_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "mode field is not specified"));
        }

        if (content.type() == YandexQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "type field is not specified"));
        }

        if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "acl.visibility field is not specified"));
        }

        if (content.type() == YandexQuery::QueryContent::STREAMING && !request.has_disposition()) {
            request.mutable_disposition()->mutable_fresh();
        }

        return issues;
    }

    template<class P>
    NYql::TIssues ValidateEvent(P& ev)
    {
        const auto& request = ev->Get()->Request;
        const TString scope = ev->Get()->Scope;
        const TString user = ev->Get()->User;
        const TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();

        NYql::TIssues issues;
        if (!scope) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "scope is not specified"));
        }

        if (!user) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "user is empty"));
        }

        if (!token) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "token is empty"));
        }

        if (byteSize > static_cast<int>(Config.Proto.GetMaxRequestSize())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "request size exceeded " + ToString(request.ByteSize()) + " out of " + ToString(Config.Proto.GetMaxRequestSize()) + " bytes"));
        }

        TString error;
        if (!request.validate(error)) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, error));
        }

        return issues;
    }

    /*
    * Creating tables
    */
    TAsyncStatus CreateDirectory(TActorSystem* as);
    TAsyncStatus CreateQueriesTable(TActorSystem* as);
    TAsyncStatus CreatePendingTable(TActorSystem* as);
    TAsyncStatus CreatePendingSmallTable(TActorSystem* as);
    TAsyncStatus CreateConnectionsTable(TActorSystem* as);
    TAsyncStatus CreateJobsTable(TActorSystem* as);
    TAsyncStatus CreateNodesTable(TActorSystem* as);
    TAsyncStatus CreateBindingsTable(TActorSystem* as);
    TAsyncStatus CreateIdempotencyKeysTable(TActorSystem* as);
    TAsyncStatus CreateResultSetsTable(TActorSystem* as);

private:
    /*
    * Utility
    */
    bool IsSuperUser(const TString& user);

    void InsertIdempotencyKey(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey, const TString& response, const TInstant& expireAt);

    void ReadIdempotencyKeyQuery(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey);

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

    template<typename T>
    THashMap<TString, T> GetEntitiesWithVisibilityPriority(const TResultSet& resultSet, const TString& columnName)
    {
        THashMap<TString, T> entities;
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            T entity;
            Y_VERIFY(entity.ParseFromString(*parser.ColumnParser(columnName).GetOptionalString())); // TODO: move to run actor
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
            if (issues) {
                CPS_LOG_AS_D(*actorSystem, name << ": " << request.DebugString() << " error: " << internalIssues.ToString());
                auto event = std::make_unique<ResponseEvent>(issues);
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": " << request.DebugString() << " success");
                auto event = std::make_unique<ResponseEvent>(result);
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
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
            if (issues) {
                CPS_LOG_AS_D(*actorSystem, name << ": " << request.DebugString() << " error: " << internalIssues.ToString());
                auto event = std::make_unique<ResponseEvent>(issues);
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ": " << request.DebugString() << " success");
                auto event = std::make_unique<ResponseEvent>(result, auditDetails);
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
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

            if (issues) {
                CPS_LOG_AS_D(*actorSystem, name << ": error: " << internalIssues.ToString());
                auto event = std::unique_ptr<ResponseEvent>(new ResponseEvent(issues));
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Error->Inc();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*actorSystem, name << ":  success");
                auto event = std::unique_ptr<ResponseEvent>(new ResponseEvent(std::make_from_tuple<ResponseEvent>(result)));
                event->DebugInfo = debugInfo;
                actorSystem->Send(new IEventHandle(ev->Sender, self, event.release(), 0, ev->Cookie));
                requestCounters->Ok->Inc();
            }
            requestCounters->InFly->Dec();
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
        Send(sender, new T(issues), 0, cookie);
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
