#include "ydb_control_plane_storage_impl.h"

#include <cstdint>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/schema.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/core/yq/libs/shared_resources/db_exec.h>

#include <util/digest/multi.h>

namespace {

YandexQuery::IamAuth::IdentityCase GetIamAuth(const YandexQuery::Connection& connection) {
    const auto& setting = connection.content().setting();
    switch (setting.connection_case()) {
        case YandexQuery::ConnectionSetting::kYdbDatabase:
            return setting.data_streams().auth().identity_case();
        case YandexQuery::ConnectionSetting::kClickhouseCluster:
            return setting.clickhouse_cluster().auth().identity_case();
        case YandexQuery::ConnectionSetting::kObjectStorage:
            return setting.object_storage().auth().identity_case();
        case YandexQuery::ConnectionSetting::kDataStreams:
            return setting.data_streams().auth().identity_case();
        case YandexQuery::ConnectionSetting::kMonitoring:
            return setting.monitoring().auth().identity_case();
        case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return YandexQuery::IamAuth::IDENTITY_NOT_SET;
    }
}

}

namespace NYq {

TString TYdbControlPlaneStorageActor::AssignTenantName(const TString& cloudId, const TString& scope) {
    const auto& mapping = Config.Proto.GetMapping();

    if (scope) {
        for (const auto& scopeToTenant: mapping.GetScopeToTenantName()) {
            if (scopeToTenant.GetKey() == scope) {
                return scopeToTenant.GetValue();
            }
        }
    }

    if (cloudId) {
        for (const auto& cloudToTenant: mapping.GetCloudIdToTenantName()) {
            if (cloudToTenant.GetKey() == cloudId) {
                return cloudToTenant.GetValue();
            }
        }
    }

    auto size = mapping.CommonTenantNameSize();

    if (size) {
        auto index = MultiHash(cloudId, scope) % size;
        return mapping.GetCommonTenantName(index);
    } else {
        return TenantName;
    }
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    auto it = event.Quotas.find(QUOTA_RESULT_LIMIT);
    ui64 resultLimit = (it != event.Quotas.end()) ? it->second.Limit : 0;
    auto exec_ttl_it = event.Quotas.find(QUOTA_TIME_LIMIT);
    ui64 executionLimitMills = (exec_ttl_it != event.Quotas.end()) ? exec_ttl_it->second.Limit : 0;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_CREATE_QUERY);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                                ? event.Permissions
                                : TPermissions{TPermissions::QUERY_INVOKE | TPermissions::CONNECTIONS_USE | TPermissions::BINDINGS_USE | TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const YandexQuery::CreateQueryRequest& request = event.Request;
    const size_t byteSize = request.ByteSizeLong();
    const TString queryId = GetEntityIdAsString(Config.IdsPrefix, EEntityType::QUERY);
    CPS_LOG_T("CreateQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateQuery(ev);
    if (request.execute_mode() != YandexQuery::SAVE && !permissions.Check(TPermissions::QUERY_INVOKE)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke"));
    }

    if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (request.disposition().has_from_last_checkpoint()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Streaming disposition \"from_last_checkpoint\" is not allowed in CreateQuery request"));
    }

    {
        auto it = event.Quotas.find(QUOTA_COUNT_LIMIT);
        if (it != event.Quotas.end()) {
            auto& quota = it->second;
            if (!quota.Usage) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::NOT_READY, "Control Plane is not ready yet. Please retry later."));
            } else if (*quota.Usage >= quota.Limit) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::QUOTA_EXCEEDED, Sprintf("Too many queries (%lu of %lu). Please delete other queries or increase limits.", *quota.Usage, quota.Limit)));
            }
        }
    }

    if (issues) {
        CPS_LOG_W("CreateQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(CreateQueryRequest, scope, user, delta, byteSize, false);
        return;
    }

    const TString idempotencyKey = request.idempotency_key();
    const TString jobId = request.execute_mode() == YandexQuery::SAVE ? "" : GetEntityIdAsString(Config.IdsPrefix, EEntityType::JOB);

    YandexQuery::Query query;
    YandexQuery::QueryContent& content = *query.mutable_content() = request.content();
    YandexQuery::QueryMeta& meta = *query.mutable_meta();
    YandexQuery::CommonMeta& common = *meta.mutable_common() = CreateCommonMeta(queryId, user, startTime, InitialRevision);
    meta.set_execute_mode(request.execute_mode());
    meta.set_status(request.execute_mode() == YandexQuery::SAVE ? YandexQuery::QueryMeta::COMPLETED : YandexQuery::QueryMeta::STARTING);

    YandexQuery::Job job;
    if (request.execute_mode() != YandexQuery::SAVE) {
        meta.set_last_job_query_revision(InitialRevision);
        meta.set_last_job_id(jobId);
        meta.set_started_by(user);

        *job.mutable_meta() = common;
        job.mutable_meta()->set_id(jobId);
        job.set_text(content.text());
        *job.mutable_query_meta() = meta;
        job.set_query_name(query.mutable_content()->name());
        *job.mutable_acl() = content.acl();
        job.set_automatic(content.automatic());
    }

    std::shared_ptr<std::pair<YandexQuery::CreateQueryResult, TAuditDetails<YandexQuery::Query>>> response = std::make_shared<std::pair<YandexQuery::CreateQueryResult, TAuditDetails<YandexQuery::Query>>>();
    response->first.set_query_id(queryId);
    response->second.CloudId = cloudId;

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "CreateQuery(read)");
    ReadIdempotencyKeyQuery(readQueryBuilder, scope, idempotencyKey);

    if (request.execute_mode() != YandexQuery::SAVE) {
        readQueryBuilder.AddString("scope", scope);
        readQueryBuilder.AddString("user", user);
        readQueryBuilder.AddInt64("scope_visibility", YandexQuery::Acl::SCOPE);

        // user connections
        readQueryBuilder.AddText(
            "SELECT `" CONNECTION_ID_COLUMN_NAME "`, `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME "`\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND (`" VISIBILITY_COLUMN_NAME "` = $scope_visibility OR `" USER_COLUMN_NAME "` = $user);"
        );

        // user bindings
        readQueryBuilder.AddText(
            "SELECT `" BINDING_ID_COLUMN_NAME "`, `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME "`\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND (`" VISIBILITY_COLUMN_NAME "` = $scope_visibility OR `" USER_COLUMN_NAME "` = $user);"
        );
    }

    auto prepareParams = [=](const TVector<TResultSet>& resultSets) mutable {
        const size_t countSets = (idempotencyKey ? 1 : 0) + (request.execute_mode() != YandexQuery::SAVE ? 2 : 0);
        if (resultSets.size() != countSets) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to " << countSets << " but equal " << resultSets.size() << ". Please contact internal support";
        }

        if (idempotencyKey) {
            TResultSetParser parser(resultSets.front());
            if (parser.TryNextRow()) {
                if (!response->first.ParseFromString(*parser.ColumnParser(RESPONSE_COLUMN_NAME).GetOptionalString())) {
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for idempotency key request. Please contact internal support";
                }
                response->second.IdempotencyResult = true;
                return make_pair(TString{}, TParamsBuilder{}.Build());
            }
        }

        YandexQuery::Internal::QueryInternal queryInternal;
        if (!Config.Proto.GetDisableCurrentIam()) {
            queryInternal.set_token(token);
        }

        queryInternal.set_cloud_id(cloudId);
        queryInternal.set_state_load_mode(YandexQuery::StateLoadMode::EMPTY);
        queryInternal.mutable_disposition()->CopyFrom(request.disposition());
        queryInternal.set_result_limit(resultLimit);
        *queryInternal.mutable_execution_ttl() = NProtoInterop::CastToProto(TDuration::MilliSeconds(executionLimitMills));

        if (request.execute_mode() != YandexQuery::SAVE) {
            // TODO: move to run actor priority selection

            TSet<TString> disabledConnections;
            for (const auto& connection: GetEntities<YandexQuery::Connection>(resultSets[resultSets.size() - 2], CONNECTION_COLUMN_NAME)) {
                if (!Config.AvailableConnections.contains(connection.content().setting().connection_case())) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }

                if (GetIamAuth(connection) == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
            }

            if (permissions.Check(TPermissions::CONNECTIONS_USE)) {
                auto connections = GetEntitiesWithVisibilityPriority<YandexQuery::Connection>(resultSets[resultSets.size() - 2], CONNECTION_COLUMN_NAME);
                for (const auto& [_, connection]: connections) {
                    if (disabledConnections.contains(connection.meta().id())) {
                        continue;
                    }
                    *queryInternal.add_connection() = connection;
                }
            }

            if (permissions.Check(TPermissions::BINDINGS_USE)) {
                auto bindings = GetEntitiesWithVisibilityPriority<YandexQuery::Binding>(resultSets[resultSets.size() - 1], BINDING_COLUMN_NAME);
                for (const auto& [_, binding]: bindings) {
                    if (!Config.AvailableBindings.contains(binding.content().setting().binding_case())) {
                        continue;
                    }

                    if (disabledConnections.contains(binding.content().connection_id())) {
                        continue;
                    }

                    *queryInternal.add_binding() = binding;
                }
            }
        }

        if (query.ByteSizeLong() > Config.Proto.GetMaxRequestSize()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Query data is not placed in the table. Please shorten your request";
        }

        if (queryInternal.ByteSizeLong() > Config.Proto.GetMaxRequestSize()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Query internal data is not placed in the table. Please reduce the number of connections and bindings";
        }

        response->second.After.ConstructInPlace().CopyFrom(query);

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "CreateQuery(write)");
        writeQueryBuilder.AddString("tenant", AssignTenantName(cloudId, scope));
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("query_id", queryId);
        writeQueryBuilder.AddString("name", query.content().name());
        writeQueryBuilder.AddInt64("status", query.meta().status());
        writeQueryBuilder.AddInt64("query_type", query.content().type());
        writeQueryBuilder.AddInt64("execute_mode", query.meta().execute_mode());
        writeQueryBuilder.AddString("user", user);
        writeQueryBuilder.AddInt64("visibility", query.content().acl().visibility());
        writeQueryBuilder.AddBool("automatic", query.content().automatic());
        writeQueryBuilder.AddInt64("revision", InitialRevision);
        writeQueryBuilder.AddString("query", query.SerializeAsString());
        writeQueryBuilder.AddString("internal", queryInternal.SerializeAsString());
        writeQueryBuilder.AddString("job_id", jobId);

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config.IdempotencyKeyTtl);

        if (request.execute_mode() != YandexQuery::SAVE) {
            writeQueryBuilder.AddString("job", job.SerializeAsString());
            writeQueryBuilder.AddTimestamp("zero_timestamp", TInstant::Zero());
            writeQueryBuilder.AddTimestamp("now", TInstant::Now());

            // insert job
            writeQueryBuilder.AddText(
                "INSERT INTO `" JOBS_TABLE_NAME "`\n"
                "(`" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`, \n"
                "`" JOB_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`)\n"
                "VALUES\n"
                "    ($scope, $query_id, $job_id, $job, $user, $visibility);"
            );

            // insert pending small
            writeQueryBuilder.AddText(
                "INSERT INTO `" PENDING_SMALL_TABLE_NAME "`\n"
                "(`" TENANT_COLUMN_NAME "`, `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`,  `" QUERY_TYPE_COLUMN_NAME "`, `" LAST_SEEN_AT_COLUMN_NAME "`, `" ASSIGNED_UNTIL_COLUMN_NAME "`,\n"
                "`" RETRY_COUNTER_COLUMN_NAME "`, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "`, `" HOST_NAME_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "`)\n"
                "VALUES\n"
                "    ($tenant, $scope, $query_id, $query_type, $zero_timestamp, $zero_timestamp, 0, $now, \"\", \"\");"
            );
        }

        // insert query
        writeQueryBuilder.AddText(
            "INSERT INTO `" QUERIES_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" NAME_COLUMN_NAME "`, `" STATUS_COLUMN_NAME "`, `" QUERY_TYPE_COLUMN_NAME "`, "
            "`" EXECUTE_MODE_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" AUTOMATIC_COLUMN_NAME "`, "
            "`" REVISION_COLUMN_NAME "`, `" QUERY_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`, `" LAST_JOB_ID_COLUMN_NAME "`, `" GENERATION_COLUMN_NAME "`, `" META_REVISION_COLUMN_NAME "`, "
            "`" TENANT_COLUMN_NAME "`)\n"
            "VALUES ($scope, $query_id, $name, $status, $query_type, "
            "$execute_mode, $user,  $visibility, $automatic, "
            "$revision, $query, $internal, $job_id, 0, 0, $tenant);"
        );

        const auto write = writeQueryBuilder.Build();
        return make_pair(write.Sql, write.Params);
    };

    const auto read = readQueryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus status = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), read.Sql, read.Params, prepareParams, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvCreateQueryResponse, YandexQuery::CreateQueryResult, TAuditDetails<YandexQuery::Query>>(
        "CreateQueryRequest - CreateQueryResult",
        NActors::TActivationContext::ActorSystem(),
        status,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(CreateQueryRequest, scope, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvListQueriesRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvListQueriesRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_LIST_QUERIES);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());

    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    const YandexQuery::ListQueriesRequest& request = event.Request;
    const TString pageToken = request.page_token();
    const int byteSize = request.ByteSize();
    const int64_t limit = request.limit();
    CPS_LOG_T("ListQueriesRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("ListQueriesRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvListQueriesResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ListQueriesRequest, scope, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ListQueries");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("last_query", pageToken);
    queryBuilder.AddTimestamp("now", TInstant::Now());
    queryBuilder.AddUint64("limit", limit + 1);

    queryBuilder.AddText(
        "SELECT `" QUERY_ID_COLUMN_NAME "`, `" QUERY_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` >= $last_query AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now)"
    );

    TString filter;
    if (request.has_filter()) {
        TVector<TString> filters;
        if (request.filter().name()) {
            queryBuilder.AddString("filter_name", request.filter().name());
            filters.push_back("`" NAME_COLUMN_NAME "` ILIKE '%' || $filter_name || '%'");
        }

        if (request.filter().query_type() != YandexQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
            queryBuilder.AddInt64("filter_query_type", request.filter().query_type());
            filters.push_back("`" QUERY_TYPE_COLUMN_NAME "` = $filter_query_type");
        }

        if (request.filter().status_size() > 0) {
            NYdb::TValueBuilder listStatusesBuilder;
            listStatusesBuilder.BeginList();
            for (const auto& status: request.filter().status()) {
                listStatusesBuilder.AddListItem(NYdb::TValueBuilder().Int64(status).Build());
            }
            listStatusesBuilder.EndList();
            queryBuilder.AddValue("filter_statuses", listStatusesBuilder.Build());
            filters.push_back("`" STATUS_COLUMN_NAME "` IN $filter_statuses");
        }

        if (request.filter().mode_size() > 0) {
            NYdb::TValueBuilder listModesBuilder;
            listModesBuilder.BeginList();
            for (const auto& mode: request.filter().mode()) {
                listModesBuilder.AddListItem(NYdb::TValueBuilder().Int64(mode).Build());
            }
            listModesBuilder.EndList();
            queryBuilder.AddValue("filter_modes", listModesBuilder.Build());
            filters.push_back("`" EXECUTE_MODE_COLUMN_NAME "` IN $filter_modes");
        }

        if (request.filter().created_by_me()) {
            queryBuilder.AddString("user", user);
            filters.push_back("`" USER_COLUMN_NAME "` = $user");
        }

        if (request.filter().visibility() != YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
            queryBuilder.AddInt64("filter_visibility", request.filter().visibility());
            filters.push_back("`" VISIBILITY_COLUMN_NAME "` = $filter_visibility");
        }

        switch (request.filter().automatic()) {
        case YandexQuery::AUTOMATIC:
            filters.push_back("`" AUTOMATIC_COLUMN_NAME "` = true");
            break;
        case YandexQuery::NOT_AUTOMATIC:
            filters.push_back("`" AUTOMATIC_COLUMN_NAME "` = false");
            break;
        default:
            break;
        }

        filter = JoinSeq(" AND ", filters);
    }

    PrepareViewAccessCondition(queryBuilder, permissions, user);

    if (filter) {
        queryBuilder.AddText(" AND (" + filter + ")\n");
    }

    queryBuilder.AddText(
        "ORDER BY " QUERY_ID_COLUMN_NAME "\n"
        "LIMIT $limit;"
    );

    const auto read = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(read.Sql, read.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        YandexQuery::ListQueriesResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            YandexQuery::Query query;
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }
            YandexQuery::BriefQuery briefQuery;
            const auto lastJobId = query.meta().last_job_id();
            query.mutable_meta()->set_last_job_id(lastJobId + "-" + query.meta().common().id());
            *briefQuery.mutable_meta() = query.meta();
            briefQuery.set_name(query.content().name());
            briefQuery.set_type(query.content().type());
            briefQuery.set_visibility(query.content().acl().visibility());
            briefQuery.set_automatic(query.content().automatic());
            *result.add_query() = briefQuery;
        }

        if (result.query_size() == limit + 1) {
            result.set_next_page_token(result.query(result.query_size() - 1).meta().common().id());
            result.mutable_query()->RemoveLast();
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvListQueriesResponse, YandexQuery::ListQueriesResult>(
        "ListQueriesRequest - ListQueriesResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(ListQueriesRequest, scope, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDescribeQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDescribeQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_DESCRIBE_QUERY);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_AST};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    const YandexQuery::DescribeQueryRequest& request = event.Request;
    const TString queryId = request.query_id();
    const int byteSize = request.ByteSize();
    CPS_LOG_T("DescribeQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("DescribeQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDescribeQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DescribeQueryRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeQuery");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("query_id", queryId);
    queryBuilder.AddTimestamp("now", TInstant::Now());
    queryBuilder.AddText(
        "SELECT `" QUERY_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);"
    );
    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, user,permissions] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        YandexQuery::DescribeQueryResult result;
        if (!result.mutable_query()->ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
        }

        const auto lastJobId = result.query().meta().last_job_id();
        result.mutable_query()->mutable_meta()->set_last_job_id(lastJobId + "-" + result.query().meta().common().id());

        const auto queryVisibility = result.query().content().acl().visibility();
        const auto queryUser = result.query().meta().common().created_by();
        const bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
        if (!hasViewAccess) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        if (!permissions.Check(TPermissions::VIEW_AST)) {
            result.mutable_query()->clear_ast();
        }

        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeQueryResponse, YandexQuery::DescribeQueryResult>(
        "DescribeQueryRequest - DescribeQueryResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(DescribeQueryRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvGetQueryStatusRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvGetQueryStatusRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_GET_QUERY_STATUS);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_AST};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const YandexQuery::GetQueryStatusRequest& request = event.Request;
    const TString queryId = request.query_id();
    const int byteSize = request.ByteSize();
    CPS_LOG_T("GetQueryStatusRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("GetQueryStatusRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvGetQueryStatusResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(GetQueryStatusRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "GetQueryStatus");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("query_id", queryId);
    queryBuilder.AddTimestamp("now", TInstant::Now());

    queryBuilder.AddText(
        "SELECT `" STATUS_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" META_REVISION_COLUMN_NAME  "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);"
    );

    const auto read = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(read.Sql, read.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, user,permissions] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        YandexQuery::GetQueryStatusResult result;
        result.set_status(static_cast<YandexQuery::QueryMeta_ComputeStatus>(*parser.ColumnParser(STATUS_COLUMN_NAME).GetOptionalInt64()));
        result.set_meta_revision(parser.ColumnParser(META_REVISION_COLUMN_NAME).GetOptionalInt64().GetOrElse(0));

        const auto queryVisibility = static_cast<YandexQuery::Acl::Visibility>(*parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64());
        const auto queryUser = *parser.ColumnParser(USER_COLUMN_NAME).GetOptionalString();
        const bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
        if (!hasViewAccess) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvGetQueryStatusResponse, YandexQuery::GetQueryStatusResult>(
        "GetQueryStatusRequest - GetQueryStatusResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(DescribeQueryRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TEvControlPlaneStorage::TEvModifyQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_MODIFY_QUERY);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                            ? event.Permissions
                            : TPermissions{TPermissions::QUERY_INVOKE | TPermissions::CONNECTIONS_USE | TPermissions::BINDINGS_USE | TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    YandexQuery::ModifyQueryRequest& request = event.Request;
    const TString queryId = request.query_id();
    const int byteSize = request.ByteSize();
    const int64_t previousRevision = request.previous_revision();
    CPS_LOG_T("ModifyQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    if (request.content().type() == YandexQuery::QueryContent::STREAMING && request.state_load_mode() == YandexQuery::STATE_LOAD_MODE_UNSPECIFIED) {
        request.set_state_load_mode(YandexQuery::EMPTY);
    }

    NYql::TIssues issues = ValidateQuery(ev);
    if (request.execute_mode() != YandexQuery::SAVE && !permissions.Check(TPermissions::QUERY_INVOKE)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke"));
    }

    if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (request.state_load_mode() == YandexQuery::FROM_LAST_CHECKPOINT) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::UNSUPPORTED, "State load mode \"FROM_LAST_CHECKPOINT\" is not supported"));
    }
    if (issues) {
        CPS_LOG_W("ModifyQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvModifyQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ModifyQueryRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    const TString idempotencyKey = request.idempotency_key();

    std::shared_ptr<std::pair<YandexQuery::ModifyQueryResult, TAuditDetails<YandexQuery::Query>>> response =
        std::make_shared<std::pair<YandexQuery::ModifyQueryResult, TAuditDetails<YandexQuery::Query>>>();

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ModifyQuery(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("query_id", queryId);
    readQueryBuilder.AddTimestamp("now", TInstant::Now());

    if (request.execute_mode() != YandexQuery::SAVE) {
        readQueryBuilder.AddString("user", user);
        readQueryBuilder.AddInt64("scope_visibility", YandexQuery::Acl::SCOPE);
        // user connections
        readQueryBuilder.AddText(
            "SELECT `" CONNECTION_ID_COLUMN_NAME "`, `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME "`\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND (`" VISIBILITY_COLUMN_NAME "` = $scope_visibility OR `" USER_COLUMN_NAME "` = $user);\n"
        );

        // user bindings
        readQueryBuilder.AddText(
            "SELECT `" BINDING_ID_COLUMN_NAME "`, `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME "`\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND (`" VISIBILITY_COLUMN_NAME "` = $scope_visibility OR `" USER_COLUMN_NAME "` = $user);\n"
        );
    }

    readQueryBuilder.AddText(
        "SELECT `" QUERY_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`, `" RESULT_ID_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);"
    );

    auto prepareParams = [=, config=Config](const TVector<TResultSet>& resultSets) {
        const size_t countSets = 1 + (request.execute_mode() != YandexQuery::SAVE ? 2 : 0);

        if (resultSets.size() != countSets) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to " << countSets << " but equal " << resultSets.size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets.back());

        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        YandexQuery::Query query;
        if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
        }

        YandexQuery::Internal::QueryInternal internal;
        if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
        }

        const TString resultId = request.execute_mode() == YandexQuery::SAVE ? parser.ColumnParser(RESULT_ID_COLUMN_NAME).GetOptionalString().GetOrElse("") : "";

        const auto queryVisibility = query.content().acl().visibility();
        const auto queryUser = query.meta().common().created_by();
        const bool hasManageAccess = HasManageAccess(permissions, queryVisibility, queryUser, user);
        if (!hasManageAccess) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        if (query.content().type() != request.content().type()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Query type cannot be changed. Please specify " << YandexQuery::QueryContent_QueryType_Name(query.content().type()) << " instead of " << YandexQuery::QueryContent_QueryType_Name(request.content().type());
        }

        if (query.content().acl().visibility() == YandexQuery::Acl::SCOPE && request.content().acl().visibility() == YandexQuery::Acl::PRIVATE) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new query with visibility PRIVATE";
        }

        auto oldVisibility = query.content().acl().visibility();

        auto now = TInstant::Now();
        auto& common = *query.mutable_meta()->mutable_common();
        common.set_revision(common.revision() + 1);
        common.set_modified_by(user);
        *common.mutable_modified_at() = NProtoInterop::CastToProto(now);

        *query.mutable_content() = request.content();
        query.mutable_meta()->set_execute_mode(request.execute_mode());

        bool isValidMode = request.execute_mode() == YandexQuery::SAVE ||
            IsIn({
                YandexQuery::QueryMeta::ABORTED_BY_USER,
                YandexQuery::QueryMeta::ABORTED_BY_SYSTEM,
                YandexQuery::QueryMeta::COMPLETED,
                YandexQuery::QueryMeta::FAILED,
                YandexQuery::QueryMeta::PAUSED
            }, query.meta().status());

        if (!isValidMode) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << YandexQuery::QueryMeta::ComputeStatus_Name(query.meta().status()) << " to " << YandexQuery::QueryMeta::ComputeStatus_Name(YandexQuery::QueryMeta::STARTING) << " is not possible. Please wait for the query to complete or stop it";
        }

        if (!Config.Proto.GetDisableCurrentIam()) {
            internal.set_token(token);
        }
        if (request.execute_mode() != YandexQuery::SAVE) {
            if (request.state_load_mode() != YandexQuery::StateLoadMode::STATE_LOAD_MODE_UNSPECIFIED) {
                internal.set_state_load_mode(request.state_load_mode());
            }
            internal.mutable_disposition()->CopyFrom(request.disposition());

            internal.clear_binding();
            internal.clear_connection();

            // TODO: move to run actor priority selection
            TSet<TString> disabledConnections;
            for (const auto& connection: GetEntities<YandexQuery::Connection>(resultSets[resultSets.size() - 3], CONNECTION_COLUMN_NAME)) {
                if (!Config.AvailableConnections.contains(connection.content().setting().connection_case())) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }

                if (GetIamAuth(connection) == YandexQuery::IamAuth::kCurrentIam && Config.Proto.GetDisableCurrentIam()) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
            }

            if (permissions.Check(TPermissions::CONNECTIONS_USE)) {
                auto connections = GetEntitiesWithVisibilityPriority<YandexQuery::Connection>(resultSets[resultSets.size() - 3], CONNECTION_COLUMN_NAME);
                for (const auto& [_, connection]: connections) {
                    if (disabledConnections.contains(connection.meta().id())) {
                        continue;
                    }
                    *internal.add_connection() = connection;
                }
            }

            if (permissions.Check(TPermissions::BINDINGS_USE)) {
                auto bindings = GetEntitiesWithVisibilityPriority<YandexQuery::Binding>(resultSets[resultSets.size() - 2], BINDING_COLUMN_NAME);
                for (const auto& [_, binding]: bindings) {
                    if (!Config.AvailableBindings.contains(binding.content().setting().binding_case())) {
                        continue;
                    }

                    if (disabledConnections.contains(binding.content().connection_id())) {
                        continue;
                    }

                    *internal.add_binding() = binding;
                }
            }
        }

        if (query.ByteSizeLong() > Config.Proto.GetMaxRequestSize()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Query data is not placed in the table. Please shorten your request";
        }

        if (internal.ByteSizeLong() > Config.Proto.GetMaxRequestSize()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Internal data is not placed in the table. Please reduce the number of connections and bindings";
        }

        YandexQuery::Job job;
        const TString jobId = request.execute_mode() == YandexQuery::SAVE ? "" : GetEntityIdAsString(Config.IdsPrefix, EEntityType::JOB);
        if (request.execute_mode() != YandexQuery::SAVE) {
            internal.clear_action();
            query.clear_result_set_meta();
            query.clear_plan();
            query.clear_ast();
            query.clear_issue();
            query.clear_transient_issue();
            query.clear_statistics();
            query.mutable_meta()->clear_started_at();
            query.mutable_meta()->clear_finished_at();
            query.mutable_meta()->set_last_job_query_revision(common.revision());
            query.mutable_meta()->set_last_job_id(jobId);
            query.mutable_meta()->set_status(YandexQuery::QueryMeta::STARTING);
            query.mutable_meta()->clear_expire_at();
            query.mutable_meta()->clear_result_expire_at();
            query.mutable_meta()->set_started_by(user);
            query.mutable_meta()->clear_action();

            auto& jobMeta = *job.mutable_meta();
            jobMeta.set_id(jobId);
            jobMeta.set_created_by(user);
            jobMeta.set_modified_by(user);
            *jobMeta.mutable_modified_at() = NProtoInterop::CastToProto(now);
            *jobMeta.mutable_created_at() = NProtoInterop::CastToProto(now);
            jobMeta.set_revision(InitialRevision);

            job.set_text(request.content().text());
            *job.mutable_query_meta() = query.meta();
            job.set_query_name(query.mutable_content()->name());
            *job.mutable_acl() = request.content().acl();
            job.set_automatic(request.content().automatic());
        }

        response->second.After.ConstructInPlace().CopyFrom(query);
        response->second.CloudId = internal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyQuery(write)");
        writeQueryBuilder.AddString("tenant", AssignTenantName(internal.cloud_id(), scope));
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("query_id", queryId);
        writeQueryBuilder.AddUint64("max_count_jobs", Config.Proto.GetMaxCountJobs());
        writeQueryBuilder.AddInt64("visibility", query.content().acl().visibility());
        writeQueryBuilder.AddString("job_id", jobId);
        writeQueryBuilder.AddString("query", query.SerializeAsString());
        writeQueryBuilder.AddString("internal", internal.SerializeAsString());
        writeQueryBuilder.AddInt64("query_type", query.content().type());
        writeQueryBuilder.AddBool("automatic", query.content().automatic());
        writeQueryBuilder.AddString("name", query.content().name());
        writeQueryBuilder.AddInt64("execute_mode", request.execute_mode());
        writeQueryBuilder.AddInt64("revision", common.revision());
        writeQueryBuilder.AddInt64("status", query.meta().status());
        writeQueryBuilder.AddString("result_id", resultId);

        writeQueryBuilder.AddText(
            "$to_delete = (\n"
                "SELECT * FROM `" JOBS_TABLE_NAME "`\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id\n"
                "ORDER BY `" JOB_ID_COLUMN_NAME "`\n"
                "LIMIT $max_count_jobs, 1\n"
            ");\n"
        );

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config.IdempotencyKeyTtl);

        if (request.content().acl().visibility() == oldVisibility) {
            writeQueryBuilder.AddInt64("visibility", query.content().acl().visibility());
            // move jobs to scope
            writeQueryBuilder.AddText(
                "UPDATE `" JOBS_TABLE_NAME "` SET `" VISIBILITY_COLUMN_NAME "` = $visibility\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        }

        if (request.execute_mode() != YandexQuery::SAVE) {
            writeQueryBuilder.AddString("job", job.SerializeAsString());
            writeQueryBuilder.AddString("user", user);
            writeQueryBuilder.AddTimestamp("zero_timestamp", TInstant::Zero());
            writeQueryBuilder.AddTimestamp("now", TInstant::Now());
            // insert job
            writeQueryBuilder.AddText(
                "UPSERT INTO `" JOBS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`,  `" JOB_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`) VALUES\n"
                "    ($scope, $query_id, $job_id, $job, $user, $visibility);\n"
            );

            // insert pending small
            writeQueryBuilder.AddText(
                "INSERT INTO `" PENDING_SMALL_TABLE_NAME "`\n"
                "   (`" TENANT_COLUMN_NAME "`, `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" LAST_SEEN_AT_COLUMN_NAME "`, `" ASSIGNED_UNTIL_COLUMN_NAME "`, `" RETRY_COUNTER_COLUMN_NAME "`, \n"
                "   `" RETRY_COUNTER_UPDATE_COLUMN_NAME "`, `" QUERY_TYPE_COLUMN_NAME "`, `" HOST_NAME_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "`)\n"
                "VALUES\n"
                "   ($tenant, $scope, $query_id, $zero_timestamp, $zero_timestamp, 0, $now, $query_type, \"\", \"\");\n"
            );
        }

        writeQueryBuilder.AddText(
            "DELETE FROM `" JOBS_TABLE_NAME "` ON\n"
            "SELECT * FROM $to_delete;\n"
            "UPDATE `" QUERIES_TABLE_NAME "` SET \n"
            "   `" LAST_JOB_ID_COLUMN_NAME "` = $job_id, `" VISIBILITY_COLUMN_NAME "` = $visibility, `" AUTOMATIC_COLUMN_NAME "` = $automatic,\n"
            "   `" NAME_COLUMN_NAME "` = $name, `" EXECUTE_MODE_COLUMN_NAME "` = $execute_mode,\n"
            "   `" REVISION_COLUMN_NAME "` = $revision, `" STATUS_COLUMN_NAME "` = $status, "
        );
        writeQueryBuilder.AddText(
            (request.execute_mode() != YandexQuery::SAVE ? "`" INTERNAL_COLUMN_NAME "` = $internal,\n" : TString{"\n"})
        );
        writeQueryBuilder.AddText(
            "   `" QUERY_TYPE_COLUMN_NAME "` = $query_type, `" QUERY_COLUMN_NAME "` = $query,\n"
            "   `" RESULT_ID_COLUMN_NAME "` = $result_id, `" META_REVISION_COLUMN_NAME "` = `" META_REVISION_COLUMN_NAME "` + 1,\n"
            "   `" TENANT_COLUMN_NAME "` = $tenant\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;"
        );

        const auto write = writeQueryBuilder.Build();
        return make_pair(write.Sql, write.Params);
    };

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    auto accessValidator = CreateManageAccessValidator(
        QUERIES_TABLE_NAME,
        QUERY_ID_COLUMN_NAME,
        scope,
        queryId,
        user,
        "Query does not exist or permission denied. Please check the id of the query or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    auto ttlValidator = CreateTtlValidator(
        QUERIES_TABLE_NAME,
        QUERY_ID_COLUMN_NAME,
        scope,
        queryId,
        "Query does not exist or permission denied. Please check the id of the query or your access rights",
        YdbConnection->TablePathPrefix);
    validators.push_back(ttlValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            QUERIES_TABLE_NAME,
            QUERY_ID_COLUMN_NAME,
            scope,
            queryId,
            previousRevision,
            "Revision of the query has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    const auto read = readQueryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), read.Sql, read.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvModifyQueryResponse, YandexQuery::ModifyQueryResult, TAuditDetails<YandexQuery::Query>>(
        "ModifyQueryRequest - ModifyQueryResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(ModifyQueryRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDeleteQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_DELETE_QUERY);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const YandexQuery::DeleteQueryRequest& request = event.Request;
    const TString queryId = request.query_id();
    const int byteSize = request.ByteSize();
    const int64_t previousRevision = request.previous_revision();
    const TString idempotencyKey = request.idempotency_key();
    CPS_LOG_T("DeleteQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("DeleteQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDeleteQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DeleteQueryRequest, scope, queryId, user, delta, byteSize, false);
        return;
    }
    std::shared_ptr<std::pair<YandexQuery::DeleteQueryResult, TAuditDetails<YandexQuery::Query>>> response = std::make_shared<std::pair<YandexQuery::DeleteQueryResult, TAuditDetails<YandexQuery::Query>>>();
    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteQuery");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("query_id", queryId);

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config.IdempotencyKeyTtl);
    queryBuilder.AddText(
        "DELETE FROM `" JOBS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "$tenant = (SELECT `" TENANT_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id);\n"
        "DELETE FROM `" PENDING_SMALL_TABLE_NAME "`\n"
        "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "DELETE FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;"
    );

    TVector<TValidationQuery> validators;

    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    auto accessValidator = CreateManageAccessValidator(
        QUERIES_TABLE_NAME,
        QUERY_ID_COLUMN_NAME,
        scope,
        queryId,
        user,
        "Query does not exist or permission denied. Please check the id of the query or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            QUERIES_TABLE_NAME,
            QUERY_ID_COLUMN_NAME,
            scope,
            queryId,
            previousRevision,
            "Revision of the query has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    validators.push_back(CreateEntityExtractor(
        scope,
        queryId,
        QUERY_COLUMN_NAME,
        QUERY_ID_COLUMN_NAME,
        QUERIES_TABLE_NAME,
        response,
        YdbConnection->TablePathPrefix));

    validators.push_back(CreateQueryComputeStatusValidator(
        { YandexQuery::QueryMeta::STARTING, YandexQuery::QueryMeta::ABORTED_BY_USER, YandexQuery::QueryMeta::ABORTED_BY_SYSTEM, YandexQuery::QueryMeta::COMPLETED, YandexQuery::QueryMeta::FAILED },
        scope,
        queryId,
        "Can't delete running query",
        YdbConnection->TablePathPrefix));

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(NActors::TActivationContext::ActorSystem(), query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvDeleteQueryResponse, YandexQuery::DeleteQueryResult, TAuditDetails<YandexQuery::Query>>(
        "DeleteQueryRequest - DeleteQueryResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(DeleteQueryRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvControlQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvControlQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_CONTROL_QUERY);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::ControlQueryRequest& request = event.Request;
    const TString user = event.User;
    const TString queryId = request.query_id();
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                    ? event.Permissions
                    : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int byteSize = request.ByteSize();
    const int64_t previousRevision = request.previous_revision();
    const TString idempotencyKey = request.idempotency_key();
    const YandexQuery::QueryAction action = request.action();
    CPS_LOG_T("ControlQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("ControlQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvControlQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ControlQueryRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<YandexQuery::ControlQueryResult, TAuditDetails<YandexQuery::Query>>> response = std::make_shared<std::pair<YandexQuery::ControlQueryResult, TAuditDetails<YandexQuery::Query>>>();

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ControlQuery(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("query_id", queryId);

    readQueryBuilder.AddText(
        "$selected = SELECT `" QUERY_COLUMN_NAME "`, `" LAST_JOB_ID_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`, `" TENANT_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "$job_id = SELECT `" LAST_JOB_ID_COLUMN_NAME "` FROM $selected;\n"
        "SELECT * FROM $selected;"
        "SELECT `" JOB_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" JOB_ID_COLUMN_NAME "` = $job_id;\n"
    );

    auto prepareParams = [=, config=Config](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 2) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 2 but equal " << resultSets.size() << ". Please contact internal support";
        }

        auto now = TInstant::Now();
        YandexQuery::Query query;
        YandexQuery::Internal::QueryInternal queryInternal;
        TString tenantName;
        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }

            if (!queryInternal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
            }
            tenantName = *parser.ColumnParser(TENANT_COLUMN_NAME).GetOptionalString();
        }

        YandexQuery::Job job;
        TString jobId;
        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the id query or your access rights";
            }

            if (!job.ParseFromString(parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString().GetOrElse(""))) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
            }

            jobId = job.meta().id();

            const bool hasManageAccess = HasManageAccess(permissions, query.content().acl().visibility(), query.meta().common().created_by(), user);
            if (!hasManageAccess) {
                ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }
        }

        queryInternal.set_action(action);

        auto& metaQuery = *query.mutable_meta();
        auto& commonQuery = *metaQuery.mutable_common();
        commonQuery.set_revision(commonQuery.revision() + 1);
        commonQuery.set_modified_by(user);
        *commonQuery.mutable_modified_at() = NProtoInterop::CastToProto(now);

        if (action == YandexQuery::ABORT || action == YandexQuery::ABORT_GRACEFULLY) {
            const bool isValidStatusForAbort = IsIn({
                YandexQuery::QueryMeta::STARTING,
                YandexQuery::QueryMeta::RESUMING,
                YandexQuery::QueryMeta::RUNNING,
                YandexQuery::QueryMeta::PAUSING
            }, metaQuery.status());

            if (isValidStatusForAbort) {
                metaQuery.set_status(YandexQuery::QueryMeta::ABORTING_BY_USER);
                metaQuery.set_aborted_by(user);
            } else {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Conversion from status " << YandexQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << YandexQuery::QueryMeta::ComputeStatus_Name(YandexQuery::QueryMeta::ABORTING_BY_USER) << " is not possible. Please wait for the previous operation to be completed";
            }
        }

        if (action == YandexQuery::PAUSE || action == YandexQuery::PAUSE_GRACEFULLY) {
            const bool isValidStatusForPause = IsIn({
                YandexQuery::QueryMeta::RESUMING,
                YandexQuery::QueryMeta::RUNNING
            }, metaQuery.status());

            if (isValidStatusForPause) {
                metaQuery.set_status(YandexQuery::QueryMeta::PAUSING);
                metaQuery.set_paused_by(user);
            } else {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << YandexQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << YandexQuery::QueryMeta::ComputeStatus_Name(YandexQuery::QueryMeta::PAUSING) << " is not possible. Please wait for the previous operation to be completed";
            }
        }

        if (action == YandexQuery::RESUME) {
            const bool isValidStatusForResume = metaQuery.status() == YandexQuery::QueryMeta::PAUSED;
            if (isValidStatusForResume) {
                metaQuery.set_status(YandexQuery::QueryMeta::RESUMING);
            } else {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << YandexQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << YandexQuery::QueryMeta::ComputeStatus_Name(YandexQuery::QueryMeta::RESUMING) << " is not possible. Please wait for the previous operation to be completed";
            }
        }

        job.mutable_query_meta()->CopyFrom(metaQuery);
        job.mutable_meta()->set_revision(job.meta().revision() + 1);
        job.mutable_meta()->set_modified_by(user);
        *job.mutable_meta()->mutable_modified_at() = NProtoInterop::CastToProto(now);

        response->second.After.ConstructInPlace().CopyFrom(query);
        response->second.CloudId = queryInternal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ControlQuery(write)");
        writeQueryBuilder.AddString("tenant", tenantName);
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("job", job.SerializeAsString());
        writeQueryBuilder.AddString("job_id", jobId);
        writeQueryBuilder.AddString("query_id", queryId);
        writeQueryBuilder.AddString("query", query.SerializeAsString());
        writeQueryBuilder.AddInt64("revision", query.meta().common().revision());
        writeQueryBuilder.AddInt64("status", query.meta().status());
        writeQueryBuilder.AddString("internal", queryInternal.SerializeAsString());

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), now + Config.IdempotencyKeyTtl);
        writeQueryBuilder.AddText(
            "UPDATE `" JOBS_TABLE_NAME "` SET `" JOB_COLUMN_NAME "` = $job\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" JOB_ID_COLUMN_NAME "` = $job_id;\n"
            "UPDATE `" QUERIES_TABLE_NAME "` SET `" QUERY_COLUMN_NAME "` = $query, `" REVISION_COLUMN_NAME "` = $revision, `" STATUS_COLUMN_NAME "` = $status, `" INTERNAL_COLUMN_NAME "` = $internal, `" META_REVISION_COLUMN_NAME "` = `" META_REVISION_COLUMN_NAME "` + 1\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" HOST_NAME_COLUMN_NAME "` = \"\"\n"
            "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;"
        );

        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    auto accessValidator = CreateManageAccessValidator(
        QUERIES_TABLE_NAME,
        QUERY_ID_COLUMN_NAME,
        scope,
        queryId,
        user,
        "Query does not exist or permission denied. Please check the id of the query or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            QUERIES_TABLE_NAME,
            QUERY_ID_COLUMN_NAME,
            scope,
            queryId,
            previousRevision,
            "Revision of the query has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    const auto readQuery = readQueryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvControlQueryResponse, YandexQuery::ControlQueryResult, TAuditDetails<YandexQuery::Query>>(
        "ControlQueryRequest - ControlQueryRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(ControlQueryRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvGetResultDataRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvGetResultDataRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_GET_RESULT_DATA);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());

    const YandexQuery::GetResultDataRequest& request = event.Request;
    const TString user = event.User;
    const int32_t resultSetIndex = request.result_set_index();
    const int64_t offset = request.offset();
    const TString queryId = request.query_id();
    const int byteSize = event.Request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                ? event.Permissions
                : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int64_t limit = request.limit();
    CPS_LOG_T("GetResultDataRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("GetResultDataRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvGetResultDataResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(GetResultDataRequest, scope, user, queryId, resultSetIndex, offset, limit, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "GetResultData");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("query_id", queryId);
    queryBuilder.AddTimestamp("now", TInstant::Now());
    queryBuilder.AddInt64("offset", offset);
    queryBuilder.AddInt32("result_set_index", resultSetIndex);
    queryBuilder.AddUint64("limit", limit);

    queryBuilder.AddText(
        "$query_info = SELECT `" RESULT_ID_COLUMN_NAME "`, `" RESULT_SETS_EXPIRE_AT_COLUMN_NAME "`, `" QUERY_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" STATUS_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);\n"
        "SELECT `" QUERY_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" STATUS_COLUMN_NAME "`, `" RESULT_SETS_EXPIRE_AT_COLUMN_NAME "` FROM $query_info;\n"
        "$result_id = SELECT `" RESULT_ID_COLUMN_NAME "` FROM $query_info\n"
        "WHERE `" RESULT_SETS_EXPIRE_AT_COLUMN_NAME "` >= $now;\n"
        "SELECT `" RESULT_SET_ID_COLUMN_NAME "`, `" RESULT_SET_COLUMN_NAME "`, `" ROW_ID_COLUMN_NAME "` FROM `" RESULT_SETS_TABLE_NAME "`\n"
        "WHERE `" RESULT_ID_COLUMN_NAME "` = $result_id AND `" RESULT_SET_ID_COLUMN_NAME "` = $result_set_index AND `" ROW_ID_COLUMN_NAME "` >= $offset\n"
        "ORDER BY `" ROW_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;\n"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, resultSetIndex, user, permissions] {
        if (resultSets->size() != 2) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 2 but equal " << resultSets->size() << ". Please contact internal support";
        }

        YandexQuery::GetResultDataResult result;
        auto& resultSetProto = *result.mutable_result_set();
        {
            const auto& resultSet = (*resultSets)[0];
            TResultSetParser parser(resultSet);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            YandexQuery::Query query;
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }

            YandexQuery::Acl::Visibility queryVisibility = static_cast<YandexQuery::Acl::Visibility>(parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64().GetOrElse(YandexQuery::Acl::VISIBILITY_UNSPECIFIED));
            TString queryUser = parser.ColumnParser(USER_COLUMN_NAME).GetOptionalString().GetOrElse("");

            bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
            if (!hasViewAccess) {
                ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            if (resultSetIndex >= query.result_set_meta_size()) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Result set index out of bound: " << resultSetIndex << " >= " << query.result_set_meta_size();
            }

            if (YandexQuery::QueryMeta::ComputeStatus(*parser.ColumnParser(STATUS_COLUMN_NAME).GetOptionalInt64()) != YandexQuery::QueryMeta::COMPLETED) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Result doesn't exist";
            }

            auto resultSetsExpireAtParser = parser.ColumnParser(RESULT_SETS_EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp();
            if (!resultSetsExpireAtParser) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Result doesn't exist";
            }

            if (*resultSetsExpireAtParser < TInstant::Now()) {
                ythrow TControlPlaneStorageException(TIssuesIds::EXPIRED) << "Result removed by TTL";
            }

            resultSetProto.mutable_columns()->CopyFrom(query.result_set_meta(resultSetIndex).column());
        }

        {
            const auto& resultSet = (*resultSets)[1];
            TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                if (!resultSetProto.add_rows()->ParseFromString(*parser.ColumnParser(RESULT_SET_COLUMN_NAME).GetOptionalString())) {
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for row. Please contact internal support";
                }
            }
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvGetResultDataResponse, YandexQuery::GetResultDataResult>(
        "GetResultDataRequest - GetResultDataResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(GetResultDataRequest, scope, user, queryId, resultSetIndex, offset, limit, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvListJobsRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvListJobsRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_LIST_JOBS_DATA);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::ListJobsRequest& request = event.Request;
    const TString user = event.User;
    TString queryId = request.query_id(); // TODO: remove it
    if (request.has_filter() && request.filter().query_id()) {
        queryId = request.filter().query_id();
    }

    auto  splittedPageToken = SplitId(request.page_token());
    const auto jobIdToken = std::move(splittedPageToken.first);
    const auto queryIdToken = std::move(splittedPageToken.second);

    const int byteSize = request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
            ? event.Permissions
            : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int64_t limit = request.limit();
    CPS_LOG_T("ListJobsRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("ListJobsRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvListJobsResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ListJobsRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ListJobs");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("last_job", jobIdToken);
    queryBuilder.AddString("last_query", queryIdToken);
    queryBuilder.AddTimestamp("now", TInstant::Now());
    queryBuilder.AddUint64("limit", limit + 1);
    queryBuilder.AddText(
        "SELECT `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` >= $last_query\n"
        "AND `" JOB_ID_COLUMN_NAME "` >= $last_job AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now) "
    );

    TString filter;
    if (request.has_filter() || queryId) {
        TVector<TString> filters;
        if (queryId) {
            queryBuilder.AddString("query_id", queryId);
            filters.push_back("`" QUERY_ID_COLUMN_NAME "` = $query_id");
        }

        if (request.filter().created_by_me()) {
            queryBuilder.AddString("user", user);
            filters.push_back("`" USER_COLUMN_NAME "` = $user");
        }

        filter = JoinSeq(" AND ", filters);
    }

    PrepareViewAccessCondition(queryBuilder, permissions, user);

    if (filter) {
        queryBuilder.AddText(" AND (" + filter + ")\n");
    }

    queryBuilder.AddText(
        "ORDER BY `" JOB_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        YandexQuery::ListJobsResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            YandexQuery::Job job;
            if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
            }
            const TString mergedId = job.meta().id() + "-" + job.query_meta().common().id();
            job.mutable_meta()->set_id(mergedId);
            job.mutable_query_meta()->set_last_job_id(mergedId);
            YandexQuery::BriefJob briefJob;
            *briefJob.mutable_meta() = job.meta();
            *briefJob.mutable_query_meta() = job.query_meta();
            briefJob.set_query_name(job.query_name());
            briefJob.set_visibility(job.acl().visibility());
            briefJob.set_automatic(job.automatic());
            *briefJob.mutable_expire_at() = job.expire_at();
            *result.add_job() = briefJob;
        }

        if (result.job_size() == limit + 1) {
            result.set_next_page_token(
                result.job(result.job_size() - 1).meta().id() + "-" + result.job(result.job_size() - 1).query_meta().common().id());
            result.mutable_job()->RemoveLast();
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvListJobsResponse, YandexQuery::ListJobsResult>(
        "ListJobsRequest - ListJobsResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(ListJobsRequest, scope, user, queryId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDescribeJobRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_DESCRIBE_JOB);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::DescribeJobRequest& request = event.Request;

    const TString user = event.User;
    auto splittedId = SplitId(request.job_id());
    const auto jobId = std::move(splittedId.first);
    const auto queryId = std::move(splittedId.second);

    const int byteSize = request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    CPS_LOG_T("DescribeJobRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("DescribeJobRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDescribeJobResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DescribeJobRequest, scope, user, jobId, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeJob");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("job_id", jobId);
    queryBuilder.AddString("query_id", queryId);
    queryBuilder.AddTimestamp("now", TInstant::Now());

    queryBuilder.AddText(
        "SELECT `" JOB_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id\n"
        " AND `" JOB_ID_COLUMN_NAME "` = $job_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);\n"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);

    auto prepare = [=, id=request.job_id(), resultSets=resultSets] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        YandexQuery::DescribeJobResult result;
        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the job id or your access rights";
        }
        if (!result.mutable_job()->ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
        }
        auto visibility = static_cast<YandexQuery::Acl::Visibility>(*parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64());
        result.mutable_job()->mutable_meta()->set_id(id);

        bool hasViewAccces = HasViewAccess(permissions, visibility, result.job().meta().created_by(), user);
        if (!hasViewAccces) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the job id or your access rights";
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeJobResponse, YandexQuery::DescribeJobResult>(
        "DescribeJobRequest - DescribeJobResult",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(DescribeJobRequest, scope, user, jobId, delta, byteSize, future.GetValue());
        });
}

} // NYq
