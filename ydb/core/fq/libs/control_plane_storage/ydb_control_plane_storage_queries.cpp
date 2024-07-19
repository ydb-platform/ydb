#include "ydb_control_plane_storage_impl.h"
#include "request_actor.h"

#include <cstdint>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/core/fq/libs/shared_resources/db_exec.h>

#include <util/digest/multi.h>

namespace {

constexpr ui64 GRPC_MESSAGE_SIZE_LIMIT = 64000000;

FederatedQuery::IamAuth::IdentityCase GetIamAuth(const FederatedQuery::Connection& connection) {
    const auto& setting = connection.content().setting();
    switch (setting.connection_case()) {
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            return setting.ydb_database().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            return setting.clickhouse_cluster().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kObjectStorage:
            return setting.object_storage().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kDataStreams:
            return setting.data_streams().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kMonitoring:
            return setting.monitoring().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            return setting.postgresql_cluster().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            return setting.greenplum_cluster().auth().identity_case();
        case FederatedQuery::ConnectionSetting::kMysqlCluster:
            return setting.mysql_cluster().auth().identity_case();
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return FederatedQuery::IamAuth::IDENTITY_NOT_SET;
    }
}

}

namespace NFq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateQueryRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateQueryRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const FederatedQuery::CreateQueryRequest& request = event.Request;
    const FederatedQuery::Internal::ComputeDatabaseInternal& computeDatabase = event.ComputeDatabase;
    ui64 resultLimit = 0;
    if (event.Quotas) {
        if (auto it = event.Quotas->find(QUOTA_QUERY_RESULT_LIMIT); it != event.Quotas->end()) {
            resultLimit = it->second.Limit.Value;
        }
    }
    auto queryType = request.content().type();
    ui64 executionLimitMills = GetExecutionLimitMills(queryType, event.Quotas);
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_QUERY, RTC_CREATE_QUERY);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                                ? event.Permissions
                                : TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const size_t byteSize = request.ByteSizeLong();
    const TString queryId = GetEntityIdAsString(Config->IdsPrefix, EEntityType::QUERY);
    CPS_LOG_T("CreateQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateQuery(ev);
    if (request.execute_mode() != FederatedQuery::SAVE && !permissions.Check(TPermissions::QUERY_INVOKE)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke"));
    }

    if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (request.disposition().has_from_last_checkpoint()) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Streaming disposition \"from_last_checkpoint\" is not allowed in CreateQuery request"));
    }

    auto tenant = ev->Get()->TenantInfo->Assign(cloudId, scope, request.content().type(), TenantName);

    if (event.Quotas) {
        TQuotaMap::const_iterator it = event.Quotas->end();
        if (queryType == FederatedQuery::QueryContent::ANALYTICS) {
            it = event.Quotas->find(QUOTA_ANALYTICS_COUNT_LIMIT);
        } else if (queryType == FederatedQuery::QueryContent::STREAMING) {
            it = event.Quotas->find(QUOTA_STREAMING_COUNT_LIMIT);
        }
        if (it != event.Quotas->end()) {
            auto& quota = it->second;
            if (!quota.Usage) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::NOT_READY, "Control Plane is not ready yet. Please retry later."));
            } else if (quota.Usage->Value >= quota.Limit.Value) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::QUOTA_EXCEEDED, Sprintf("Too many queries (%lu of %lu). Please delete other queries or increase limits.", quota.Usage->Value, quota.Limit.Value)));
            }
        }
    }

    if (request.content().limits().vcpu_rate_limit() < 0) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "VCPU rate limit can't be less than zero"));
    }

    if (issues) {
        CPS_LOG_W("CreateQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(CreateQueryRequest, scope, user, delta, byteSize, false);
        return;
    }

    const TString idempotencyKey = request.idempotency_key();
    const TString jobId = request.execute_mode() == FederatedQuery::SAVE ? "" : GetEntityIdAsString(Config->IdsPrefix, EEntityType::JOB);

    FederatedQuery::Query query;
    FederatedQuery::QueryContent& content = *query.mutable_content() = request.content();
    FederatedQuery::QueryMeta& meta = *query.mutable_meta();
    FederatedQuery::CommonMeta& common = *meta.mutable_common() = CreateCommonMeta(queryId, user, startTime, InitialRevision);
    meta.set_execute_mode(request.execute_mode());
    meta.set_status(request.execute_mode() == FederatedQuery::SAVE ? FederatedQuery::QueryMeta::COMPLETED : FederatedQuery::QueryMeta::STARTING);

    FederatedQuery::Job job;
    if (request.execute_mode() != FederatedQuery::SAVE) {
        meta.set_last_job_query_revision(InitialRevision);
        meta.set_last_job_id(jobId);
        meta.set_started_by(user);
        *meta.mutable_submitted_at() = NProtoInterop::CastToProto(startTime);
        *job.mutable_meta() = common;
        job.mutable_meta()->set_id(jobId);
        job.set_text(content.text());
        *job.mutable_query_meta() = meta;
        job.set_query_name(query.mutable_content()->name());
        *job.mutable_acl() = content.acl();
        job.set_automatic(content.automatic());
        *job.mutable_parameters() = content.parameters();
    }

    std::shared_ptr<std::pair<FederatedQuery::CreateQueryResult, TAuditDetails<FederatedQuery::Query>>> response = std::make_shared<std::pair<FederatedQuery::CreateQueryResult, TAuditDetails<FederatedQuery::Query>>>();
    response->first.set_query_id(queryId);
    response->second.CloudId = cloudId;

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "CreateQuery(read)");
    ReadIdempotencyKeyQuery(readQueryBuilder, scope, idempotencyKey);

    if (request.execute_mode() != FederatedQuery::SAVE) {
        readQueryBuilder.AddString("scope", scope);
        readQueryBuilder.AddString("user", user);
        readQueryBuilder.AddInt64("scope_visibility", FederatedQuery::Acl::SCOPE);

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

    auto prepareParams = [=, as=TActivationContext::ActorSystem(), commonCounters=requestCounters.Common](const TVector<TResultSet>& resultSets) mutable {
        const size_t countSets = (idempotencyKey ? 1 : 0) + (request.execute_mode() != FederatedQuery::SAVE ? 2 : 0);
        if (resultSets.size() != countSets) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to " << countSets << " but equal " << resultSets.size() << ". Please contact internal support";
        }

        if (idempotencyKey) {
            TResultSetParser parser(resultSets.front());
            if (parser.TryNextRow()) {
                if (!response->first.ParseFromString(*parser.ColumnParser(RESPONSE_COLUMN_NAME).GetOptionalString())) {
                    commonCounters->ParseProtobufError->Inc();
                    ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for idempotency key request. Please contact internal support";
                }
                response->second.IdempotencyResult = true;
                return make_pair(TString{}, TParamsBuilder{}.Build());
            }
        }

        FederatedQuery::Internal::QueryInternal queryInternal;
        if (!Config->Proto.GetDisableCurrentIam()) {
            queryInternal.set_token(token);
        }

        queryInternal.set_cloud_id(cloudId);
        queryInternal.set_state_load_mode(FederatedQuery::StateLoadMode::EMPTY);
        queryInternal.mutable_disposition()->CopyFrom(request.disposition());
        queryInternal.set_result_limit(resultLimit);
        *queryInternal.mutable_execution_ttl() = NProtoInterop::CastToProto(TDuration::MilliSeconds(executionLimitMills));

        if (request.execute_mode() != FederatedQuery::SAVE) {
            // TODO: move to run actor priority selection
            *queryInternal.mutable_compute_connection() = computeDatabase.connection();
            TSet<TString> disabledConnections;
            for (const auto& connection: GetEntities<FederatedQuery::Connection>(resultSets[resultSets.size() - 2], CONNECTION_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters)) {
                auto connectionCase = connection.content().setting().connection_case();
                if (!Config->AvailableConnections.contains(connectionCase)) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
                if ((queryType == FederatedQuery::QueryContent::STREAMING) && !Config->AvailableStreamingConnections.contains(connectionCase)) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }

                if (GetIamAuth(connection) == FederatedQuery::IamAuth::kCurrentIam && Config->Proto.GetDisableCurrentIam()) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
            }

            TSet<TString> connectionIds;
            auto connections = GetEntitiesWithVisibilityPriority<FederatedQuery::Connection>(resultSets[resultSets.size() - 2], CONNECTION_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters);
            for (const auto& [_, connection]: connections) {
                if (disabledConnections.contains(connection.meta().id())) {
                    continue;
                }
                *queryInternal.add_connection() = connection;
                connectionIds.insert(connection.meta().id());
            }

            auto bindings = GetEntitiesWithVisibilityPriority<FederatedQuery::Binding>(resultSets[resultSets.size() - 1], BINDING_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters);
            for (const auto& [_, binding]: bindings) {
                if (!Config->AvailableBindings.contains(binding.content().setting().binding_case())) {
                    continue;
                }

                if (disabledConnections.contains(binding.content().connection_id())) {
                    continue;
                }

                *queryInternal.add_binding() = binding;
                if (!connectionIds.contains(binding.content().connection_id())) {
                    ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Unable to resolve connection for binding " << binding.meta().id() << ", name " << binding.content().name() << ", connection id " << binding.content().connection_id();
                }
            }
        }

        if (query.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Incoming request exceeded the size limit: " << query.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() <<  ". Please shorten your request";
        }

        if (queryInternal.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "The size of all connections and bindings in the project exceeded the limit: " << queryInternal.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() << ". Please reduce the number of connections and bindings";
        }

        response->second.After.ConstructInPlace().CopyFrom(query);

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "CreateQuery(write)");
        writeQueryBuilder.AddString("tenant", tenant);
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

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config->IdempotencyKeyTtl);

        if (request.execute_mode() != FederatedQuery::SAVE) {
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
                "`" RETRY_RATE_COLUMN_NAME "`, `" RETRY_COUNTER_COLUMN_NAME "`, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "`, `" HOST_NAME_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "`)\n"
                "VALUES\n"
                "    ($tenant, $scope, $query_id, $query_type, $zero_timestamp, $zero_timestamp, 0, 0, $now, \"\", \"\");"
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus status = ReadModifyWrite(read.Sql, read.Params, prepareParams, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvCreateQueryResponse, FederatedQuery::CreateQueryResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_QUERIES, RTC_LIST_QUERIES);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());

    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    const FederatedQuery::ListQueriesRequest& request = event.Request;
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
        "SELECT `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" QUERY_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` >= $last_query AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now)"
    );

    TString filter;
    if (request.has_filter()) {
        TVector<TString> filters;
        if (request.filter().name()) {
            queryBuilder.AddString("filter_name", request.filter().name());
            filters.push_back("Re2::Grep($filter_name, Re2::Options(false AS CaseSensitive, true as Literal))(`" NAME_COLUMN_NAME "`)");
        }

        if (request.filter().query_type() != FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
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

        if (request.filter().visibility() != FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
            queryBuilder.AddInt64("filter_visibility", request.filter().visibility());
            filters.push_back("`" VISIBILITY_COLUMN_NAME "` = $filter_visibility");
        }

        switch (request.filter().automatic()) {
        case FederatedQuery::AUTOMATIC:
            filters.push_back("`" AUTOMATIC_COLUMN_NAME "` = true");
            break;
        case FederatedQuery::NOT_AUTOMATIC:
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
        "ORDER BY `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;"
    );

    const auto read = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(read.Sql, read.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::ListQueriesResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            FederatedQuery::Query query;
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }
            FederatedQuery::BriefQuery briefQuery;
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

    auto success = SendResponse<TEvControlPlaneStorage::TEvListQueriesResponse, FederatedQuery::ListQueriesResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_QUERY, RTC_DESCRIBE_QUERY);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_AST | TPermissions::VIEW_QUERY_TEXT};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    const FederatedQuery::DescribeQueryRequest& request = event.Request;
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
        "SELECT `" QUERY_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND (`" EXPIRE_AT_COLUMN_NAME "` is NULL OR `" EXPIRE_AT_COLUMN_NAME "` > $now);"
    );
    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, user, permissions, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        FederatedQuery::DescribeQueryResult result;
        if (!result.mutable_query()->ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
        }

        const auto lastJobId = result.query().meta().last_job_id();
        result.mutable_query()->mutable_meta()->set_last_job_id(lastJobId + "-" + result.query().meta().common().id());

        const auto queryVisibility = result.query().content().acl().visibility();
        const auto queryUser = result.query().meta().common().created_by();
        const bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
        if (!hasViewAccess) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        FederatedQuery::Internal::QueryInternal internal;
        if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
        }

        // decompress plan
        if (internal.plan_compressed().data()) { // todo: remove this if after migration
            TCompressor compressor(internal.plan_compressed().method());
            result.mutable_query()->mutable_plan()->set_json(compressor.Decompress(internal.plan_compressed().data()));
            if (result.query().ByteSizeLong() > GRPC_MESSAGE_SIZE_LIMIT) {
                if (result.query().plan().json().size() > 1000) {
                    // modifing plan this way should definitely reduce query msg size
                    result.mutable_query()->mutable_plan()->set_json(TStringBuilder() << "Message is too big: " << result.query().ByteSizeLong() << " bytes, dropping plan of size " << result.query().plan().json().size() << " bytes");
                }
            }
        }
        if (!permissions.Check(TPermissions::VIEW_AST)) {
            result.mutable_query()->clear_ast();
        } else {
            // decompress AST
            if (internal.ast_compressed().data()) { // todo: remove this if after migration
                TCompressor compressor(internal.ast_compressed().method());
                result.mutable_query()->mutable_ast()->set_data(compressor.Decompress(internal.ast_compressed().data()));
            }
            if (result.query().ByteSizeLong() > GRPC_MESSAGE_SIZE_LIMIT) {
                if (result.query().ast().data().size() > 1000) {
                    // modifing AST this way should definitely reduce query msg size
                    result.mutable_query()->mutable_ast()->set_data(TStringBuilder() << "Message is too big: " << result.query().ByteSizeLong() << " bytes, dropping AST of size " << result.query().ast().data().size() << " bytes");
                }
            }
        }
        if (!permissions.Check(TPermissions::VIEW_QUERY_TEXT)) {
            result.mutable_query()->mutable_content()->clear_text();
        }

        if (result.query().ByteSizeLong() > GRPC_MESSAGE_SIZE_LIMIT) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Resulting query of size " << result.query().ByteSizeLong() << " bytes is too big";
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeQueryResponse, FederatedQuery::DescribeQueryResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_QUERY_STATUS, RTC_GET_QUERY_STATUS);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_AST | TPermissions::VIEW_QUERY_TEXT};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const FederatedQuery::GetQueryStatusRequest& request = event.Request;
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(read.Sql, read.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, user,permissions] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        FederatedQuery::GetQueryStatusResult result;
        result.set_status(static_cast<FederatedQuery::QueryMeta_ComputeStatus>(*parser.ColumnParser(STATUS_COLUMN_NAME).GetOptionalInt64()));
        result.set_meta_revision(parser.ColumnParser(META_REVISION_COLUMN_NAME).GetOptionalInt64().GetOrElse(0));

        const auto queryVisibility = static_cast<FederatedQuery::Acl::Visibility>(*parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64());
        const auto queryUser = *parser.ColumnParser(USER_COLUMN_NAME).GetOptionalString();
        const bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
        if (!hasViewAccess) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvGetQueryStatusResponse, FederatedQuery::GetQueryStatusResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_QUERY, RTC_MODIFY_QUERY);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                            ? event.Permissions
                            : TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    FederatedQuery::ModifyQueryRequest& request = event.Request;
    FederatedQuery::Internal::ComputeDatabaseInternal& computeDatabase = event.ComputeDatabase;
    const TString queryId = request.query_id();
    const int byteSize = request.ByteSize();
    const int64_t previousRevision = request.previous_revision();
    CPS_LOG_T("ModifyQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    if (request.content().type() == FederatedQuery::QueryContent::STREAMING && request.state_load_mode() == FederatedQuery::STATE_LOAD_MODE_UNSPECIFIED) {
        request.set_state_load_mode(FederatedQuery::EMPTY);
    }

    NYql::TIssues issues = ValidateQuery(ev);
    if (request.execute_mode() != FederatedQuery::SAVE && !permissions.Check(TPermissions::QUERY_INVOKE)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke"));
    }

    if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (request.state_load_mode() == FederatedQuery::FROM_LAST_CHECKPOINT) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::UNSUPPORTED, "State load mode \"FROM_LAST_CHECKPOINT\" is not supported"));
    }

    auto tenant = ev->Get()->TenantInfo->Assign(cloudId, scope, request.content().type(), TenantName);

    if (issues) {
        CPS_LOG_W("ModifyQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvModifyQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ModifyQueryRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    ui64 executionLimitMills = GetExecutionLimitMills(request.content().type(), event.Quotas);

    const TString idempotencyKey = request.idempotency_key();

    std::shared_ptr<std::pair<FederatedQuery::ModifyQueryResult, TAuditDetails<FederatedQuery::Query>>> response =
        std::make_shared<std::pair<FederatedQuery::ModifyQueryResult, TAuditDetails<FederatedQuery::Query>>>();

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ModifyQuery(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("query_id", queryId);
    readQueryBuilder.AddTimestamp("now", TInstant::Now());

    if (request.execute_mode() != FederatedQuery::SAVE) {
        readQueryBuilder.AddString("user", user);
        readQueryBuilder.AddInt64("scope_visibility", FederatedQuery::Acl::SCOPE);
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

    auto prepareParams = [=, config=Config, commonCounters=requestCounters.Common](const TVector<TResultSet>& resultSets) {
        const size_t countSets = 1 + (request.execute_mode() != FederatedQuery::SAVE ? 2 : 0);

        if (resultSets.size() != countSets) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to " << countSets << " but equal " << resultSets.size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets.back());

        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        FederatedQuery::Query query;
        if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
        }

        FederatedQuery::Internal::QueryInternal internal;
        if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
        }
        *internal.mutable_execution_ttl() = NProtoInterop::CastToProto(TDuration::MilliSeconds(executionLimitMills));

        const TString resultId = request.execute_mode() == FederatedQuery::SAVE ? parser.ColumnParser(RESULT_ID_COLUMN_NAME).GetOptionalString().GetOrElse("") : "";

        const auto queryVisibility = query.content().acl().visibility();
        const auto queryUser = query.meta().common().created_by();
        const bool hasManageAccess = HasManageAccess(permissions, queryVisibility, queryUser, user);
        if (!hasManageAccess) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
        }

        if (query.content().type() != request.content().type()) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Query type cannot be changed. Please specify " << FederatedQuery::QueryContent_QueryType_Name(query.content().type()) << " instead of " << FederatedQuery::QueryContent_QueryType_Name(request.content().type());
        }

        if (query.content().acl().visibility() == FederatedQuery::Acl::SCOPE && request.content().acl().visibility() == FederatedQuery::Acl::PRIVATE) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new query with visibility PRIVATE";
        }

        auto oldVisibility = query.content().acl().visibility();

        auto now = TInstant::Now();
        auto& common = *query.mutable_meta()->mutable_common();
        common.set_revision(common.revision() + 1);
        common.set_modified_by(user);
        *common.mutable_modified_at() = NProtoInterop::CastToProto(now);

        *query.mutable_content() = request.content();
        query.mutable_meta()->set_execute_mode(request.execute_mode());

        bool isValidMode = request.execute_mode() == FederatedQuery::SAVE ||
            IsIn({
                FederatedQuery::QueryMeta::ABORTED_BY_USER,
                FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM,
                FederatedQuery::QueryMeta::COMPLETED,
                FederatedQuery::QueryMeta::FAILED,
                FederatedQuery::QueryMeta::PAUSED
            }, query.meta().status());

        if (!isValidMode) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << FederatedQuery::QueryMeta::ComputeStatus_Name(query.meta().status()) << " to " << FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::STARTING) << " is not possible. Please wait for the query to complete or stop it";
        }

        if (!Config->Proto.GetDisableCurrentIam()) {
            internal.set_token(token);
        }
        if (request.execute_mode() != FederatedQuery::SAVE) {
            if (request.state_load_mode() != FederatedQuery::StateLoadMode::STATE_LOAD_MODE_UNSPECIFIED) {
                internal.set_state_load_mode(request.state_load_mode());
            }
            internal.mutable_disposition()->CopyFrom(request.disposition());

            internal.clear_binding();
            internal.clear_connection();
            internal.clear_resources();
            *internal.mutable_compute_connection() = computeDatabase.connection();

            // TODO: move to run actor priority selection
            TSet<TString> disabledConnections;
            for (const auto& connection: GetEntities<FederatedQuery::Connection>(resultSets[resultSets.size() - 3], CONNECTION_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters)) {
                auto connectionCase = connection.content().setting().connection_case();
                if (!Config->AvailableConnections.contains(connectionCase)) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
                if ((query.content().type() == FederatedQuery::QueryContent::STREAMING) && !Config->AvailableStreamingConnections.contains(connectionCase)) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }

                if (GetIamAuth(connection) == FederatedQuery::IamAuth::kCurrentIam && Config->Proto.GetDisableCurrentIam()) {
                    disabledConnections.insert(connection.meta().id());
                    continue;
                }
            }

            TSet<TString> connectionIds;
            auto connections = GetEntitiesWithVisibilityPriority<FederatedQuery::Connection>(resultSets[resultSets.size() - 3], CONNECTION_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters);
            for (const auto& [_, connection]: connections) {
                if (disabledConnections.contains(connection.meta().id())) {
                    continue;
                }
                *internal.add_connection() = connection;
                connectionIds.insert(connection.meta().id());
            }

            auto bindings = GetEntitiesWithVisibilityPriority<FederatedQuery::Binding>(resultSets[resultSets.size() - 2], BINDING_COLUMN_NAME, Config->Proto.GetIgnorePrivateSources(), commonCounters);
            for (const auto& [_, binding]: bindings) {
                if (!Config->AvailableBindings.contains(binding.content().setting().binding_case())) {
                    continue;
                }

                if (disabledConnections.contains(binding.content().connection_id())) {
                    continue;
                }

                *internal.add_binding() = binding;
                if (!connectionIds.contains(binding.content().connection_id())) {
                    ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Unable to resolve connection for binding " << binding.meta().id() << ", name " << binding.content().name() << ", connection id " << binding.content().connection_id();
                }
            }
        }

        if (query.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Incoming request exceeded the size limit: " << query.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() <<  ". Please shorten your request";
        }

        if (internal.ByteSizeLong() > Config->Proto.GetMaxRequestSize()) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "The size of all connections and bindings in the project exceeded the limit: " << internal.ByteSizeLong() << " of " << Config->Proto.GetMaxRequestSize() << ". Please reduce the number of connections and bindings";
        }

        FederatedQuery::Job job;
        const TString jobId = request.execute_mode() == FederatedQuery::SAVE ? "" : GetEntityIdAsString(Config->IdsPrefix, EEntityType::JOB);
        if (request.execute_mode() != FederatedQuery::SAVE) {
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
            query.mutable_meta()->set_status(FederatedQuery::QueryMeta::STARTING);
            query.mutable_meta()->clear_expire_at();
            query.mutable_meta()->clear_result_expire_at();
            query.mutable_meta()->set_started_by(user);
            *query.mutable_meta()->mutable_submitted_at() = NProtoInterop::CastToProto(now);
            query.mutable_meta()->clear_action();

            internal.clear_plan_compressed();
            internal.clear_ast_compressed();
            internal.clear_dq_graph_compressed();
            internal.clear_execution_id();
            internal.clear_operation_id();

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
            *job.mutable_parameters() = request.content().parameters();
        }

        response->second.After.ConstructInPlace().CopyFrom(query);
        response->second.CloudId = internal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyQuery(write)");
        writeQueryBuilder.AddString("tenant", tenant);
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("query_id", queryId);
        writeQueryBuilder.AddUint64("max_count_jobs", Config->Proto.GetMaxCountJobs());
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
                "ORDER BY `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME  "`, `" JOB_ID_COLUMN_NAME "`\n"
                "LIMIT $max_count_jobs, 1\n"
            ");\n"
        );

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config->IdempotencyKeyTtl);

        if (request.content().acl().visibility() == oldVisibility) {
            writeQueryBuilder.AddInt64("visibility", query.content().acl().visibility());
            // move jobs to scope
            writeQueryBuilder.AddText(
                "UPDATE `" JOBS_TABLE_NAME "` SET `" VISIBILITY_COLUMN_NAME "` = $visibility\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        }

        if (request.execute_mode() != FederatedQuery::SAVE) {
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
            "   `" VISIBILITY_COLUMN_NAME "` = $visibility, `" AUTOMATIC_COLUMN_NAME "` = $automatic,\n"
            "   `" NAME_COLUMN_NAME "` = $name, `" EXECUTE_MODE_COLUMN_NAME "` = $execute_mode,\n"
            "   `" REVISION_COLUMN_NAME "` = $revision, `" STATUS_COLUMN_NAME "` = $status, "
        );

        if (request.execute_mode() != FederatedQuery::SAVE) {
            writeQueryBuilder.AddText(
                "   `" LAST_JOB_ID_COLUMN_NAME "` = $job_id, "
                "   `" INTERNAL_COLUMN_NAME "` = $internal,\n"
            );
        }

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
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(read.Sql, read.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvModifyQueryResponse, FederatedQuery::ModifyQueryResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_QUERY, RTC_DELETE_QUERY);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const FederatedQuery::DeleteQueryRequest& request = event.Request;
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
    std::shared_ptr<std::pair<FederatedQuery::DeleteQueryResult, TAuditDetails<FederatedQuery::Query>>> response = std::make_shared<std::pair<FederatedQuery::DeleteQueryResult, TAuditDetails<FederatedQuery::Query>>>();
    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteQuery");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("query_id", queryId);

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config->IdempotencyKeyTtl);
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
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
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
        YdbConnection->TablePathPrefix,
        requestCounters.Common->ParseProtobufError));

    validators.push_back(CreateQueryComputeStatusValidator(
        { FederatedQuery::QueryMeta::STARTING, FederatedQuery::QueryMeta::ABORTED_BY_USER, FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM, FederatedQuery::QueryMeta::COMPLETED, FederatedQuery::QueryMeta::FAILED },
        scope,
        queryId,
        "Can't delete running query",
        YdbConnection->TablePathPrefix,
        requestCounters.Common->ParseProtobufError));

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvDeleteQueryResponse, FederatedQuery::DeleteQueryResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CONTROL_QUERY, RTC_CONTROL_QUERY);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::ControlQueryRequest& request = event.Request;
    const TString user = event.User;
    const TString queryId = request.query_id();
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                    ? event.Permissions
                    : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int byteSize = request.ByteSize();
    const int64_t previousRevision = request.previous_revision();
    const TString idempotencyKey = request.idempotency_key();
    const FederatedQuery::QueryAction action = request.action();
    CPS_LOG_T("ControlQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token));

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_W("ControlQueryRequest: {" << request.DebugString() << "} " << MakeUserInfo(user, token) << "validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvControlQueryResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ControlQueryRequest, scope, user, queryId, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<FederatedQuery::ControlQueryResult, TAuditDetails<FederatedQuery::Query>>> response = std::make_shared<std::pair<FederatedQuery::ControlQueryResult, TAuditDetails<FederatedQuery::Query>>>();

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

    auto prepareParams = [=, config=Config, commonCounters=requestCounters.Common](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 2) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 2 but equal " << resultSets.size() << ". Please contact internal support";
        }

        auto now = TInstant::Now();
        FederatedQuery::Query query;
        FederatedQuery::Internal::QueryInternal queryInternal;
        TString tenantName;
        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }

            if (!queryInternal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
            }
            tenantName = *parser.ColumnParser(TENANT_COLUMN_NAME).GetOptionalString();
        }

        FederatedQuery::Job job;
        TString jobId;
        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the id query or your access rights";
            }

            if (!job.ParseFromString(parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString().GetOrElse(""))) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
            }

            jobId = job.meta().id();

            const bool hasManageAccess = HasManageAccess(permissions, query.content().acl().visibility(), query.meta().common().created_by(), user);
            if (!hasManageAccess) {
                ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }
        }

        queryInternal.set_action(action);

        auto& metaQuery = *query.mutable_meta();
        auto& commonQuery = *metaQuery.mutable_common();
        commonQuery.set_revision(commonQuery.revision() + 1);
        commonQuery.set_modified_by(user);
        *commonQuery.mutable_modified_at() = NProtoInterop::CastToProto(now);

        if (action == FederatedQuery::ABORT || action == FederatedQuery::ABORT_GRACEFULLY) {
            const bool isValidStatusForAbort = IsIn({
                FederatedQuery::QueryMeta::STARTING,
                FederatedQuery::QueryMeta::RESUMING,
                FederatedQuery::QueryMeta::RUNNING,
                FederatedQuery::QueryMeta::PAUSING
            }, metaQuery.status());

            const bool isTerminalStatus = IsTerminalStatus(metaQuery.status());
            if (isTerminalStatus) {
                return make_pair(TString{}, NYdb::TParamsBuilder{}.Build());
            }

            if (isValidStatusForAbort) {
                metaQuery.set_status(FederatedQuery::QueryMeta::ABORTING_BY_USER);
                metaQuery.set_aborted_by(user);
            } else {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Conversion from status " << FederatedQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::ABORTING_BY_USER) << " is not possible. Please wait for the previous operation to be completed";
            }
        }

        if (action == FederatedQuery::PAUSE || action == FederatedQuery::PAUSE_GRACEFULLY) {
            const bool isValidStatusForPause = IsIn({
                FederatedQuery::QueryMeta::RESUMING,
                FederatedQuery::QueryMeta::RUNNING
            }, metaQuery.status());

            if (isValidStatusForPause) {
                metaQuery.set_status(FederatedQuery::QueryMeta::PAUSING);
                metaQuery.set_paused_by(user);
            } else {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << FederatedQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::PAUSING) << " is not possible. Please wait for the previous operation to be completed";
            }
        }

        if (action == FederatedQuery::RESUME) {
            const bool isValidStatusForResume = metaQuery.status() == FederatedQuery::QueryMeta::PAUSED;
            if (isValidStatusForResume) {
                metaQuery.set_status(FederatedQuery::QueryMeta::RESUMING);
            } else {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Conversion from status " << FederatedQuery::QueryMeta::ComputeStatus_Name(metaQuery.status()) << " to " << FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::RESUMING) << " is not possible. Please wait for the previous operation to be completed";
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

        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), now + Config->IdempotencyKeyTtl);
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
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvControlQueryResponse, FederatedQuery::ControlQueryResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_RESULT_DATA, RTC_GET_RESULT_DATA);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());

    const FederatedQuery::GetResultDataRequest& request = event.Request;
    const TString user = event.User;
    const int32_t resultSetIndex = request.result_set_index();
    const int64_t offset = request.offset();
    const TString queryId = request.query_id();
    const int byteSize = event.Request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
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
        "SELECT `" RESULT_ID_COLUMN_NAME "`, `" RESULT_SET_ID_COLUMN_NAME "`, `" RESULT_SET_COLUMN_NAME "`, `" ROW_ID_COLUMN_NAME "` FROM `" RESULT_SETS_TABLE_NAME "`\n"
        "WHERE `" RESULT_ID_COLUMN_NAME "` = $result_id AND `" RESULT_SET_ID_COLUMN_NAME "` = $result_set_index AND `" ROW_ID_COLUMN_NAME "` >= $offset\n"
        "ORDER BY `" RESULT_ID_COLUMN_NAME "`, `" RESULT_SET_ID_COLUMN_NAME "`, `" ROW_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;\n"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, resultSetIndex, user, permissions, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 2) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 2 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::GetResultDataResult result;
        auto& resultSetProto = *result.mutable_result_set();
        {
            const auto& resultSet = (*resultSets)[0];
            TResultSetParser parser(resultSet);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            FederatedQuery::Query query;
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
            }

            FederatedQuery::Acl::Visibility queryVisibility = static_cast<FederatedQuery::Acl::Visibility>(parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64().GetOrElse(FederatedQuery::Acl::VISIBILITY_UNSPECIFIED));
            TString queryUser = parser.ColumnParser(USER_COLUMN_NAME).GetOptionalString().GetOrElse("");

            bool hasViewAccess = HasViewAccess(permissions, queryVisibility, queryUser, user);
            if (!hasViewAccess) {
                ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Query does not exist or permission denied. Please check the id of the query or your access rights";
            }

            if (resultSetIndex >= query.result_set_meta_size()) {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Result set index out of bound: " << resultSetIndex << " >= " << query.result_set_meta_size();
            }

            if (FederatedQuery::QueryMeta::ComputeStatus(*parser.ColumnParser(STATUS_COLUMN_NAME).GetOptionalInt64()) != FederatedQuery::QueryMeta::COMPLETED) {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Result doesn't exist";
            }

            auto resultSetsExpireAtParser = parser.ColumnParser(RESULT_SETS_EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp();
            if (!resultSetsExpireAtParser) {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Result doesn't exist";
            }

            if (*resultSetsExpireAtParser < TInstant::Now()) {
                ythrow TCodeLineException(TIssuesIds::EXPIRED) << "Result removed by TTL";
            }

            resultSetProto.mutable_columns()->CopyFrom(query.result_set_meta(resultSetIndex).column());
        }

        {
            const auto& resultSet = (*resultSets)[1];
            TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                if (!resultSetProto.add_rows()->ParseFromString(*parser.ColumnParser(RESULT_SET_COLUMN_NAME).GetOptionalString())) {
                    commonCounters->ParseProtobufError->Inc();
                    ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for row. Please contact internal support";
                }
            }
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvGetResultDataResponse, FederatedQuery::GetResultDataResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_JOBS_DATA, RTC_LIST_JOBS_DATA);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::ListJobsRequest& request = event.Request;
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
    TPermissions permissions = Config->Proto.GetEnablePermissions()
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
        "SELECT `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n"
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
        "ORDER BY `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::ListJobsResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            FederatedQuery::Job job;
            if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
            }
            const TString mergedId = job.meta().id() + "-" + job.query_meta().common().id();
            job.mutable_meta()->set_id(mergedId);
            job.mutable_query_meta()->set_last_job_id(mergedId);
            FederatedQuery::BriefJob briefJob;
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

    auto success = SendResponse<TEvControlPlaneStorage::TEvListJobsResponse, FederatedQuery::ListJobsResult>(
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
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_JOB, RTC_DESCRIBE_JOB);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::DescribeJobRequest& request = event.Request;

    const TString user = event.User;
    auto splittedId = SplitId(request.job_id());
    const auto jobId = std::move(splittedId.first);
    const auto queryId = std::move(splittedId.second);

    const int byteSize = request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
        ? event.Permissions
        : TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_AST | TPermissions::VIEW_QUERY_TEXT};
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);

    auto prepare = [=, id=request.job_id(), resultSets=resultSets, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::DescribeJobResult result;
        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the job id or your access rights";
        }
        if (!result.mutable_job()->ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support";
        }
        auto visibility = static_cast<FederatedQuery::Acl::Visibility>(*parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64());
        result.mutable_job()->mutable_meta()->set_id(id);

        bool hasViewAccces = HasViewAccess(permissions, visibility, result.job().meta().created_by(), user);
        if (!hasViewAccces) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Job does not exist or permission denied. Please check the job id or your access rights";
        }
        if (!permissions.Check(TPermissions::VIEW_AST)) {
            result.mutable_job()->clear_ast();
        }
        if (!permissions.Check(TPermissions::VIEW_QUERY_TEXT)) {
            result.mutable_job()->clear_text();
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeJobResponse, FederatedQuery::DescribeJobResult>(
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

ui64 TYdbControlPlaneStorageActor::GetExecutionLimitMills(
    FederatedQuery::QueryContent_QueryType queryType,
    const TMaybe<TQuotaMap>& quotas) {

    if (!quotas) {
        return 0;
    }
    auto key = queryType == FederatedQuery::QueryContent::ANALYTICS
        ? QUOTA_ANALYTICS_DURATION_LIMIT
        : QUOTA_STREAMING_DURATION_LIMIT;

    auto execTtlIt = quotas->find(key);
    if (execTtlIt == quotas->end()) {
        return 0;
    }
    return execTtlIt->second.Limit.Value * 60 * 1000;
}

} // NFq
