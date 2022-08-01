#include "utils.h"

#include <random>

#include <util/datetime/base.h>

#include <ydb/core/yq/libs/control_plane_storage/schema.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NYq {

namespace {

struct TTaskInternal {
    TTask Task;
    TRetryLimiter RetryLimiter;
    bool ShouldAbortTask = false;
    TString TablePathPrefix;
    TString Owner;
    TString HostName;
    TMaybe<YandexQuery::Job> Job;
    TInstant Deadline;
    TString TenantName;
};

    TString GetServiceAccountId(const YandexQuery::IamAuth& auth) {
        return auth.has_service_account()
                ? auth.service_account().id()
                : TString{};
    }

    TString ExtractServiceAccountId(const YandexQuery::Connection& c) {
        switch (c.content().setting().connection_case()) {
        case YandexQuery::ConnectionSetting::kYdbDatabase: {
            return GetServiceAccountId(c.content().setting().ydb_database().auth());
        }
        case YandexQuery::ConnectionSetting::kDataStreams: {
            return GetServiceAccountId(c.content().setting().data_streams().auth());
        }
        case YandexQuery::ConnectionSetting::kObjectStorage: {
            return GetServiceAccountId(c.content().setting().object_storage().auth());
        }
        case YandexQuery::ConnectionSetting::kMonitoring: {
            return GetServiceAccountId(c.content().setting().monitoring().auth());
        }
        case YandexQuery::ConnectionSetting::kClickhouseCluster: {
            return GetServiceAccountId(c.content().setting().clickhouse_cluster().auth());
        }
        // Do not replace with default. Adding a new connection should cause a compilation error
        case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET:
        break;
        }
        return {};
    }

std::pair<TString, NYdb::TParams> MakeSql(const TTaskInternal& taskInternal, const TInstant& nowTimestamp, const TInstant& taskLeaseUntil) {
    const auto& task = taskInternal.Task;
    TSqlQueryBuilder queryBuilder(taskInternal.TablePathPrefix, "GetTask(write)");
    queryBuilder.AddString("tenant", taskInternal.TenantName);
    queryBuilder.AddString("scope", task.Scope);
    queryBuilder.AddString("query_id", task.QueryId);
    queryBuilder.AddString("query", task.Query.SerializeAsString());
    queryBuilder.AddString("internal", task.Internal.SerializeAsString());
    queryBuilder.AddString("host", taskInternal.HostName);
    queryBuilder.AddString("owner", taskInternal.Owner);
    queryBuilder.AddTimestamp("now", nowTimestamp);
    queryBuilder.AddTimestamp("ttl", taskLeaseUntil);
    queryBuilder.AddUint64("retry_counter", taskInternal.RetryLimiter.RetryCount);
    queryBuilder.AddUint64("generation", task.Generation);
    queryBuilder.AddTimestamp("retry_counter_update_time", taskInternal.RetryLimiter.RetryCounterUpdatedAt);
    queryBuilder.AddDouble("retry_rate", taskInternal.RetryLimiter.RetryRate);

    // update queries
    queryBuilder.AddText(
        "UPDATE `" QUERIES_TABLE_NAME "` SET `" GENERATION_COLUMN_NAME "` = $generation, `" QUERY_COLUMN_NAME "` = $query, `" INTERNAL_COLUMN_NAME "` = $internal\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
    );

    // update pending small
    queryBuilder.AddText(
        "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" LAST_SEEN_AT_COLUMN_NAME "` = $now, `" ASSIGNED_UNTIL_COLUMN_NAME "` = $ttl,\n"
        "`" RETRY_COUNTER_COLUMN_NAME "` = $retry_counter, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "` = $retry_counter_update_time, `" RETRY_RATE_COLUMN_NAME "` = $retry_rate,\n"
        "`" HOST_NAME_COLUMN_NAME "` = $host, `" OWNER_COLUMN_NAME "` = $owner\n"
        "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
    );

    const auto query = queryBuilder.Build();
    return std::make_pair(query.Sql, query.Params);
}

} // namespace

std::tuple<TString, NYdb::TParams, std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>> MakeGetTaskUpdateQuery(
    const TTaskInternal& taskInternal,
    const std::shared_ptr<TResponseTasks>& responseTasks,
    const TInstant& nowTimestamp,
    const TInstant& taskLeaseUntil,
    bool disableCurrentIam,
    const TDuration& automaticQueriesTtl,
    const TDuration& resultSetsTtl)
{
    const auto& task = taskInternal.Task;

    TSqlQueryBuilder queryBuilder(taskInternal.TablePathPrefix, "GetTask(read)");
    queryBuilder.AddString("tenant", taskInternal.TenantName);
    queryBuilder.AddString("scope", task.Scope);
    queryBuilder.AddString("query_id", task.QueryId);
    queryBuilder.AddTimestamp("now", nowTimestamp);

    queryBuilder.AddText(
        "SELECT `" GENERATION_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`, `" QUERY_COLUMN_NAME "`\n"
        "FROM `" QUERIES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "SELECT `" ASSIGNED_UNTIL_COLUMN_NAME "`\n"
        "FROM `" PENDING_SMALL_TABLE_NAME "`\n"
        "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" ASSIGNED_UNTIL_COLUMN_NAME "` < $now;\n"
    );

    auto prepareParams = [=, taskInternal=taskInternal, responseTasks=responseTasks](const TVector<TResultSet>& resultSets) mutable {
        auto& task = taskInternal.Task;
        const auto shouldAbortTask = taskInternal.ShouldAbortTask;
        constexpr size_t expectedResultSetsSize = 2;

        if (resultSets.size() != expectedResultSetsSize || !resultSets[1].RowsCount()) {
            return std::make_pair(TString{}, TParamsBuilder{}.Build());
        }

        {
            TResultSetParser parser(resultSets[0]);
            while (parser.TryNextRow()) {
                task.Generation = parser.ColumnParser(GENERATION_COLUMN_NAME).GetOptionalUint64().GetOrElse(0) + 1;

                if (!task.Query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                    throw TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support";
                }
                const TInstant deadline = TInstant::Now() + (task.Query.content().automatic() ? std::min(automaticQueriesTtl, resultSetsTtl) : resultSetsTtl);
                task.Deadline = deadline;
                if (!task.Internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                    throw TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support";
                }

                if (disableCurrentIam) {
                    task.Internal.clear_token();
                }
            }
        }

        if (shouldAbortTask) {
            Ydb::Issue::IssueMessage abortIssueMsg;
            abortIssueMsg.set_message("Query was aborted by system due to high failure rate");
            abortIssueMsg.set_severity(NYql::TSeverityIds::S_ERROR);
            *task.Query.add_issue() = abortIssueMsg;
            task.Query.mutable_meta()->set_status(YandexQuery::QueryMeta::ABORTING_BY_SYSTEM);
        }

        responseTasks->AddTaskBlocking(task.QueryId, task);

        return MakeSql(taskInternal, nowTimestamp, taskLeaseUntil);
    };

    const auto query = queryBuilder.Build();
    return std::make_tuple(query.Sql, query.Params, prepareParams);
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvGetTaskRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TRequestCountersPtr requestCounters = Counters.GetCommonCounters(RTC_GET_TASK);
    requestCounters->InFly->Inc();

    auto& request = ev->Get()->Request;
    const TString owner = request.owner_id();
    const TString hostName = request.host();
    const TString tenantName = request.tenant();
    const ui64 tasksBatchSize = Config.Proto.GetTasksBatchSize();
    const ui64 numTasksProportion = Config.Proto.GetNumTasksProportion();

    CPS_LOG_T("GetTaskRequest: {" << request.DebugString() << "}");

    NYql::TIssues issues = ValidateGetTask(owner, hostName);
    if (issues) {
        CPS_LOG_W("GetTaskRequest: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvGetTaskResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(GetTaskRequest, owner, hostName, delta, false);
        return;
    }

    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};

    auto response = std::make_shared<std::tuple<TVector<TTask>, TString>>(); //tasks, owner

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "GetTask(read stale ro)");
    auto now = TInstant::Now();
    queryBuilder.AddString("tenant", tenantName);
    queryBuilder.AddTimestamp("from", now);
    queryBuilder.AddUint64("tasks_limit", tasksBatchSize);
    queryBuilder.AddText(
        "SELECT `" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" OWNER_COLUMN_NAME "`, `" LAST_SEEN_AT_COLUMN_NAME "`,\n"
        "`" RETRY_COUNTER_COLUMN_NAME "`, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "`, `" RETRY_RATE_COLUMN_NAME "`, `" QUERY_TYPE_COLUMN_NAME "`\n"
        "FROM `" PENDING_SMALL_TABLE_NAME "`\n"
        "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" ASSIGNED_UNTIL_COLUMN_NAME "` < $from ORDER BY `" QUERY_ID_COLUMN_NAME "` DESC LIMIT $tasks_limit;\n"
    );

    auto responseTasks = std::make_shared<TResponseTasks>();

    auto prepareParams = [=, rootCounters=Counters.Counters, actorSystem=NActors::TActivationContext::ActorSystem(), responseTasks=responseTasks](const TVector<TResultSet>& resultSets) mutable {
        TVector<TTaskInternal> tasks;
        TVector<TPickTaskParams> pickTaskParams;
        const auto now = TInstant::Now();
        if (resultSets.empty() || !resultSets.back().RowsCount()) {
            return pickTaskParams;
        }

        TResultSetParser parser(resultSets.back());
        while (parser.TryNextRow()) {
            TTaskInternal& taskInternal = tasks.emplace_back();
            taskInternal.Owner = owner;
            taskInternal.HostName = hostName;
            taskInternal.TenantName = tenantName;
            taskInternal.TablePathPrefix = YdbConnection->TablePathPrefix;

            auto& task = taskInternal.Task;

            task.Scope = *parser.ColumnParser(SCOPE_COLUMN_NAME).GetOptionalString();
            task.QueryId = *parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString();
            auto previousOwner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();

            taskInternal.RetryLimiter.Assign(
                parser.ColumnParser(RETRY_COUNTER_COLUMN_NAME).GetOptionalUint64().GetOrElse(0),
                parser.ColumnParser(RETRY_COUNTER_UPDATE_COLUMN_NAME).GetOptionalTimestamp().GetOrElse(TInstant::Zero()),
                parser.ColumnParser(RETRY_RATE_COLUMN_NAME).GetOptionalDouble().GetOrElse(0.0)
            );
            auto lastSeenAt = parser.ColumnParser(LAST_SEEN_AT_COLUMN_NAME).GetOptionalTimestamp().GetOrElse(TInstant::Zero());

            if (previousOwner) { // task lease timeout case only, other cases are updated at ping time
                CPS_LOG_AS_T(*actorSystem, "Task (Query): " << task.QueryId <<  " Lease TIMEOUT, RetryCounterUpdatedAt " << taskInternal.RetryLimiter.RetryCounterUpdatedAt
                    << " LastSeenAt: " << lastSeenAt);
                taskInternal.ShouldAbortTask = !taskInternal.RetryLimiter.UpdateOnRetry(lastSeenAt, Config.TaskLeaseRetryPolicy, now);
            }

            *rootCounters->GetSubgroup("scope", task.Scope)->GetSubgroup("query_id", task.QueryId)->GetCounter("RetryCount") = taskInternal.RetryLimiter.RetryCount;

            CPS_LOG_AS_T(*actorSystem, "Task (Query): " << task.QueryId <<  " RetryRate: " << taskInternal.RetryLimiter.RetryRate
                << " RetryCounter: " << taskInternal.RetryLimiter.RetryCount << " At: " << taskInternal.RetryLimiter.RetryCounterUpdatedAt
                << (taskInternal.ShouldAbortTask ? " ABORTED" : ""));
        }

        std::shuffle(tasks.begin(), tasks.end(), std::default_random_engine());
        const size_t numTasks = (std::min(tasks.size(), tasksBatchSize) + numTasksProportion - 1) / numTasksProportion;

        for (size_t i = 0; i < numTasks; ++i) {
            auto tupleParams = MakeGetTaskUpdateQuery(tasks[i],
                responseTasks, now, now + Config.TaskLeaseTtl, Config.Proto.GetDisableCurrentIam(),
                Config.AutomaticQueriesTtl, Config.ResultSetsTtl); // using for win32 build
            auto readQuery = std::get<0>(tupleParams);
            auto readParams = std::get<1>(tupleParams);
            auto prepareParams = std::get<2>(tupleParams);
            pickTaskParams.emplace_back(TPickTaskParams{readQuery, readParams, prepareParams, tasks[i].Task.QueryId, tasks[i].ShouldAbortTask});
        }
        return pickTaskParams;
    };

    const auto query = queryBuilder.Build();
    auto [readStatus, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo, TTxSettings::StaleRO());
    auto result = readStatus.Apply(
        [=,
        resultSets=resultSets,
        requestCounters=requestCounters,
        debugInfo=debugInfo,
        responseTasks=responseTasks] (const auto& readFuture) mutable
    {
        try {
            if (!readFuture.GetValue().IsSuccess())
                return readFuture;
        } catch (...) {
            return readFuture;
        }

        auto pickTaskParams = prepareParams(*resultSets);
        if (pickTaskParams.empty())
            return readFuture;

        auto debugInfos = std::make_shared<TVector<TDebugInfoPtr>>(pickTaskParams.size());
        if (Config.Proto.GetEnableDebugMode()) {
            for (size_t i = 0; i < pickTaskParams.size(); i++) {
                (*debugInfos)[i] = std::make_shared<TDebugInfo>();
            }
        }

        TVector<TFuture<void>> futures;
        for (size_t i = 0; i < pickTaskParams.size(); ++i) {
            futures.emplace_back(PickTask(pickTaskParams[i], requestCounters, (*debugInfos)[i], responseTasks));
        }

        auto allFuture = NThreading::WaitExceptionOrAll(futures);
        return allFuture.Apply([=, responseTasks=responseTasks](const auto& future) mutable {
            if (debugInfo) {
                for (const auto& info: *debugInfos) {
                    debugInfo->insert(debugInfo->end(), info->begin(), info->end());
                }
            }
            NYql::TIssues issues;
            auto status = MakeFuture(TStatus{EStatus::SUCCESS, std::move(issues)});
            try {
                future.GetValue();
                TVector<TTask> tasks;
                for (const auto& [_, task] : responseTasks->GetTasksNonBlocking()) {
                    tasks.emplace_back(task);
                }
                *response = std::make_tuple(tasks, owner);
            } catch (...) {
                issues.AddIssue(CurrentExceptionMessage());
                status = MakeFuture(TStatus{EStatus::GENERIC_ERROR, std::move(issues)});
            }
            return status;
        });
    });

    auto prepare = [response] {
        Fq::Private::GetTaskResult result;
        const auto& tasks = std::get<0>(*response);

        for (const auto& task : tasks) {
            const auto& queryType = task.Query.content().type();
            if (queryType != YandexQuery::QueryContent::ANALYTICS && queryType != YandexQuery::QueryContent::STREAMING) { //TODO: fix
                ythrow yexception()
                    << "query type "
                    << YandexQuery::QueryContent::QueryType_Name(queryType)
                    << " unsupported";
            }

            auto userExecutionLimit = TDuration::MilliSeconds(task.Query.content().Getlimits().vcpu_time_limit());
            auto systemExecutionLimit = NProtoInterop::CastFromProto(task.Internal.execution_ttl());
            auto executionLimit = std::min(userExecutionLimit, systemExecutionLimit);
            if (systemExecutionLimit == TDuration::Zero()) {
                executionLimit = userExecutionLimit;
            } else if (userExecutionLimit == TDuration::Zero()) {
                executionLimit = systemExecutionLimit;
            }

            auto* newTask = result.add_tasks();
            newTask->set_query_type(queryType);
            newTask->set_execute_mode(task.Query.meta().execute_mode());
            newTask->set_state_load_mode(task.Internal.state_load_mode());
            auto* queryId = newTask->mutable_query_id();
            queryId->set_value(task.Query.meta().common().id());
            newTask->set_streaming(queryType == YandexQuery::QueryContent::STREAMING);
            newTask->set_text(task.Query.content().text());
            *newTask->mutable_connection() = task.Internal.connection();
            *newTask->mutable_binding() = task.Internal.binding();
            newTask->set_user_token(task.Internal.token());
            newTask->set_user_id(task.Query.meta().common().created_by());
            newTask->set_generation(task.Generation);
            newTask->set_status(task.Query.meta().status());
            *newTask->mutable_created_topic_consumers() = task.Internal.created_topic_consumers();
            newTask->mutable_sensor_labels()->insert({"cloud_id", task.Internal.cloud_id()});
            newTask->mutable_sensor_labels()->insert({"scope", task.Scope});
            newTask->set_automatic(task.Query.content().automatic());
            newTask->set_query_name(task.Query.content().name());
            *newTask->mutable_deadline() = NProtoInterop::CastToProto(task.Deadline);
            newTask->mutable_disposition()->CopyFrom(task.Internal.disposition());
            newTask->set_result_limit(task.Internal.result_limit());
            *newTask->mutable_execution_limit() = NProtoInterop::CastToProto(executionLimit);
            *newTask->mutable_request_started_at() = task.Query.meta().started_at();

            for (const auto& connection: task.Internal.connection()) {
                const auto serviceAccountId = ExtractServiceAccountId(connection);
                if (!serviceAccountId) {
                        continue;
                }
                auto* account = newTask->add_service_accounts();
                account->set_value(serviceAccountId);
            }

            *newTask->mutable_dq_graph() = task.Internal.dq_graph();
            newTask->set_dq_graph_index(task.Internal.dq_graph_index());
            *newTask->mutable_dq_graph_compressed() = task.Internal.dq_graph_compressed();

            *newTask->mutable_result_set_meta() = task.Query.result_set_meta();
            newTask->set_scope(task.Scope);
        }

        return result;
    };
    auto success = SendResponse<TEvControlPlaneStorage::TEvGetTaskResponse, Fq::Private::GetTaskResult>
        ("GetTaskRequest - GetTaskResult",
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
            LWPROBE(GetTaskRequest, owner, hostName, delta, future.GetValue());
        });
}

} // NYq
