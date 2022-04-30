#include "utils.h"

#include <util/datetime/base.h>

#include <ydb/core/yq/libs/db_schema/db_schema.h>

namespace NYq {

namespace {

bool IsFinishedStatus(YandexQuery::QueryMeta::ComputeStatus status) {
    return status == YandexQuery::QueryMeta::ABORTED_BY_SYSTEM
        || status == YandexQuery::QueryMeta::ABORTED_BY_USER
        || status == YandexQuery::QueryMeta::COMPLETED
        || status == YandexQuery::QueryMeta::FAILED;
}

} // namespace

std::tuple<TString, TParams, const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>> ConstructHardPingTask(
    const TEvControlPlaneStorage::TEvPingTaskRequest* request, std::shared_ptr<YandexQuery::QueryAction> response,
    const TString& tablePathPrefix, const TDuration& automaticQueriesTtl, const TDuration& taskLeaseTtl, const THashMap<ui64, TRetryPolicyItem>& retryPolicies) {

    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "HardPingTask(read)");
    readQueryBuilder.AddString("tenant", request->TenantName);
    readQueryBuilder.AddString("scope", request->Scope);
    readQueryBuilder.AddString("query_id", request->QueryId);
    readQueryBuilder.AddText(
        "$last_job_id = SELECT `" LAST_JOB_ID_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "SELECT `" QUERY_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n"
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        "SELECT `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n"
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" JOB_ID_COLUMN_NAME "` = $last_job_id;\n"
        "SELECT `" OWNER_COLUMN_NAME "`, `" RETRY_COUNTER_COLUMN_NAME "`, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "`, `" RETRY_RATE_COLUMN_NAME "`\n"
        "FROM `" PENDING_SMALL_TABLE_NAME "` WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
    );

    auto prepareParams = [=, actorSystem = NActors::TActivationContext::ActorSystem()](const TVector<TResultSet>& resultSets) {
        TString jobId;
        YandexQuery::Query query;
        YandexQuery::Internal::QueryInternal internal;
        YandexQuery::Job job;
        TString owner;
        ui64 retryCounter = 0;
        TInstant retryCounterUpdatedAt = TInstant::Zero();
        double retryRate = 0.0;

        if (resultSets.size() != 3) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "RESULT SET SIZE of " << resultSets.size() << " != 3";
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"";
            }
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." QUERY_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\" and " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\"";
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\" and " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " JOBS_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"";
            }
            if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " JOBS_TABLE_NAME "." JOB_COLUMN_NAME " where " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"";
            }
            jobId = *parser.ColumnParser(JOB_ID_COLUMN_NAME).GetOptionalString();
        }

        TRetryLimiter retryLimiter;
        {
            TResultSetParser parser(resultSets[2]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request->TenantName << "\" and " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request->Owner) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request->QueryId << "\" MISMATCHED: \"" << request->Owner << "\" (received) != \"" << owner << "\" (selected)";
            }
            retryLimiter.Assign(
                parser.ColumnParser(RETRY_COUNTER_COLUMN_NAME).GetOptionalUint64().GetOrElse(0),
                retryCounterUpdatedAt = parser.ColumnParser(RETRY_COUNTER_UPDATE_COLUMN_NAME).GetOptionalTimestamp().GetOrElse(TInstant::Zero()),
                retryRate = parser.ColumnParser(RETRY_RATE_COLUMN_NAME).GetOptionalDouble().GetOrElse(0.0)
            );
        }

        TMaybe<YandexQuery::QueryMeta::ComputeStatus> queryStatus = request->Status;
        TMaybe<NYql::TIssues> issues = request->Issues;
        TMaybe<NYql::TIssues> transientIssues = request->TransientIssues;

        // running query us locked for lease period
        TDuration backoff = taskLeaseTtl;

        if (request->ResignQuery) {
            TRetryPolicyItem policy(0, TDuration::Seconds(1), TDuration::Zero());
            auto it = retryPolicies.find(request->StatusCode);
            auto policyFound = it != retryPolicies.end();
            if (policyFound) {
                policy = it->second;
            }

            if (retryLimiter.UpdateOnRetry(Now(), policy)) {
                queryStatus.Clear();
                // failing query is throttled for backoff period
                backoff = policy.BackoffPeriod * retryLimiter.RetryRate;
                owner = "";
            } else {
                // failure query should be processed instantly
                queryStatus = YandexQuery::QueryMeta::FAILING;
                backoff = TDuration::Zero();
                // all transient issues became final
                if (transientIssues) {
                    if (issues) {
                        issues->AddIssues(*transientIssues);
                        transientIssues.Clear();
                    } else {
                        issues.Swap(transientIssues);
                    }
                }
            }
            CPS_LOG_AS_D(*actorSystem, "PingTaskRequest (resign): " << (!policyFound ? " DEFAULT POLICY" : "") << (owner ? " FAILURE" : "") << request->StatusCode << " " << retryLimiter.RetryCount << " " << retryLimiter.RetryCounterUpdatedAt << " " << backoff);
        }

        if (queryStatus) {
            query.mutable_meta()->set_status(*queryStatus);
            job.mutable_query_meta()->set_status(*queryStatus);
        }

        if (issues) {
            NYql::IssuesToMessage(*issues, query.mutable_issue());
            NYql::IssuesToMessage(*issues, job.mutable_issue());
        }

        if (transientIssues) {
            NYql::TIssues issues = *transientIssues;
            for (const auto& issue: *query.mutable_transient_issue()) {
                issues.AddIssue(NYql::IssueFromMessage(issue));
            }

            NYql::TIssues newIssues;
            std::for_each_n(issues.begin(), std::min(static_cast<unsigned long long>(issues.Size()), 20ULL), [&](auto& issue){ newIssues.AddIssue(issue); });

            NYql::IssuesToMessage(newIssues, query.mutable_transient_issue());
        }

        if (request->Statistics) {
            *query.mutable_statistics()->mutable_json() = *request->Statistics;
            *job.mutable_statistics()->mutable_json() = *request->Statistics;
        }

        if (request->ResultSetMetas) {
            // we will overwrite result_set_meta's COMPLETELY
            query.clear_result_set_meta();
            job.clear_result_set_meta();
            for (const auto& resultSetMeta : *request->ResultSetMetas) {
                *query.add_result_set_meta() = resultSetMeta;
                *job.add_result_set_meta() = resultSetMeta;
            }
        }

        if (request->Ast) {
            query.mutable_ast()->set_data(*request->Ast);
            job.mutable_ast()->set_data(*request->Ast);
        }

        if (request->Plan) {
            query.mutable_plan()->set_json(*request->Plan);
            job.mutable_plan()->set_json(*request->Plan);
        }

        if (request->StartedAt) {
            *query.mutable_meta()->mutable_started_at() = NProtoInterop::CastToProto(*request->StartedAt);
            *job.mutable_query_meta()->mutable_started_at() = NProtoInterop::CastToProto(*request->StartedAt);
        }

        if (request->FinishedAt) {
            *query.mutable_meta()->mutable_finished_at() = NProtoInterop::CastToProto(*request->FinishedAt);
            *job.mutable_query_meta()->mutable_finished_at() = NProtoInterop::CastToProto(*request->FinishedAt);
            if (!query.meta().has_started_at()) {
                *query.mutable_meta()->mutable_started_at() = NProtoInterop::CastToProto(*request->FinishedAt);
                *job.mutable_query_meta()->mutable_started_at() = NProtoInterop::CastToProto(*request->FinishedAt);
            }
        }

        TInstant expireAt = TInstant::Now() + automaticQueriesTtl;
        if (IsTerminalStatus(query.meta().status()) && query.content().automatic()) {
            *query.mutable_meta()->mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
            *job.mutable_query_meta()->mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
            *job.mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
        }

        if (query.meta().status() == YandexQuery::QueryMeta::COMPLETED) {
            *query.mutable_meta()->mutable_result_expire_at() = NProtoInterop::CastToProto(request->Deadline);
        }

        if (request->StateLoadMode) {
            internal.set_state_load_mode(request->StateLoadMode);
            if (request->StateLoadMode == YandexQuery::FROM_LAST_CHECKPOINT) { // Saved checkpoint
                query.mutable_meta()->set_has_saved_checkpoints(true);
            }
        }

        if (request->StreamingDisposition) {
            internal.mutable_disposition()->CopyFrom(*request->StreamingDisposition);
        }

        if (request->Status && IsFinishedStatus(*request->Status)) {
            internal.clear_created_topic_consumers();
            internal.clear_dq_graph();
            internal.clear_dq_graph_index();
        }

        if (!request->CreatedTopicConsumers.empty()) {
            std::set<Yq::Private::TopicConsumer, TTopicConsumerLess> mergedConsumers;
            for (auto&& c : *internal.mutable_created_topic_consumers()) {
                mergedConsumers.emplace(std::move(c));
            }

            for (const auto& c : request->CreatedTopicConsumers) {
                Yq::Private::TopicConsumer proto;
                proto.set_database_id(c.DatabaseId);
                proto.set_database(c.Database);
                proto.set_topic_path(c.TopicPath);
                proto.set_consumer_name(c.ConsumerName);
                proto.set_cluster_endpoint(c.ClusterEndpoint);
                proto.set_use_ssl(c.UseSsl);
                proto.set_token_name(c.TokenName);
                proto.set_add_bearer_to_token(c.AddBearerToToken);
                mergedConsumers.emplace(std::move(proto));
            }
            internal.clear_created_topic_consumers();
            for (auto&& c : mergedConsumers) {
                *internal.add_created_topic_consumers() = std::move(c);
            }
        }

        if (!request->DqGraphs.empty()) {
            internal.clear_dq_graph();
            for (const auto& g : request->DqGraphs) {
                internal.add_dq_graph(g);
            }
        }

        if (request->DqGraphIndex) {
            internal.set_dq_graph_index(request->DqGraphIndex);
        }

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "HardPingTask(write)");
        writeQueryBuilder.AddString("tenant", request->TenantName);
        writeQueryBuilder.AddString("scope", request->Scope);
        writeQueryBuilder.AddString("job_id", jobId);
        writeQueryBuilder.AddString("job", job.SerializeAsString());
        writeQueryBuilder.AddString("query", query.SerializeAsString());
        writeQueryBuilder.AddInt64("status", query.meta().status());
        writeQueryBuilder.AddString("internal", internal.SerializeAsString());
        writeQueryBuilder.AddString("result_id", request->ResultId);
        writeQueryBuilder.AddString("query_id", request->QueryId);

        if (IsTerminalStatus(query.meta().status())) {
            // delete pending
            writeQueryBuilder.AddText(
                "DELETE FROM `" PENDING_SMALL_TABLE_NAME "`\n"
                "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        } else {
            // update pending small
            writeQueryBuilder.AddTimestamp("now", TInstant::Now());
            writeQueryBuilder.AddTimestamp("ttl", TInstant::Now() + backoff);
            writeQueryBuilder.AddTimestamp("retry_counter_update_time", retryCounterUpdatedAt);
            writeQueryBuilder.AddDouble("retry_rate", retryRate);
            writeQueryBuilder.AddUint64("retry_counter", retryCounter);
            writeQueryBuilder.AddString("owner", owner);
            writeQueryBuilder.AddText(
                "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" LAST_SEEN_AT_COLUMN_NAME "` = $now, `" ASSIGNED_UNTIL_COLUMN_NAME "` = $ttl,\n"
                "`" RETRY_COUNTER_COLUMN_NAME "` = $retry_counter, `" RETRY_COUNTER_UPDATE_COLUMN_NAME "` = $retry_counter_update_time, `" RETRY_RATE_COLUMN_NAME "` = $retry_rate,\n"
                "`" OWNER_COLUMN_NAME "` = $owner\n"
                "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        }

        if (IsTerminalStatus(query.meta().status()) && query.content().automatic()) {
            // set jobs ttl
            writeQueryBuilder.AddText(
                "UPDATE `" JOBS_TABLE_NAME "` SET `" EXPIRE_AT_COLUMN_NAME "` = $expire_at\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" EXPIRE_AT_COLUMN_NAME "` IS NULL;\n"
            );
        } else if (IsTerminalStatus(query.meta().status())) {
            // set jobs ttl null
            writeQueryBuilder.AddText(
                "UPDATE `" JOBS_TABLE_NAME "` SET `" EXPIRE_AT_COLUMN_NAME "` = NULL\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" EXPIRE_AT_COLUMN_NAME "` IS NOT NULL;\n"
            );
        }

        TString updateResultSetsExpire;
        if (query.meta().status() == YandexQuery::QueryMeta::COMPLETED) {
            writeQueryBuilder.AddTimestamp("result_sets_expire_at", request->Deadline);
            updateResultSetsExpire = "`" RESULT_SETS_EXPIRE_AT_COLUMN_NAME "` = $result_sets_expire_at";
        } else {
            updateResultSetsExpire = "`" RESULT_SETS_EXPIRE_AT_COLUMN_NAME "` = NULL";
        }

        TString updateQueryTtl;
        if (IsTerminalStatus(query.meta().status()) && query.content().automatic()) {
            writeQueryBuilder.AddTimestamp("expire_at", expireAt);
            updateQueryTtl = "`" EXPIRE_AT_COLUMN_NAME "` = $expire_at";
        } else {
            updateQueryTtl = "`" EXPIRE_AT_COLUMN_NAME "` = NULL";
        }

        writeQueryBuilder.AddText(
            "UPSERT INTO `" JOBS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "`) VALUES($scope, $query_id, $job_id, $job);\n"
            "UPDATE `" QUERIES_TABLE_NAME "` SET `" QUERY_COLUMN_NAME "` = $query, `" STATUS_COLUMN_NAME "` = $status, `" INTERNAL_COLUMN_NAME "` = $internal, `" RESULT_ID_COLUMN_NAME "` = $result_id, " + updateResultSetsExpire + ", " + updateQueryTtl + ", `" META_REVISION_COLUMN_NAME  "` = `" META_REVISION_COLUMN_NAME "` + 1\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        );

        *response = internal.action();
        const auto writeQuery = writeQueryBuilder.Build();
        return std::make_pair(writeQuery.Sql, writeQuery.Params);
    };
    const auto readQuery = readQueryBuilder.Build();
    return std::make_tuple(readQuery.Sql, readQuery.Params, prepareParams);
}

std::tuple<TString, TParams, const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>> ConstructSoftPingTask(
    const TEvControlPlaneStorage::TEvPingTaskRequest* request, std::shared_ptr<YandexQuery::QueryAction> response,
    const TString& tablePathPrefix, const TDuration& taskLeaseTtl) {
    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "SoftPingTask(read)");
    readQueryBuilder.AddString("tenant", request->TenantName);
    readQueryBuilder.AddString("scope", request->Scope);
    readQueryBuilder.AddString("query_id", request->QueryId);
    readQueryBuilder.AddText(
        "SELECT `" INTERNAL_COLUMN_NAME "`\n"
        "FROM `" QUERIES_TABLE_NAME "` WHERE `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" SCOPE_COLUMN_NAME "` = $scope;\n"
        "SELECT `" OWNER_COLUMN_NAME "`\n"
        "FROM `" PENDING_SMALL_TABLE_NAME "` WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
    );

    auto prepareParams = [=](const TVector<TResultSet>& resultSets) {
        TString owner;
        YandexQuery::Internal::QueryInternal internal;

        if (resultSets.size() != 2) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "RESULT SET SIZE of " << resultSets.size() << " != 2";
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"" ;
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\" and " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request->TenantName << "\" and " SCOPE_COLUMN_NAME " = \"" << request->Scope << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request->QueryId << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request->Owner) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request->QueryId << "\" MISMATCHED: \"" << request->Owner << "\" (received) != \"" << owner << "\" (selected)";
            }
        }

        *response = internal.action();

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "SoftPingTask(write)");
        writeQueryBuilder.AddTimestamp("now", TInstant::Now());
        writeQueryBuilder.AddTimestamp("ttl", TInstant::Now() + taskLeaseTtl);
        writeQueryBuilder.AddString("tenant", request->TenantName);
        writeQueryBuilder.AddString("scope", request->Scope);
        writeQueryBuilder.AddString("query_id", request->QueryId);

        writeQueryBuilder.AddText(
            "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" LAST_SEEN_AT_COLUMN_NAME "` = $now, `" ASSIGNED_UNTIL_COLUMN_NAME "` = $ttl\n"
            "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        );

        const auto writeQuery = writeQueryBuilder.Build();
        return std::make_pair(writeQuery.Sql, writeQuery.Params);
    };
    const auto readQuery = readQueryBuilder.Build();
    return std::make_tuple(readQuery.Sql, readQuery.Params, prepareParams);
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvPingTaskRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TEvControlPlaneStorage::TEvPingTaskRequest* request = ev->Get();
    const TString cloudId = request->CloudId;
    const TString scope = request->Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_PING_TASK);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(request->GetByteSize());
    const TString queryId = request->QueryId;
    const TString owner = request->Owner;
    const TInstant deadline = request->Deadline;

    CPS_LOG_T("PingTaskRequest: " << request->TenantName << " " << scope << " " << queryId
        << " " << owner << " " << deadline << " "
        << (request->Status ? YandexQuery::QueryMeta_ComputeStatus_Name(*request->Status) : "no status"));

    NYql::TIssues issues = ValidatePingTask(scope, queryId, owner, deadline, Config.ResultSetsTtl);
    if (issues) {
        CPS_LOG_D("PingTaskRequest, validation failed: " << scope << " " << queryId  << " " << owner << " " << deadline << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvPingTaskResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(PingTaskRequest, queryId, delta, false);
        return;
    }

    std::shared_ptr<YandexQuery::QueryAction> response = std::make_shared<YandexQuery::QueryAction>();

    if (request->Status)
        Counters.GetFinalStatusCounters(cloudId, scope)->IncByStatus(*request->Status);
    auto pingTaskParams = DoesPingTaskUpdateQueriesTable(request) ?
        ConstructHardPingTask(request, response, YdbConnection->TablePathPrefix, Config.AutomaticQueriesTtl, Config.TaskLeaseTtl, Config.RetryPolicies) :
        ConstructSoftPingTask(request, response, YdbConnection->TablePathPrefix, Config.TaskLeaseTtl);
    auto readQuery = std::get<0>(pingTaskParams); // Use std::get for win compiler
    auto readParams = std::get<1>(pingTaskParams);
    auto prepareParams = std::get<2>(pingTaskParams);

    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), readQuery, readParams, prepareParams, requestCounters, debugInfo);
    auto prepare = [response] { return std::make_tuple(*response); };
    auto success = SendResponseTuple<TEvControlPlaneStorage::TEvPingTaskResponse, std::tuple<YandexQuery::QueryAction>>(
        "PingTaskRequest",
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
            LWPROBE(PingTaskRequest, queryId, delta, future.GetValue());
        });
}

} // NYq
