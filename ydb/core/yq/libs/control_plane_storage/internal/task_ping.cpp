#include "utils.h"

#include <util/datetime/base.h>

#include <ydb/core/yq/libs/db_schema/db_schema.h>

#include <google/protobuf/util/time_util.h>

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
    const Fq::Private::PingTaskRequest& request, std::shared_ptr<Fq::Private::PingTaskResult> response,
    const TString& tablePathPrefix, const TDuration& automaticQueriesTtl, const TDuration& taskLeaseTtl, const THashMap<ui64, TRetryPolicyItem>& retryPolicies,
    ::NMonitoring::TDynamicCounterPtr rootCounters) {

    auto scope = request.scope();
    auto query_id = request.query_id().value();
    auto counters = rootCounters->GetSubgroup("scope", scope)->GetSubgroup("query_id", query_id);

    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "HardPingTask(read)");
    readQueryBuilder.AddString("tenant", request.tenant());
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("query_id", query_id);
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

    auto prepareParams = [=, counters=counters, actorSystem = NActors::TActivationContext::ActorSystem()](const TVector<TResultSet>& resultSets) {
        TString jobId;
        YandexQuery::Query query;
        YandexQuery::Internal::QueryInternal internal;
        YandexQuery::Job job;
        TString owner;

        if (resultSets.size() != 3) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "RESULT SET SIZE of " << resultSets.size() << " != 3";
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." QUERY_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " JOBS_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " JOBS_TABLE_NAME "." JOB_COLUMN_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            jobId = *parser.ColumnParser(JOB_ID_COLUMN_NAME).GetOptionalString();
        }

        TRetryLimiter retryLimiter;
        {
            TResultSetParser parser(resultSets[2]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request.tenant() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request.owner_id()) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request.query_id().value() << "\" MISMATCHED: \"" << request.owner_id() << "\" (received) != \"" << owner << "\" (selected)";
            }
            retryLimiter.Assign(
                parser.ColumnParser(RETRY_COUNTER_COLUMN_NAME).GetOptionalUint64().GetOrElse(0),
                parser.ColumnParser(RETRY_COUNTER_UPDATE_COLUMN_NAME).GetOptionalTimestamp().GetOrElse(TInstant::Zero()),
                parser.ColumnParser(RETRY_RATE_COLUMN_NAME).GetOptionalDouble().GetOrElse(0.0)
            );
        }

        TMaybe<YandexQuery::QueryMeta::ComputeStatus> queryStatus;
        if (request.status() != YandexQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED) {
            queryStatus = request.status();
        }
        TMaybe<NYql::TIssues> issues;
        if (request.issues().size() > 0) {
            NYql::TIssues requestIssues;
            NYql::IssuesFromMessage(request.issues(), requestIssues);
            issues = requestIssues;
        }
        TMaybe<NYql::TIssues> transientIssues;
        if (request.transient_issues().size() > 0) {
            NYql::TIssues requestTransientIssues;
            NYql::IssuesFromMessage(request.transient_issues(), requestTransientIssues);
            transientIssues = requestTransientIssues;
        }
        // running query us locked for lease period
        TDuration backoff = taskLeaseTtl;

        if (request.resign_query()) {
            TRetryPolicyItem policy(0, TDuration::Seconds(1), TDuration::Zero());
            auto it = retryPolicies.find(request.status_code());
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
            CPS_LOG_AS_T(*actorSystem, "PingTaskRequest (resign): " << (!policyFound ? " DEFAULT POLICY" : "") << (owner ? " FAILURE " : " ") << NYql::NDqProto::StatusIds_StatusCode_Name(request.status_code()) << " " << retryLimiter.RetryCount << " " << retryLimiter.RetryCounterUpdatedAt << " " << backoff);
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

        if (request.statistics()) {
            *query.mutable_statistics()->mutable_json() = request.statistics();
            *job.mutable_statistics()->mutable_json() = request.statistics();
        }

        if (!request.result_set_meta().empty()) {
            // we will overwrite result_set_meta's COMPLETELY
            *query.mutable_result_set_meta() = request.result_set_meta();
            *job.mutable_result_set_meta() = request.result_set_meta();
        }

        if (request.ast()) {
            query.mutable_ast()->set_data(request.ast());
            job.mutable_ast()->set_data(request.ast());
        }

        if (request.plan()) {
            query.mutable_plan()->set_json(request.plan());
            job.mutable_plan()->set_json(request.plan());
        }

        if (request.ast_compressed().data()) {
            internal.mutable_ast_compressed()->set_method(request.ast_compressed().method());
            internal.mutable_ast_compressed()->set_data(request.ast_compressed().data());
            // todo: keep AST compressed in JobInternal
            // job.mutable_ast()->set_data(request.ast());
        }

        if (request.plan_compressed().data()) {
            internal.mutable_plan_compressed()->set_method(request.plan_compressed().method());
            internal.mutable_plan_compressed()->set_data(request.plan_compressed().data());
            // todo: keep plan compressed in JobInternal
            // job.mutable_plan()->set_json(request.plan());
        }

        if (request.has_started_at()) {
            *query.mutable_meta()->mutable_started_at() = request.started_at();
            *job.mutable_query_meta()->mutable_started_at() = request.started_at();
        }

        if (request.has_finished_at()) {
            *query.mutable_meta()->mutable_finished_at() = request.finished_at();
            *job.mutable_query_meta()->mutable_finished_at() = request.finished_at();
            if (!query.meta().has_started_at()) {
                *query.mutable_meta()->mutable_started_at() = request.finished_at();
                *job.mutable_query_meta()->mutable_started_at() = request.finished_at();
            }
        }

        TInstant expireAt = TInstant::Now() + automaticQueriesTtl;
        if (IsTerminalStatus(query.meta().status()) && query.content().automatic()) {
            *query.mutable_meta()->mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
            *job.mutable_query_meta()->mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
            *job.mutable_expire_at() = NProtoInterop::CastToProto(expireAt);
        }

        if (query.meta().status() == YandexQuery::QueryMeta::COMPLETED) {
            *query.mutable_meta()->mutable_result_expire_at() = request.deadline();
        }

        if (request.state_load_mode()) {
            internal.set_state_load_mode(request.state_load_mode());
            if (request.state_load_mode() == YandexQuery::FROM_LAST_CHECKPOINT) { // Saved checkpoint
                query.mutable_meta()->set_has_saved_checkpoints(true);
            }
        }

        if (request.has_disposition()) {
            *internal.mutable_disposition() = request.disposition();
        }

        if (request.status() && IsFinishedStatus(request.status())) {
            internal.clear_created_topic_consumers();
            // internal.clear_dq_graph(); keep for debug
            internal.clear_dq_graph_index();
        }

        if (!request.created_topic_consumers().empty()) {
            std::set<Fq::Private::TopicConsumer, TTopicConsumerLess> mergedConsumers;
            for (auto&& c : *internal.mutable_created_topic_consumers()) {
                mergedConsumers.emplace(std::move(c));
            }
            for (const auto& c : request.created_topic_consumers()) {
                mergedConsumers.emplace(c);
            }
            internal.clear_created_topic_consumers();
            for (auto&& c : mergedConsumers) {
                *internal.add_created_topic_consumers() = std::move(c);
            }
        }

        if (!request.dq_graph().empty()) {
            *internal.mutable_dq_graph() = request.dq_graph();
        }

        if (!request.dq_graph_compressed().empty()) {
            *internal.mutable_dq_graph_compressed() = request.dq_graph_compressed();
        }

        if (request.dq_graph_index()) {
            internal.set_dq_graph_index(request.dq_graph_index());
        }

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "HardPingTask(write)");
        writeQueryBuilder.AddString("tenant", request.tenant());
        writeQueryBuilder.AddString("scope", request.scope());
        writeQueryBuilder.AddString("job_id", jobId);
        writeQueryBuilder.AddString("job", job.SerializeAsString());
        writeQueryBuilder.AddString("query", query.SerializeAsString());
        writeQueryBuilder.AddInt64("status", query.meta().status());
        writeQueryBuilder.AddString("internal", internal.SerializeAsString());
        writeQueryBuilder.AddString("result_id", request.result_id().value());
        writeQueryBuilder.AddString("query_id", request.query_id().value());

        TInstant ttl;
        if (IsTerminalStatus(query.meta().status())) {
            // delete pending
            writeQueryBuilder.AddText(
                "DELETE FROM `" PENDING_SMALL_TABLE_NAME "`\n"
                "WHERE `" TENANT_COLUMN_NAME "` = $tenant AND `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        } else {
            *counters->GetCounter("RetryCount") = retryLimiter.RetryCount;
            // update pending small
            ttl = TInstant::Now() + backoff;
            writeQueryBuilder.AddTimestamp("now", TInstant::Now());
            writeQueryBuilder.AddTimestamp("ttl", ttl);
            writeQueryBuilder.AddTimestamp("retry_counter_update_time", retryLimiter.RetryCounterUpdatedAt);
            writeQueryBuilder.AddDouble("retry_rate", retryLimiter.RetryRate);
            writeQueryBuilder.AddUint64("retry_counter", retryLimiter.RetryCount);
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
            writeQueryBuilder.AddTimestamp("result_sets_expire_at", NProtoInterop::CastFromProto(request.deadline()));
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

        response->set_action(internal.action());
        if (ttl) {
            *response->mutable_expired_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ttl.MilliSeconds());
        }
        const auto writeQuery = writeQueryBuilder.Build();
        return std::make_pair(writeQuery.Sql, writeQuery.Params);
    };
    const auto readQuery = readQueryBuilder.Build();
    return std::make_tuple(readQuery.Sql, readQuery.Params, prepareParams);
}

std::tuple<TString, TParams, const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>> ConstructSoftPingTask(
    const Fq::Private::PingTaskRequest& request, std::shared_ptr<Fq::Private::PingTaskResult> response,
    const TString& tablePathPrefix, const TDuration& taskLeaseTtl) {
    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "SoftPingTask(read)");
    readQueryBuilder.AddString("tenant", request.tenant());
    readQueryBuilder.AddString("scope", request.scope());
    readQueryBuilder.AddString("query_id", request.query_id().value());
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
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request.tenant() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request.owner_id()) {
                ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request.query_id().value() << "\" MISMATCHED: \"" << request.owner_id() << "\" (received) != \"" << owner << "\" (selected)";
            }
        }

        TInstant ttl = TInstant::Now() + taskLeaseTtl;
        response->set_action(internal.action());
        *response->mutable_expired_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ttl.MilliSeconds());

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "SoftPingTask(write)");
        writeQueryBuilder.AddTimestamp("now", TInstant::Now());
        writeQueryBuilder.AddTimestamp("ttl", ttl);
        writeQueryBuilder.AddString("tenant", request.tenant());
        writeQueryBuilder.AddString("scope", request.scope());
        writeQueryBuilder.AddString("query_id", request.query_id().value());

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
    Fq::Private::PingTaskRequest& request = ev->Get()->Request;
    const TString cloudId = "";
    const TString scope = request.scope();
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters("" /*CloudId*/, scope, RTS_PING_TASK);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(ev->Get()->GetByteSize());
    const TString queryId = request.query_id().value();
    const TString owner = request.owner_id();
    const TInstant deadline = NProtoInterop::CastFromProto(request.deadline());

    CPS_LOG_T("PingTaskRequest: {" << request.DebugString() << "}");

    NYql::TIssues issues = ValidatePingTask(scope, queryId, owner, deadline, Config.ResultSetsTtl);
    if (issues) {
        CPS_LOG_W("PingTaskRequest: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvPingTaskResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(PingTaskRequest, queryId, delta, false);
        return;
    }

    std::shared_ptr<Fq::Private::PingTaskResult> response = std::make_shared<Fq::Private::PingTaskResult>();

    if (request.status())
        Counters.GetFinalStatusCounters(cloudId, scope)->IncByStatus(request.status());
    auto pingTaskParams = DoesPingTaskUpdateQueriesTable(request) ?
        ConstructHardPingTask(request, response, YdbConnection->TablePathPrefix, Config.AutomaticQueriesTtl, Config.TaskLeaseTtl, Config.RetryPolicies, Counters.Counters) :
        ConstructSoftPingTask(request, response, YdbConnection->TablePathPrefix, Config.TaskLeaseTtl);
    auto readQuery = std::get<0>(pingTaskParams); // Use std::get for win compiler
    auto readParams = std::get<1>(pingTaskParams);
    auto prepareParams = std::get<2>(pingTaskParams);

    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), readQuery, readParams, prepareParams, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvPingTaskResponse, Fq::Private::PingTaskResult>(
        "PingTaskRequest - PingTaskResult",
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
