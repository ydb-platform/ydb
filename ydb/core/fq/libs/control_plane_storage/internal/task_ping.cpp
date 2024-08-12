#include "utils.h"

#include <util/datetime/base.h>

#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/metering/metering.h>

#include <ydb/library/protobuf_printer/size_printer.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>

#include <google/protobuf/util/time_util.h>

#include <util/system/hostname.h>

namespace NFq {

namespace {

bool HasIssuesCode(const NYql::TIssues& issues, ::NYql::TIssuesIds::EIssueCode code) {
    for (const auto& issue: issues) {
        bool found = false;
        NYql::WalkThroughIssues(issue, false, [&found, code](const auto& issue, ui16) {
            if (issue.GetCode() == static_cast<NYql::TIssueCode>(code)) {
                found = true;
            }
        });
        if (found) {
            return found;
        }
    }
    return false;
}

THashMap<TString, i64> DeserializeFlatStats(const google::protobuf::RepeatedPtrField<Ydb::ValuePair>& src) {
    THashMap<TString, i64> stats;
    for (const auto& stat_pair : src) {
        if (stat_pair.key().has_text_value() && stat_pair.payload().has_int64_value()) {
            stats[stat_pair.key().text_value()] = stat_pair.payload().int64_value();
        }
    }
    return stats;
}

}

struct TPingTaskParams {
    TString Query;
    TParams Params;
    const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)> Prepare;
    std::shared_ptr<std::vector<TString>> MeteringRecords;
};

struct TFinalStatus {
    FederatedQuery::QueryMeta::ComputeStatus Status = FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED;
    NYql::NDqProto::StatusIds::StatusCode StatusCode = NYql::NDqProto::StatusIds::UNSPECIFIED;
    FederatedQuery::QueryContent::QueryType QueryType = FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED;
    NYql::TIssues Issues;
    NYql::TIssues TransientIssues;
    StatsValuesList FinalStatistics;
    TString CloudId;
    TString JobId;
};

TPingTaskParams ConstructHardPingTask(
    const Fq::Private::PingTaskRequest& request, std::shared_ptr<Fq::Private::PingTaskResult> response,
    const TString& tablePathPrefix, const TDuration& automaticQueriesTtl, const TDuration& taskLeaseTtl,
    const THashMap<ui64, TRetryPolicyItem>& retryPolicies, ::NMonitoring::TDynamicCounterPtr rootCounters,
    uint64_t maxRequestSize, bool dumpRawStatistics, const std::shared_ptr<TFinalStatus>& finalStatus,
    const TRequestCommonCountersPtr& commonCounters) {

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

    auto meteringRecords = std::make_shared<std::vector<TString>>();

    auto prepareParams = [=, counters=counters, actorSystem = NActors::TActivationContext::ActorSystem(), request=request](const TVector<TResultSet>& resultSets) mutable {
        TString jobId;
        FederatedQuery::Query query;
        FederatedQuery::Internal::QueryInternal internal;
        FederatedQuery::Job job;
        TString owner;

        if (resultSets.size() != 3) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "RESULT SET SIZE of " << resultSets.size() << " != 3";
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." QUERY_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " JOBS_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " JOBS_TABLE_NAME "." JOB_COLUMN_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"";
            }
            jobId = *parser.ColumnParser(JOB_ID_COLUMN_NAME).GetOptionalString();
        }

        TRetryLimiter retryLimiter;
        {
            TResultSetParser parser(resultSets[2]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request.tenant() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request.owner_id()) {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request.query_id().value() << "\" MISMATCHED: \"" << request.owner_id() << "\" (received) != \"" << owner << "\" (selected)";
            }
            retryLimiter.Assign(
                parser.ColumnParser(RETRY_COUNTER_COLUMN_NAME).GetOptionalUint64().GetOrElse(0),
                parser.ColumnParser(RETRY_COUNTER_UPDATE_COLUMN_NAME).GetOptionalTimestamp().GetOrElse(TInstant::Zero()),
                parser.ColumnParser(RETRY_RATE_COLUMN_NAME).GetOptionalDouble().GetOrElse(0.0)
            );
        }

        TMaybe<FederatedQuery::QueryMeta::ComputeStatus> queryStatus;
        if (request.status() != FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED) {
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
            if (request.status_code() == NYql::NDqProto::StatusIds::UNSPECIFIED && internal.pending_status_code() != NYql::NDqProto::StatusIds::UNSPECIFIED) {
                request.set_status_code(internal.pending_status_code());
                internal.clear_pending_status_code();
                internal.clear_execution_id();
                internal.clear_operation_id();
            }

            TRetryPolicyItem policy(0, 0, TDuration::Seconds(1), TDuration::Zero());
            auto it = retryPolicies.find(request.status_code());
            auto policyFound = it != retryPolicies.end();
            if (policyFound) {
                policy = it->second;
            }

            if (retryLimiter.UpdateOnRetry(Now(), policy)) {
                queryStatus.Clear();
                // failing query is throttled for backoff period
                backoff = policy.BackoffPeriod * (retryLimiter.RetryRate + 1);
                owner = "";
                if (!transientIssues) {
                    transientIssues.ConstructInPlace();
                }
                TStringBuilder builder;
                builder << "Query failed with code " << NYql::NDqProto::StatusIds_StatusCode_Name(request.status_code())
                    << " and will be restarted (RetryCount: " << retryLimiter.RetryCount << ")"
                    << " at " << Now();
                transientIssues->AddIssue(NYql::TIssue(builder));
            } else {
                // failure query should be processed instantly
                queryStatus = FederatedQuery::QueryMeta::FAILING;
                backoff = TDuration::Zero();
                TStringBuilder builder;
                builder << "Query failed with code " << NYql::NDqProto::StatusIds_StatusCode_Name(request.status_code());
                if (policy.RetryCount) {
                    builder << " (" << retryLimiter.LastError << ")";
                }
                builder << " at " << Now();

                // in case of problems with finalization, do not change the issues
                if (query.meta().status() == FederatedQuery::QueryMeta::FAILING || query.meta().status() == FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM || query.meta().status() == FederatedQuery::QueryMeta::ABORTING_BY_USER) {
                    if (issues) {
                        transientIssues->AddIssues(*issues);
                    }
                    transientIssues->AddIssue(NYql::TIssue(builder));
                } else {
                    if (!issues) {
                        issues.ConstructInPlace();
                    }
                    auto issue = NYql::TIssue(builder);
                    if (query.issue().size() > 0 && request.issues().empty()) {
                        NYql::TIssues queryIssues;
                        NYql::IssuesFromMessage(query.issue(), queryIssues);
                        for (auto& subIssue : queryIssues) {
                            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
                        }
                    }
                    if (transientIssues) {
                        for (auto& subIssue : *transientIssues) {
                            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
                        }
                        transientIssues.Clear();
                    }
                    issues->AddIssue(issue);
                }
            }
            CPS_LOG_AS_D(*actorSystem, "PingTaskRequest (resign): " << (!policyFound ? " DEFAULT POLICY" : "") << (owner ? " FAILURE " : " ") << NYql::NDqProto::StatusIds_StatusCode_Name(request.status_code()) << " " << retryLimiter.RetryCount << " " << retryLimiter.RetryCounterUpdatedAt << " " << backoff);
        }

        if (queryStatus) {
            query.mutable_meta()->set_status(*queryStatus);
            job.mutable_query_meta()->set_status(*queryStatus);
        }

        if (request.status_code() != NYql::NDqProto::StatusIds::UNSPECIFIED) {
            internal.set_status_code(request.status_code());
        }

        if (request.pending_status_code() != NYql::NDqProto::StatusIds::UNSPECIFIED) {
            internal.set_pending_status_code(request.pending_status_code());
        }

        if (issues) {
            NYql::IssuesToMessage(*issues, query.mutable_issue());
            NYql::IssuesToMessage(*issues, job.mutable_issue());
        }

        if (transientIssues) {
            AddTransientIssues(query.mutable_transient_issue(), std::move(*transientIssues));
        }

        if (request.internal_issues().size()) {
            *internal.mutable_internal_issue() = request.internal_issues();
        }

        if (request.statistics()) {
            TString statistics = request.statistics();
            if (request.flat_stats_size() == 0) {
                internal.clear_statistics();
                // TODO: remove once V1 and V2 stats go the same way
                PackStatisticsToProtobuf(*internal.mutable_statistics(), statistics, TInstant::Now() - NProtoInterop::CastFromProto(job.meta().created_at()));
            }

            // global dumpRawStatistics will be removed with YQv1
            if (!dumpRawStatistics && !request.dump_raw_statistics()) {
                try {
                    statistics = GetPrettyStatistics(statistics);
                } catch (const std::exception&) {
                    // LOG_AS?
                    CPS_LOG_E("Error on statistics prettification: " << CurrentExceptionMessage());
                }
            }
            *query.mutable_statistics()->mutable_json() = statistics;
            *job.mutable_statistics()->mutable_json() = statistics;
        }

        if (request.current_load()) {
            internal.set_current_load(request.current_load());
        }

        if (request.timeline()) {
            internal.set_timeline(request.timeline());
        }

        if (request.flat_stats_size() != 0) {
            internal.clear_statistics();
            auto stats = DeserializeFlatStats(request.flat_stats());
            PackStatisticsToProtobuf(*internal.mutable_statistics(), stats, TInstant::Now() - NProtoInterop::CastFromProto(job.meta().created_at()));
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

        if (query.meta().status() == FederatedQuery::QueryMeta::COMPLETED) {
            *query.mutable_meta()->mutable_result_expire_at() = request.deadline();
        }

        if (request.state_load_mode()) {
            internal.set_state_load_mode(request.state_load_mode());
            if (request.state_load_mode() == FederatedQuery::FROM_LAST_CHECKPOINT) { // Saved checkpoint
                query.mutable_meta()->set_has_saved_checkpoints(true);
            }
        }

        if (request.has_disposition()) {
            *internal.mutable_disposition() = request.disposition();
        }

        if (request.status() && IsTerminalStatus(request.status())) {
            internal.clear_created_topic_consumers();
            // internal.clear_dq_graph(); keep for debug
            internal.clear_dq_graph_index();
            // internal.clear_execution_id(); keep for debug
            // internal.clear_operation_id(); keep for debug
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

        if (!request.execution_id().empty()) {
            internal.set_execution_id(request.execution_id());
        }

        if (!request.operation_id().empty()) {
            internal.set_operation_id(request.operation_id());
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

        if (request.has_resources()) {
            *internal.mutable_resources() = request.resources();
        }

        if (job.ByteSizeLong() > maxRequestSize) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Job proto exceeded the size limit: " << job.ByteSizeLong() << " of " << maxRequestSize << " " << TSizeFormatPrinter(job).ToString();
        }

        if (query.ByteSizeLong() > maxRequestSize) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Query proto exceeded the size limit: " << query.ByteSizeLong() << " of " << maxRequestSize << " " << TSizeFormatPrinter(query).ToString();
        }

        if (internal.ByteSizeLong() > maxRequestSize) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "QueryInternal proto exceeded the size limit: " << internal.ByteSizeLong() << " of " << maxRequestSize << " " << TSizeFormatPrinter(internal).ToString();
        }

        finalStatus->Status = query.meta().status();
        finalStatus->QueryType = query.content().type();
        finalStatus->StatusCode = internal.status_code();
        finalStatus->CloudId = internal.cloud_id();
        finalStatus->JobId = jobId;
        NYql::IssuesFromMessage(query.issue(), finalStatus->Issues);
        NYql::IssuesFromMessage(query.transient_issue(), finalStatus->TransientIssues);

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
        if (query.meta().status() == FederatedQuery::QueryMeta::COMPLETED) {
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

        TString updateResultId;
        if (request.has_result_id()) {
            updateResultId = "`" RESULT_ID_COLUMN_NAME "` = $result_id, ";
        }

        writeQueryBuilder.AddText(
            "UPSERT INTO `" JOBS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" QUERY_ID_COLUMN_NAME "`, `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "`) VALUES($scope, $query_id, $job_id, $job);\n"
            "UPDATE `" QUERIES_TABLE_NAME "` SET `" QUERY_COLUMN_NAME "` = $query, `" STATUS_COLUMN_NAME "` = $status, `" INTERNAL_COLUMN_NAME "` = $internal, " + updateResultId + updateResultSetsExpire + ", " + updateQueryTtl + ", `" META_REVISION_COLUMN_NAME  "` = `" META_REVISION_COLUMN_NAME "` + 1\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
        );

        response->set_action(internal.action());
        if (ttl) {
            *response->mutable_expired_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(ttl.MilliSeconds());
        }
        const auto writeQuery = writeQueryBuilder.Build();

        if (IsTerminalStatus(request.status())) {
            try {
                auto isBillable = IsBillablelStatus(request.status(), internal.status_code());
                if (!isBillable) {
                    CPS_LOG_AS_N(*actorSystem, "Query " << request.query_id().value() << " is NOT billable, status: "
                    << FederatedQuery::QueryMeta::ComputeStatus_Name(request.status())
                    << ", statusCode: " << NYql::NDqProto::StatusIds_StatusCode_Name(internal.status_code()));
                }
                auto statistics = request.statistics();
                if (!statistics) {
                    // YQv2 may not provide statistics with terminal status, use saved one
                    statistics = query.statistics().json();
                }
                finalStatus->FinalStatistics = ExtractStatisticsFromProtobuf(internal.statistics());
                finalStatus->FinalStatistics.push_back(std::make_pair("IsAutomatic", query.content().automatic()));
                if (query.content().name().Contains("DataLens YQ query")) {
                    finalStatus->FinalStatistics.push_back(std::make_pair("IsDataLens", 1));
                } else if (query.content().name().Contains("Audit-trails")) {
                    finalStatus->FinalStatistics.push_back(std::make_pair("IsAuditTrails", 1));
                } else if (query.content().name().Contains("Query from YDB SDK")) {
                    finalStatus->FinalStatistics.push_back(std::make_pair("IsSDK", 1));
                }
                finalStatus->FinalStatistics.push_back(std::make_pair("RetryCount", retryLimiter.RetryCount));
                finalStatus->FinalStatistics.push_back(std::make_pair("RetryRate", retryLimiter.RetryRate * 100));
                finalStatus->FinalStatistics.push_back(std::make_pair("Load", internal.current_load()));

                auto records = GetMeteringRecords(statistics, isBillable, jobId, request.scope(), HostName());
                meteringRecords->swap(records);
            } catch (const std::exception&) {
                CPS_LOG_AS_E(*actorSystem, "Error on statistics meterification: " << CurrentExceptionMessage());
            }
        }

        return std::make_pair(writeQuery.Sql, writeQuery.Params);
    };
    const auto readQuery = readQueryBuilder.Build();

    return {readQuery.Sql, readQuery.Params, prepareParams, meteringRecords};
}

TPingTaskParams ConstructSoftPingTask(
    const Fq::Private::PingTaskRequest& request, std::shared_ptr<Fq::Private::PingTaskResult> response,
    const TString& tablePathPrefix, const TDuration& taskLeaseTtl, const TRequestCommonCountersPtr& commonCounters) {
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
        FederatedQuery::Internal::QueryInternal internal;

        if (resultSets.size() != 2) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "RESULT SET SIZE of " << resultSets.size() << " != 2";
        }

        {
            TResultSetParser parser(resultSets[0]);
            if (!parser.TryNextRow()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " QUERIES_TABLE_NAME " where " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "ERROR PARSING " QUERIES_TABLE_NAME "." INTERNAL_COLUMN_NAME " where " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\"";
            }
        }

        {
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "NOT FOUND " PENDING_SMALL_TABLE_NAME " where " TENANT_COLUMN_NAME " = \"" << request.tenant() << "\" and " SCOPE_COLUMN_NAME " = \"" << request.scope() << "\" and " QUERY_ID_COLUMN_NAME " = \"" << request.query_id().value() << "\"" ;
            }
            owner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString();
            if (owner != request.owner_id()) {
                ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "OWNER of QUERY ID = \"" << request.query_id().value() << "\" MISMATCHED: \"" << request.owner_id() << "\" (received) != \"" << owner << "\" (selected)";
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
    return {readQuery.Sql, readQuery.Params, prepareParams, std::shared_ptr<std::vector<TString>>{}};
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvPingTaskRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    Fq::Private::PingTaskRequest& request = ev->Get()->Request;
    const TString scope = request.scope();
    TRequestCounters requestCounters = Counters.GetCounters("" /*CloudId*/, scope, RTS_PING_TASK, RTC_PING_TASK);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(ev->Get()->GetByteSize());
    const TString queryId = request.query_id().value();
    const TString owner = request.owner_id();
    const TInstant deadline = NProtoInterop::CastFromProto(request.deadline());
    const TString tenant = request.tenant();

    CPS_LOG_T("PingTaskRequest: {" << request.DebugString() << "}");

    NYql::TIssues issues = ValidatePingTask(scope, queryId, owner, deadline, Config->ResultSetsTtl);

    auto tenantInfo = ev->Get()->TenantInfo;
    if (tenantInfo && tenantInfo->TenantState.Value(tenant, TenantState::Active) == TenantState::Idle) {
        issues.AddIssue("Tenant is idle, no processing is allowed");
    }

    if (issues) {
        CPS_LOG_W("PingTaskRequest: {" << request.DebugString() << "} validation FAILED: " << issues.ToOneLineString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvPingTaskResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(PingTaskRequest, queryId, delta, false);
        return;
    }

    std::shared_ptr<Fq::Private::PingTaskResult> response = std::make_shared<Fq::Private::PingTaskResult>();
    std::shared_ptr<TFinalStatus> finalStatus = std::make_shared<TFinalStatus>();

    auto pingTaskParams = DoesPingTaskUpdateQueriesTable(request) ?
        ConstructHardPingTask(request, response, YdbConnection->TablePathPrefix, Config->AutomaticQueriesTtl,
            Config->TaskLeaseTtl, Config->RetryPolicies, Counters.Counters, Config->Proto.GetMaxRequestSize(),
            Config->Proto.GetDumpRawStatistics(), finalStatus, requestCounters.Common) :
        ConstructSoftPingTask(request, response, YdbConnection->TablePathPrefix, Config->TaskLeaseTtl, requestCounters.Common);
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(pingTaskParams.Query, pingTaskParams.Params, pingTaskParams.Prepare, requestCounters, debugInfo);
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

    success.Apply([=, actorSystem=NActors::TActivationContext::ActorSystem(), meteringRecords=pingTaskParams.MeteringRecords](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            const auto success = future.GetValue();
            LWPROBE(PingTaskRequest, queryId, delta, success);
            if (meteringRecords) {
                for (const auto& metric : *meteringRecords) {
                    actorSystem->Send(NKikimr::NMetering::MakeMeteringServiceID(), new NKikimr::NMetering::TEvMetering::TEvWriteMeteringJson(metric));
                }
            }

            if (success) {
                actorSystem->Send(ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvFinalStatusReport(
                    request.query_id().value(), finalStatus->JobId, finalStatus->CloudId, scope, std::move(finalStatus->FinalStatistics),
                    finalStatus->Status, finalStatus->StatusCode, finalStatus->QueryType, finalStatus->Issues, finalStatus->TransientIssues));
            }
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvFinalStatusReport::TPtr& ev) {
    const auto& event = *ev->Get();
    if (!IsTerminalStatus(event.Status)) {
        return;
    }

    if (IsFailedStatus(event.Status)) {
        FailedStatusCodeCounters->IncByScopeAndStatusCode(event.Scope, event.StatusCode, event.Issues);
        LOG_YQ_AUDIT_SERVICE_INFO("FinalFailedStatus: cloud id: [" << event.CloudId  << "], scope: [" << event.Scope << "], query id: [" <<
                                event.QueryId << "], job id: [" << event.JobId << "], query type: [" << FederatedQuery::QueryContent::QueryType_Name(event.QueryType) << "], "
                                "status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(event.Status) <<
                                ", label: " << LabelNameFromStatusCodeAndIssues(event.StatusCode, event.Issues) <<
                                ", status code: " << NYql::NDqProto::StatusIds::StatusCode_Name(event.StatusCode) <<
                                ", issues: " << event.Issues.ToOneLineString() <<
                                ", transient issues " << event.TransientIssues.ToOneLineString());
    }

    if (HasIssuesCode(event.Issues, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE) || HasIssuesCode(event.TransientIssues, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE)) {
        Counters.GetFinalStatusCounters(event.CloudId, event.Scope)->Unavailable->Inc();
    }

    Counters.GetFinalStatusCounters(event.CloudId, event.Scope)->IncByStatus(event.Status);

    Statistics statistics{event.Statistics};
    LOG_YQ_AUDIT_SERVICE_INFO("FinalStatus: cloud id: [" << event.CloudId  << "], scope: [" << event.Scope << "], query id: [" <<
                              event.QueryId << "], job id: [" << event.JobId << "], query type: [" << FederatedQuery::QueryContent::QueryType_Name(event.QueryType) << "], " << statistics << ", " <<
                              "status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(event.Status));
}


} // NFq
