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
    const TString& tablePathPrefix, const TDuration& automaticQueriesTtl) {
 
    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "HardPingTask(read)"); 
    readQueryBuilder.AddString("scope", request->Scope);
    readQueryBuilder.AddString("query_id", request->QueryId);
    readQueryBuilder.AddText(
        "$last_job_id = SELECT `" LAST_JOB_ID_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n" 
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n" 
        "SELECT `" QUERY_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" QUERIES_TABLE_NAME "`\n" 
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n" 
        "SELECT `" JOB_ID_COLUMN_NAME "`, `" JOB_COLUMN_NAME "` FROM `" JOBS_TABLE_NAME "`\n" 
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" JOB_ID_COLUMN_NAME "` = $last_job_id;\n" 
        "SELECT `" OWNER_COLUMN_NAME "` FROM `" PENDING_SMALL_TABLE_NAME "`\n" 
        "   WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n" 
    );
 
    auto prepareParams = [=](const TVector<TResultSet>& resultSets) { 
        TString jobId; 
        YandexQuery::Query query; 
        YandexQuery::Internal::QueryInternal internal; 
        YandexQuery::Job job; 
        TString selectedOwner; 
 
        if (resultSets.size() != 3) { 
           ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 3 but equal " << resultSets.size() << ". Please contact internal support"; 
        } 
 
        { 
            TResultSetParser parser(resultSets[0]); 
            if (parser.TryNextRow()) { 
                if (!query.ParseFromString(*parser.ColumnParser(QUERY_COLUMN_NAME).GetOptionalString())) { 
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query. Please contact internal support"; 
                } 
                if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) { 
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for query internal. Please contact internal support"; 
                } 
            } 
        } 
 
        { 
            TResultSetParser parser(resultSets[1]); 
            if (parser.TryNextRow()) { 
                if (!job.ParseFromString(*parser.ColumnParser(JOB_COLUMN_NAME).GetOptionalString())) { 
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for job. Please contact internal support"; 
                } 
                jobId = *parser.ColumnParser(JOB_ID_COLUMN_NAME).GetOptionalString(); 
            } 
        } 
 
        { 
            TResultSetParser parser(resultSets[2]); 
            if (parser.TryNextRow()) { 
                selectedOwner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString(); 
            } 
        } 
 
        if (selectedOwner != request->Owner) { 
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Query with the specified Owner: \"" <<  request->Owner << "\" does not exist. Selected owner: \"" << selectedOwner << "\""; 
        } 
 
        if (request->Status) { 
            query.mutable_meta()->set_status(*request->Status); 
            job.mutable_query_meta()->set_status(*request->Status); 
        } 
 
        if (request->Issues) { 
            NYql::IssuesToMessage(*request->Issues, query.mutable_issue()); 
            NYql::IssuesToMessage(*request->Issues, job.mutable_issue()); 
        } 
 
        if (request->TransientIssues) { 
            NYql::TIssues issues = *request->TransientIssues; 
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
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
            );
        } else {
            // update pending small 
            writeQueryBuilder.AddTimestamp("now", request->ResignQuery ? TInstant::Zero() : TInstant::Now());
            const TString updateResignQueryFlag = request->ResignQuery ? ", `" IS_RESIGN_QUERY_COLUMN_NAME "` = true" : "";
            writeQueryBuilder.AddText(
                "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" LAST_SEEN_AT_COLUMN_NAME "` = $now " + updateResignQueryFlag + "\n"
                "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n"
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
    const TString& tablePathPrefix) { 
    TSqlQueryBuilder readQueryBuilder(tablePathPrefix, "SoftPingTask(read)"); 
    readQueryBuilder.AddString("scope", request->Scope);
    readQueryBuilder.AddString("query_id", request->QueryId);
    readQueryBuilder.AddText(
        "SELECT `" INTERNAL_COLUMN_NAME "`\n" 
        "FROM `" QUERIES_TABLE_NAME "` WHERE `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" SCOPE_COLUMN_NAME "` = $scope;\n" 
        "SELECT `" OWNER_COLUMN_NAME "`\n" 
        "FROM `" PENDING_SMALL_TABLE_NAME "` WHERE `" QUERY_ID_COLUMN_NAME "` = $query_id AND `" SCOPE_COLUMN_NAME "` = $scope;\n" 
    );
 
    auto prepareParams = [=](const TVector<TResultSet>& resultSets) { 
        TString selectedOwner; 
        YandexQuery::Internal::QueryInternal internal; 
 
        if (resultSets.size() != 2) { 
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 2 but equal " << resultSets.size() << ". Please contact internal support"; 
        } 
 
        { 
            TResultSetParser parser(resultSets[0]); 
            if (parser.TryNextRow()) { 
                if (!internal.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) { 
                    ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for pending internal query. Please contact internal support"; 
                } 
            } 
        } 
 
        { 
            TResultSetParser parser(resultSets[1]); 
            if (parser.TryNextRow()) { 
                selectedOwner = *parser.ColumnParser(OWNER_COLUMN_NAME).GetOptionalString(); 
            } 
        } 
        *response = internal.action(); 
 
        if (selectedOwner != request->Owner) { 
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "query with the specified Owner: \"" <<  request->Owner << "\" does not exist. Selected owner: \"" << selectedOwner << "\""; 
        } 
 
        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "SoftPingTask(write)"); 
        writeQueryBuilder.AddTimestamp("now", request->ResignQuery ? TInstant::Zero() : TInstant::Now());
        writeQueryBuilder.AddString("scope", request->Scope);
        writeQueryBuilder.AddString("query_id", request->QueryId);
        writeQueryBuilder.AddString("owner", request->Owner);
 
        const TString updateResignQueryFlag = request->ResignQuery ? ", `" IS_RESIGN_QUERY_COLUMN_NAME "` = true" : ""; 
        writeQueryBuilder.AddText(
            "UPDATE `" PENDING_SMALL_TABLE_NAME "` SET `" LAST_SEEN_AT_COLUMN_NAME "` = $now " + updateResignQueryFlag + "\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" QUERY_ID_COLUMN_NAME "` = $query_id;\n" 
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
    TRequestCountersPtr requestCounters = Counters.Requests[RT_PING_TASK]; 
    requestCounters->InFly->Inc(); 
 
    TEvControlPlaneStorage::TEvPingTaskRequest* request = ev->Get(); 
    const TString scope = request->Scope; 
    const TString queryId = request->QueryId; 
    const TString owner = request->Owner; 
    const TInstant deadline = request->Deadline;
 
    CPS_LOG_T("PingTaskRequest: " << scope << " " << queryId 
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
        FinalStatusCounters.IncByStatus(*request->Status); 
    auto pingTaskParams = DoesPingTaskUpdateQueriesTable(request) ? 
        ConstructHardPingTask(request, response, YdbConnection->TablePathPrefix, Config.AutomaticQueriesTtl) :
        ConstructSoftPingTask(request, response, YdbConnection->TablePathPrefix); 
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
