#include "ydb_service_experimental.h"


#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/lib/fq/fq.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <contrib/libs/protobuf/src/google/protobuf/util/time_util.h>
#include <util/generic/guid.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#include <iomanip>

namespace {

template<typename T>
T CreateFqSettings(const TString& scope)
{
    T settings;
    settings.Header_ = {
        { "x-ydb-fq-project",  scope }
    };
    return settings;
}

// Prints protobuf message DebugString() only when verbose mode is enabled. Used to avoid
// leaking sensitive request fields (passwords, OAuth tokens, IAM tokens, etc.) to stdout/logs
// by default while still letting users inspect the full request via `-v`/`--verbose`.
template <typename TRequest>
void PrintRequestIfVerbose(const NYdb::NConsoleClient::TClientCommand::TConfig& config, const TRequest& request) {
    if (config.IsVerbose()) {
        Cout << "Request:" << Endl << request.DebugString() << Endl;
    }
}

TString SnakeToCamelCase(TString name) {
    name[0] = tolower(name[0]);
    size_t max = name.size() - 1;
    for (size_t i = 1; i < max;) {
        if (name[i] == '_') {
            name[i] = toupper(name[i + 1]);
            name.erase(i + 1, 1);
            max--;
        } else {
            ++i;
        }
    }
    return name;
}

NProtoBuf::Timestamp MicroSecondToProtoTimeStamp(ui64 microseconds)
{
    NProtoBuf::Timestamp timestamp;
    timestamp.set_seconds((i64)(microseconds / 1000000));
    timestamp.set_nanos((i32)((microseconds % 1000000) * 1000));
    return timestamp;
}

NProtoBuf::Duration MicroSecondToProtoDuration(ui64 microseconds)
{
    NProtoBuf::Duration duration;
    duration.set_seconds((i64)(microseconds / 1000000));
    duration.set_nanos((i32)((microseconds % 1000000) * 1000));
    return duration;
}

TString SnakeToCamelCaseProtoConverter(const google::protobuf::FieldDescriptor& field) {
    return SnakeToCamelCase(field.name());
}

static NProtobufJson::TJson2ProtoConfig Json2ProtoConfig = NProtobufJson::TJson2ProtoConfig()
    .SetNameGenerator(SnakeToCamelCaseProtoConverter)
    .SetMapAsObject(true);

} // namespace

namespace NYdb {
namespace NConsoleClient {

using namespace NFq;

TCommandFederatedQueryTree::TCommandFederatedQueryTree()
    : TClientCommandTree("yq", {}, "Yandex Query tree")
{
    AddCommand(std::make_unique<TCommandFederatedQueryQueryTree>());
    AddCommand(std::make_unique<TCommandFederatedQueryConnectionTree>());
    AddCommand(std::make_unique<TCommandFederatedQueryBindingTree>());
    AddCommand(std::make_unique<TCommandFederatedQueryJobTree>());
}

TCommandFederatedQueryQueryTree::TCommandFederatedQueryQueryTree()
    : TClientCommandTree("query", {}, "Commands to manage queries")
{
    AddCommand(std::make_unique<TCommandFederatedQueryCreateQuery>());
    AddCommand(std::make_unique<TCommandFederatedQueryListQueries>());
    AddCommand(std::make_unique<TCommandFederatedQueryDescribeQuery>());
    AddCommand(std::make_unique<TCommandFederatedQueryGetQueryStatus>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyQuery>());
    AddCommand(std::make_unique<TCommandFederatedQueryDeleteQuery>());
    AddCommand(std::make_unique<TCommandFederatedQueryControlQuery>());
    AddCommand(std::make_unique<TCommandFederatedQueryQueryJobTree>());
    AddCommand(std::make_unique<TCommandFederatedQueryQueryResultTree>());
}

TCommandFederatedQueryQueryJobTree::TCommandFederatedQueryQueryJobTree()
    : TClientCommandTree("job", {}, "Commands to manage jobs")
{
    AddCommand(std::make_unique<TCommandFederatedQueryQueryListJobs>());
}

TCommandFederatedQueryQueryResultTree::TCommandFederatedQueryQueryResultTree()
    : TClientCommandTree("result", {}, "Commands to manage results")
{
    AddCommand(std::make_unique<TCommandFederatedQueryGetResultData>());
}

TCommandFederatedQueryJobTree::TCommandFederatedQueryJobTree()
    : TClientCommandTree("job", {}, "Commands to manage jobs")
{
    AddCommand(std::make_unique<TCommandFederatedQueryListJobs>());
    AddCommand(std::make_unique<TCommandFederatedQueryDescribeJob>());
}

TCommandFederatedQueryConnectionTree::TCommandFederatedQueryConnectionTree()
    : TClientCommandTree("connection", {}, "Commands to manage connections")
{
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnection>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnection>());
    AddCommand(std::make_unique<TCommandFederatedQueryListConnections>());
    AddCommand(std::make_unique<TCommandFederatedQueryDescribeConnection>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnection>());
    AddCommand(std::make_unique<TCommandFederatedQueryDeleteConnection>());
    AddCommand(std::make_unique<TCommandFederatedQueryConnectionBindingTree>());
}

TCommandFederatedQueryConnectionBindingTree::TCommandFederatedQueryConnectionBindingTree()
    : TClientCommandTree("binding", {}, "Bindings for connection")
{
    AddCommand(std::make_unique<TCommandFederatedQueryConnectionListBindings>());
}

TCommandFederatedQueryBindingTree::TCommandFederatedQueryBindingTree()
    : TClientCommandTree("binding", {}, "Commands to manage bindings")
{
    AddCommand(std::make_unique<TCommandFederatedQueryCreateBinding>());
    AddCommand(std::make_unique<TCommandFederatedQueryListBindings>());
    AddCommand(std::make_unique<TCommandFederatedQueryDescribeBinding>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyBinding>());
    AddCommand(std::make_unique<TCommandFederatedQueryDeleteBinding>());
}

template<typename T>
void ProcessProtoResult(const T& res)
{
    if (res.GetStatus() == NYdb::EStatus::SUCCESS) {
        Cout << "Success" << Endl;
        SerializeToTextFormat(res.GetResult(), Cout);
    } else {
        Cout << "Failed" << Endl;
        Cout << res.GetIssues().ToString();
    }
}

TCommandFederatedQueryCreateQuery::TCommandFederatedQueryCreateQuery()
    : TYdbCommand("create", {}, "Execute YQL query")
{}

void TCommandFederatedQueryCreateQuery::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('t', "type", "Type of query (ANALYTICS, STREAMING) (default: \"ANALYTICS\")")
        .StoreResult(&Type);
    config.Opts->AddLongOption('n', "name", "Query name").StoreResult(&Name);
    config.Opts->AddLongOption('s', "script", "Script").StoreResult(&Content);
    config.Opts->AddLongOption("file", "Script file").StoreResult(&ContentFile);
    config.Opts->AddLongOption('m', "mode", "Execution mode (SAVE, PARSE, COMPILE, VALIDATE, EXPLAIN, RUN) (default: \"RUN\")")
        .StoreResult(&Mode);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption('d', "disposition", "Disposition for sources of streaming queries (OLDEST, FRESH, FROM_TIME, TIME_AGO, FROM_LAST_CHECKPOINT) (default: \"FROM_LAST_CHECKPOINT\")")
        .StoreResult(&Disposition);
    config.Opts->AddLongOption("disposition-timestamp", "Timestamp that will be used in disposition")
        .StoreResult(&DispositionTimestamp);
    config.Opts->AddLongOption("disposition-duration", "Duration that will be used in disposition")
        .StoreResult(&DispositionDuration);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption('j', "json", "Json file").StoreResult(&JsonFile);
    config.Opts->AddLongOption("automatic", "Flag that indicates that an automatic query will be created").NoArgument().StoreTrue(&Automatic);
    config.Opts->AddLongOption('w', "wait", "Wait result data").NoArgument().StoreTrue(&Wait);
    config.Opts->AddLongOption("skip-result", "Do not print query results").NoArgument().StoreTrue(&SkipResult);
}

void TCommandFederatedQueryCreateQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

void TCommandFederatedQueryCreateQuery::PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Query& query)
{
    TResultSetPrinter printer(OutputFormat);
    Cout << "query id: " << query.meta().common().id() << Endl;
    Cout << "result set size: " << query.result_set_meta().size()<< Endl;
    auto from = query.meta().common().created_at();
    auto to = query.meta().finished_at();
    Cout << "elapsed time: " << google::protobuf::util::TimeUtil::ToString(to - from) << Endl;
    for (int64_t i = 0; i < query.result_set_meta().size(); ++i) {
        Cout << "result sets #" << i << Endl;
        const auto& resultSetMeta = query.result_set_meta(i);
        const int64_t limit = 10;
        for (int64_t offset = 0; offset < resultSetMeta.rows_count(); offset += limit) {
            FederatedQuery::GetResultDataRequest request;
            request.set_query_id(query.meta().common().id());
            request.set_result_set_index(i);
            request.set_offset(offset);
            request.set_limit(limit);
            const auto& response = client.GetResultData(request, CreateFqSettings<NYdb::NFq::TGetResultDataSettings>(scope)).ExtractValueSync();
            auto result = response.GetResult();
            printer.Print(result.result_set());
        }
    }
}

int TCommandFederatedQueryCreateQuery::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::CreateQueryRequest request;

    if (JsonFile) {
        auto jsonText = ReadFromFile(JsonFile, "json");
        NProtobufJson::Json2Proto(jsonText, request, Json2ProtoConfig);
    }

    if (!Type && request.content().type() == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        Type = "ANALYTICS";
    }

    if (Type) {
        FederatedQuery::QueryContent::QueryType type = FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED;
        if (!FederatedQuery::QueryContent::QueryType_Parse(Type, &type)) {
            throw TMisuseException() << "Unknown type";
        }
        request.mutable_content()->set_type(type);
    }

    if (request.content().type() == FederatedQuery::QueryContent::STREAMING && !request.has_disposition()) {
        request.mutable_disposition()->mutable_fresh();
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (ContentFile) {
        auto content = ReadFromFile(ContentFile, "content");
        request.mutable_content()->set_text(content);
    } else if (Content) {
        request.mutable_content()->set_text(Content);
    }

    if (!Mode && request.execute_mode() == FederatedQuery::EXECUTE_MODE_UNSPECIFIED) {
        Mode = "RUN";
    }

    if (Mode) {
        FederatedQuery::ExecuteMode mode = FederatedQuery::EXECUTE_MODE_UNSPECIFIED;
        if (!FederatedQuery::ExecuteMode_Parse(Mode, &mode)) {
            throw TMisuseException() << "Unknown mode";
        }
        request.set_execute_mode(mode);
    }

    if (!Visibility && request.content().acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        Visibility = "PRIVATE";
    }

    if (Visibility) {
        FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
        if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
            throw TMisuseException() << "Unknown visibility";
        }
        request.mutable_content()->mutable_acl()->set_visibility(visibility);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    if (Automatic) {
        request.mutable_content()->set_automatic(Automatic);
    }

    if (!Disposition && !request.has_disposition() && request.content().type() == FederatedQuery::QueryContent::STREAMING) {
        Disposition = "FROM_LAST_CHECKPOINT";
    }

    if (Disposition) {
        if (Disposition == "OLDEST") {
            request.mutable_disposition()->mutable_oldest();
        } else if (Disposition == "FRESH") {
            request.mutable_disposition()->mutable_fresh();
        } else if (Disposition == "FROM_TIME") {
            auto timestamp = TInstant::ParseIso8601(DispositionTimestamp);
            auto& fromTime = *request.mutable_disposition()->mutable_from_time();
            *fromTime.mutable_timestamp() = MicroSecondToProtoTimeStamp(timestamp.MicroSeconds());
        } else if (Disposition == "TIME_AGO") {
            TDuration duration = TDuration::Parse(DispositionDuration);
            auto& timeAgo = *request.mutable_disposition()->mutable_time_ago();
            *timeAgo.mutable_duration() = MicroSecondToProtoDuration(duration.MicroSeconds());
        } else if (Disposition == "FROM_LAST_CHECKPOINT") {
            request.mutable_disposition()->mutable_from_last_checkpoint();
        } else {
            throw TMisuseException() << "Unknown disposition: " << Disposition;
        }
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    auto response = client.CreateQuery(request, CreateFqSettings<NYdb::NFq::TCreateQuerySettings>(config.YScope)).ExtractValueSync();
    ProcessProtoResult(response);

    while (Wait) {
        FederatedQuery::DescribeQueryRequest describeRequest;
        describeRequest.set_query_id(response.GetResult().query_id());
        const auto& description = client.DescribeQuery(describeRequest, CreateFqSettings<NYdb::NFq::TDescribeQuerySettings>(config.YScope)).ExtractValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(description);

        const auto& descriptionProto = description.GetResult();
        const auto& query = descriptionProto.query();
        auto status = query.meta().status();
        if (status == FederatedQuery::QueryMeta::COMPLETED) {
            if (!SkipResult) {
                PrintResult(client, config.YScope, query);
            }
            break;
        }

        if (IsIn(
            {
                FederatedQuery::QueryMeta::RUNNING,
                FederatedQuery::QueryMeta::STARTING,
                FederatedQuery::QueryMeta::COMPLETING,
                FederatedQuery::QueryMeta::FAILING,
                FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM,
                FederatedQuery::QueryMeta::ABORTING_BY_USER,
                FederatedQuery::QueryMeta::RESUMING,
                FederatedQuery::QueryMeta::PAUSING
            }, status)) {
            Sleep(TDuration::Seconds(1));
        } else {
            if (query.issue_size() > 0) {
                NYdb::NIssue::TIssues issues;
                NYdb::NIssue::IssuesFromMessage(query.issue(), issues);
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            } else {
                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Unknown Error " + FederatedQuery::QueryMeta_ComputeStatus_Name(status));
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            }
            break;
        }
    }

    return EXIT_SUCCESS;
}

TCommandFederatedQueryListQueries::TCommandFederatedQueryListQueries()
    : TYdbCommand("list", {}, "List available queries")
{}

void TCommandFederatedQueryListQueries::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption('j', "json", "Json file").StoreResult(&JsonFile);
}

void TCommandFederatedQueryListQueries::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryListQueries::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::ListQueriesRequest request;

    if (JsonFile) {
        auto jsonText = ReadFromFile(JsonFile, "json");
        NProtobufJson::Json2Proto(jsonText, request, Json2ProtoConfig);
    }

    if (PageToken) {
        request.set_page_token(PageToken);
    }

    if (Limit) {
        request.set_limit(Limit);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListQueries(request, CreateFqSettings<NYdb::NFq::TListQueriesSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDescribeQuery::TCommandFederatedQueryDescribeQuery()
    : TYdbCommand("describe", {}, "Get detailed information about query")
{}

void TCommandFederatedQueryDescribeQuery::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
}

void TCommandFederatedQueryDescribeQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDescribeQuery::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::DescribeQueryRequest request;
    request.set_query_id(QueryId);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DescribeQuery(request, CreateFqSettings<NYdb::NFq::TDescribeQuerySettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryGetQueryStatus::TCommandFederatedQueryGetQueryStatus()
    : TYdbCommand("status", {}, "Receive query status")
{}

void TCommandFederatedQueryGetQueryStatus::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
}

void TCommandFederatedQueryGetQueryStatus::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryGetQueryStatus::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::GetQueryStatusRequest request;
    request.set_query_id(QueryId);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.GetQueryStatus(request, CreateFqSettings<NYdb::NFq::TGetQueryStatusSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyQuery::TCommandFederatedQueryModifyQuery()
    : TYdbCommand("modify", {}, "Change query settings")
{}

void TCommandFederatedQueryModifyQuery::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
    config.Opts->AddLongOption('n', "name", "Query name").StoreResult(&Name);
    config.Opts->AddLongOption('s', "script", "Script").StoreResult(&Content);
    config.Opts->AddLongOption("file", "Script file").StoreResult(&ContentFile);
    config.Opts->AddLongOption('t', "type", "Type of query (ANALYTICS, STREAMING) (default: \"ANALYTICS\")")
        .StoreResult(&Type);
    config.Opts->AddLongOption('m', "mode", "Execution mode (SAVE, PARSE, COMPILE, VALIDATE, EXPLAIN, RUN) (default: \"RUN\")")
        .StoreResult(&Mode);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption('d', "disposition", "Disposition for sources of streaming queries (OLDEST, FRESH, FROM_TIME, TIME_AGO, FROM_LAST_CHECKPOINT) (default: \"FROM_LAST_CHECKPOINT\")")
        .StoreResult(&Disposition);
    config.Opts->AddLongOption("disposition-timestamp", "Timestamp that will be used in disposition")
        .StoreResult(&DispositionTimestamp);
    config.Opts->AddLongOption("disposition-duration", "Duration that will be used in disposition")
        .StoreResult(&DispositionDuration);
    config.Opts->AddLongOption("state", "Previous state usage mode (EMPTY, FROM_LAST_CHECKPOINT)")
        .StoreResult(&State);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption('j', "json", "Json file").StoreResult(&JsonFile);
    config.Opts->AddLongOption('w', "wait", "Wait result data").NoArgument().StoreTrue(&Wait);
    config.Opts->AddLongOption('f', "disposition-force", "Force disposition (mode for FROM_LAST_CHECKPOINT disposition)").NoArgument().StoreTrue(&DispositionForce);
}

void TCommandFederatedQueryModifyQuery::PrintResult(NYdb::NFq::TClient& client, const TString& scope, const FederatedQuery::Query& query)
{
    TResultSetPrinter printer(OutputFormat);
    Cout << "query id: " << query.meta().common().id() << Endl;
    Cout << "result set size: " << query.result_set_meta().size()<< Endl;
    for (int64_t i = 0; i < query.result_set_meta().size(); ++i) {
        Cout << "result sets #" << i << Endl;
        const auto& resultSetMeta = query.result_set_meta(i);
        const int64_t limit = 10;
        for (int64_t offset = 0; offset < resultSetMeta.rows_count(); offset += limit) {
            FederatedQuery::GetResultDataRequest request;
            request.set_query_id(query.meta().common().id());
            request.set_result_set_index(i);
            request.set_offset(offset);
            request.set_limit(limit);
            const auto& response = client.GetResultData(request, CreateFqSettings<NYdb::NFq::TGetResultDataSettings>(scope)).ExtractValueSync();
            auto result = response.GetResult();
            printer.Print(result.result_set());
        }
    }
}

void TCommandFederatedQueryModifyQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyQuery::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::ModifyQueryRequest request;

    if (JsonFile) {
        auto jsonText = ReadFromFile(JsonFile, "json");
        NProtobufJson::Json2Proto(jsonText, request, Json2ProtoConfig);
    }

    if (!Type && request.content().type() == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        Type = "ANALYTICS";
    }

    if (Type) {
        FederatedQuery::QueryContent::QueryType type = FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED;
        if (!FederatedQuery::QueryContent::QueryType_Parse(Type, &type)) {
            throw TMisuseException() << "Unknown type";
        }
        request.mutable_content()->set_type(type);
    }

    if (QueryId) {
        request.set_query_id(QueryId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (ContentFile) {
        auto content = ReadFromFile(ContentFile, "content");
        request.mutable_content()->set_text(content);
    } else if (Content) {
        request.mutable_content()->set_text(Content);
    }

    if (!Mode && request.execute_mode() == FederatedQuery::EXECUTE_MODE_UNSPECIFIED) {
        Mode = "RUN";
    }

    if (Mode) {
        FederatedQuery::ExecuteMode mode = FederatedQuery::EXECUTE_MODE_UNSPECIFIED;
        if (!FederatedQuery::ExecuteMode_Parse(Mode, &mode)) {
            throw TMisuseException() << "Unknown mode";
        }
        request.set_execute_mode(mode);
    }

    if (!Visibility && request.content().acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        Visibility = "PRIVATE";
    }

    if (Visibility) {
        FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
        if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
            throw TMisuseException() << "Unknown visibility";
        }
        request.mutable_content()->mutable_acl()->set_visibility(visibility);
    }

    if (!Disposition && !request.has_disposition() && request.content().type() == FederatedQuery::QueryContent::STREAMING) {
        Disposition = "FROM_LAST_CHECKPOINT";
    }

    if (Disposition) {
        if (Disposition == "OLDEST") {
            request.mutable_disposition()->mutable_oldest();
        } else if (Disposition == "FRESH") {
            request.mutable_disposition()->mutable_fresh();
        } else if (Disposition == "FROM_TIME") {
            auto timestamp = TInstant::ParseIso8601(DispositionTimestamp);
            auto& fromTime = *request.mutable_disposition()->mutable_from_time();
            *fromTime.mutable_timestamp() = MicroSecondToProtoTimeStamp(timestamp.MicroSeconds());
        } else if (Disposition == "TIME_AGO") {
            TDuration duration = TDuration::Parse(DispositionDuration);
            auto& timeAgo = *request.mutable_disposition()->mutable_time_ago();
            *timeAgo.mutable_duration() = MicroSecondToProtoDuration(duration.MicroSeconds());
        } else if (Disposition == "FROM_LAST_CHECKPOINT") {
            request.mutable_disposition()->mutable_from_last_checkpoint()->set_force(DispositionForce);
        } else {
            throw TMisuseException() << "Unknown disposition: " << Disposition;
        }
    }

    if (!State && request.state_load_mode() == FederatedQuery::STATE_LOAD_MODE_UNSPECIFIED && request.content().type() == FederatedQuery::QueryContent::STREAMING) {
        State = "FROM_LAST_CHECKPOINT";
    }

    if (State) {
        if (State == "EMPTY") {
            request.set_state_load_mode(FederatedQuery::EMPTY);
        } else if (State == "FROM_LAST_CHECKPOINT") {
            request.set_state_load_mode(FederatedQuery::FROM_LAST_CHECKPOINT);
        } else {
            throw TMisuseException() << "Unknown state mode: " << State;
        }
    }

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyQuery(request, CreateFqSettings<NYdb::NFq::TModifyQuerySettings>(config.YScope)).ExtractValueSync());

    while (Wait) {
        FederatedQuery::DescribeQueryRequest describeRequest;
        describeRequest.set_query_id(QueryId);
        const auto& description = client.DescribeQuery(describeRequest, CreateFqSettings<NYdb::NFq::TDescribeQuerySettings>(config.YScope)).ExtractValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(description);

        const auto& descriptionProto = description.GetResult();
        const auto& query = descriptionProto.query();
        auto status = query.meta().status();
        if (status == FederatedQuery::QueryMeta::COMPLETED) {
            PrintResult(client, config.YScope, query);
            break;
        }

        if (IsIn(
            {
                FederatedQuery::QueryMeta::RUNNING,
                FederatedQuery::QueryMeta::STARTING,
                FederatedQuery::QueryMeta::COMPLETING,
                FederatedQuery::QueryMeta::FAILING,
                FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM,
                FederatedQuery::QueryMeta::ABORTING_BY_USER,
                FederatedQuery::QueryMeta::RESUMING,
                FederatedQuery::QueryMeta::PAUSING
            }, status)) {
            Sleep(TDuration::Seconds(1));
        } else {
            if (query.issue_size() > 0) {
                NYdb::NIssue::TIssues issues;
                NYdb::NIssue::IssuesFromMessage(query.issue(), issues);
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            } else {
                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Unknown Error " + FederatedQuery::QueryMeta_ComputeStatus_Name(status));
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            }
            break;
        }
    }

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDeleteQuery::TCommandFederatedQueryDeleteQuery()
    : TYdbCommand("delete", {}, "Delete specified query")
{}

void TCommandFederatedQueryDeleteQuery::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
}

void TCommandFederatedQueryDeleteQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDeleteQuery::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::DeleteQueryRequest request;
    request.set_query_id(QueryId);
    request.set_previous_revision(PreviousRevision);
    request.set_idempotency_key(IdempotencyKey);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DeleteQuery(request, CreateFqSettings<NYdb::NFq::TDeleteQuerySettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryControlQuery::TCommandFederatedQueryControlQuery()
    : TYdbCommand("control", {}, "Control over query state")
{}

void TCommandFederatedQueryControlQuery::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
    config.Opts->AddLongOption('a', "action", "Action over the query (PAUSE, PAUSE_GRACEFULLY, ABORT, ABORT_GRACEFULLY, RESUME) (default: \"ABORT\")")
        .StoreResult(&Action);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
}

void TCommandFederatedQueryControlQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryControlQuery::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));

    FederatedQuery::ControlQueryRequest request;
    request.set_query_id(QueryId);

    if (!Action) {
        Action = "ABORT";
    }

    FederatedQuery::QueryAction action = FederatedQuery::QUERY_ACTION_UNSPECIFIED;
    if (!FederatedQuery::QueryAction_Parse(Action, &action)) {
        throw TMisuseException() << "Unknown action: " << Action;
    }
    request.set_action(action);
    request.set_previous_revision(PreviousRevision);
    request.set_idempotency_key(IdempotencyKey);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ControlQuery(request, CreateFqSettings<NYdb::NFq::TControlQuerySettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryGetResultData::TCommandFederatedQueryGetResultData()
    : TYdbCommand("data", {}, "Receive processed query data")
{}

void TCommandFederatedQueryGetResultData::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier").StoreResult(&QueryId);
    config.Opts->AddLongOption("index", "Index of the set").StoreResult(&ResultSetIndex);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("offset", "Data offset").StoreResult(&Offset);
    config.Opts->AddLongOption('w', "wait", "Wait result set").NoArgument().StoreTrue(&Wait);
}

void TCommandFederatedQueryGetResultData::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryGetResultData::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::GetResultDataRequest request;
    request.set_query_id(QueryId);
    request.set_result_set_index(ResultSetIndex);
    request.set_limit(Limit);
    request.set_offset(Offset);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;

    while (Wait) {
        FederatedQuery::DescribeQueryRequest describeRequest;
        describeRequest.set_query_id(request.query_id());
        const auto& description = client.DescribeQuery(describeRequest, CreateFqSettings<NYdb::NFq::TDescribeQuerySettings>(config.YScope)).ExtractValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(description);

        const auto& descriptionProto = description.GetResult();
        const auto& query = descriptionProto.query();
        auto status = query.meta().status();
        if (status == FederatedQuery::QueryMeta::COMPLETED) {
            break;
        }

        if (IsIn(
            {
                FederatedQuery::QueryMeta::RUNNING,
                FederatedQuery::QueryMeta::STARTING,
                FederatedQuery::QueryMeta::COMPLETING,
                FederatedQuery::QueryMeta::FAILING,
                FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM,
                FederatedQuery::QueryMeta::ABORTING_BY_USER,
                FederatedQuery::QueryMeta::RESUMING,
                FederatedQuery::QueryMeta::PAUSING
            }, status)) {
            Sleep(TDuration::Seconds(1));
        } else {
            if (query.issue_size() > 0) {
                NYdb::NIssue::TIssues issues;
                NYdb::NIssue::IssuesFromMessage(query.issue(), issues);
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            } else {
                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Unknown Error " + FederatedQuery::QueryMeta_ComputeStatus_Name(status));
                NStatusHelpers::ThrowOnErrorOrPrintIssues(NYdb::TStatus{NYdb::EStatus::BAD_REQUEST, std::move(issues)});
            }
            break;
        }
    }

    auto response = client.GetResultData(request, CreateFqSettings<NYdb::NFq::TGetResultDataSettings>(config.YScope)).ExtractValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(response);
    TResultSetPrinter printer(OutputFormat);
    printer.Print(response.GetResult().result_set());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryQueryListJobs::TCommandFederatedQueryQueryListJobs()
    : TYdbCommand("list", {}, "List available jobs for specified query")
{}

void TCommandFederatedQueryQueryListJobs::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Query identifier")
                    .Required()
                    .StoreResult(&QueryId);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("created-by-me", "Only created by me").NoArgument().StoreTrue(&CreatedByMe);
}

void TCommandFederatedQueryQueryListJobs::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryQueryListJobs::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ListJobsRequest request;
    request.mutable_filter()->set_query_id(QueryId);
    request.set_page_token(PageToken);
    request.set_limit(Limit);

    if (CreatedByMe) {
        request.mutable_filter()->set_created_by_me(CreatedByMe);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListJobs(request, CreateFqSettings<NYdb::NFq::TListJobsSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryListJobs::TCommandFederatedQueryListJobs()
    : TYdbCommand("list", {}, "List available jobs")
{}

void TCommandFederatedQueryListJobs::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("created-by-me", "Only created by me").NoArgument().StoreTrue(&CreatedByMe);
}

void TCommandFederatedQueryListJobs::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryListJobs::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ListJobsRequest request;
    request.set_page_token(PageToken);
    request.set_limit(Limit);

    if (CreatedByMe) {
        request.mutable_filter()->set_created_by_me(CreatedByMe);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListJobs(request, CreateFqSettings<NYdb::NFq::TListJobsSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDescribeJob::TCommandFederatedQueryDescribeJob()
    : TYdbCommand("describe", {}, "Get detailed information about job")
{}

void TCommandFederatedQueryDescribeJob::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Job identifier").StoreResult(&JobId);
}

void TCommandFederatedQueryDescribeJob::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDescribeJob::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::DescribeJobRequest request;
    request.set_job_id(JobId);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DescribeJob(request, CreateFqSettings<NYdb::NFq::TDescribeJobSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryCreateConnection::TCommandFederatedQueryCreateConnection()
    : TClientCommandTree("create", {}, "Create new connection that can be used in queries")
{
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionYdb>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionClickHouse>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionDataStreams>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionObjectStorage>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionMonitoring>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionPostgreSQL>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionGreenplum>());
    AddCommand(std::make_unique<TCommandFederatedQueryCreateConnectionIceberg>());
}

TCommandFederatedQueryCreateConnectionYdb::TCommandFederatedQueryCreateConnectionYdb()
    : TYdbCommand("ydb", {}, "Create new connection to YDB that can be used in queries")
{}

void TCommandFederatedQueryCreateConnectionYdb::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access YDB").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access YDB").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

void TCommandFederatedQueryCreateConnectionYdb::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateConnectionYdb::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::YdbDatabase* const ydb = request.mutable_content()->mutable_setting()->mutable_ydb_database();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        ydb->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        ydb->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        ydb->set_database_id(DatabaseId);
    } else {
        ydb->set_database(Database);
        ydb->set_endpoint(Endpoint);
    }

    ydb->set_secure(Secure);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}


TCommandFederatedQueryCreateConnectionIceberg::TCommandFederatedQueryCreateConnectionIceberg()
    : TYdbCommand("iceberg", {}, "Create a new connection to an iceberg that can be used in queries")
{}

void TCommandFederatedQueryCreateConnectionIceberg::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access to an iceberg").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access to an iceberg").NoArgument().StoreTrue(&CurrentIAM);

    config.Opts->AddLongOption(TS3WarehouseParams::BUCKET, "Warehouse s3 bucket e.g., s3a://iceberg-bucket")
        .StoreResult(&S3Warehouse.Bucket);

    config.Opts->AddLongOption(TS3WarehouseParams::PATH, "Warehouse s3 bucket path e.g., storage")
        .StoreResult(&S3Warehouse.Path);

    config.Opts->AddLongOption(THadoopCatalogParams::DIR, "Hadoop directory e.g., warehouse")
        .StoreResult(&Hadoop.Directory);

    config.Opts->AddLongOption(THiveMetastoreCatalogParams::URI, "HiveMetastore location e.g., thrift://host:9083/")
        .StoreResult(&HiveMetastore.Uri);

    config.Opts->AddLongOption(THiveMetastoreCatalogParams::DB, "HiveMetastore database which holds an iceberg namespace e.g., warehouse")
        .StoreResult(&HiveMetastore.Database);
}

void TCommandFederatedQueryCreateConnectionIceberg::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateConnectionIceberg::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;
    auto& content = *request.mutable_content();

    if (Name) {
        content.set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    auto visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;

    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }

    content.mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    auto& iceberg = *content.mutable_setting()->mutable_iceberg();
    auto& auth = *iceberg.mutable_warehouse_auth();

    // Fill Auth
    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        auth.mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        auth.mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (Hadoop.HasData() && HiveMetastore.HasData()) {
        throw TMisuseException() << "Either Hadoop or HiveMetastore should be used";
    }

    // Fill Warehouse
    S3Warehouse.Fill(*iceberg.mutable_warehouse());

    // Fill Catalog
    auto& catalog = *iceberg.mutable_catalog();

    if (Hadoop.HasData()) {
        Hadoop.Fill(catalog);
    } else if (HiveMetastore.HasData()) {
        HiveMetastore.Fill(catalog);
    } else {
        throw TMisuseException() << "Catalog has to be set";
    }

    PrintRequestIfVerbose(config, request);
    Cout << "Response:" << Endl;

    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());
    return EXIT_SUCCESS;
}

TString LowerCaseName(TStringBuf name) {
    auto out = TString(name);
    out.to_lower();
    return out;
}

template <typename TDataSource>
TCommandFederatedQueryCreateConnectionGeneric<TDataSource>::TCommandFederatedQueryCreateConnectionGeneric()
    : TYdbCommand(
        LowerCaseName(TDataSource::Name),
        {},
        TStringBuilder() << "Create new connection to " << TDataSource::Name << " that can be used in queries")
{}

template <typename TDataSource>
void TCommandFederatedQueryCreateConnectionGeneric<TDataSource>::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", TStringBuilder() << "Service account to be used to access " << TDataSource::Name).StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", TStringBuilder() << "Current IAM token to be used to access " << TDataSource::Name).NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("login", "Login").StoreResult(&Login);
    config.Opts->AddLongOption("pwd", "Password").StoreResult(&Password);
    config.Opts->AddLongOption("host", "Host").StoreResult(&Host);
    config.Opts->AddLongOption("port", "Port").StoreResult(&Port);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
    config.Opts->AddLongOption("db-name", "Database Name").StoreResult(&DatabaseName);
}

template <typename TDataSource>
void TCommandFederatedQueryCreateConnectionGeneric<TDataSource>::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

// SFINAE detectors check the setter on the cluster object itself, not on a pointer to it.
// Note: protobuf string setters typically take const std::string& / std::string_view; we use
// const std::string& here, which is what generated set_host signatures accept.
template <typename, typename = std::void_t<>>
struct has_set_host: std::false_type {};

template <typename T>
struct has_set_host<T, std::void_t<decltype(std::declval<T&>().set_host(std::declval<const std::string&>()))>>: std::true_type {};

template <typename T>
constexpr bool has_set_host_v = has_set_host<std::remove_pointer_t<T>>::value;

template <typename, typename = std::void_t<>>
struct has_set_port: std::false_type {};

template <typename T>
struct has_set_port<T, std::void_t<decltype(std::declval<T&>().set_port(std::declval<int>()))>>: std::true_type {};

template <typename T>
constexpr bool has_set_port_v = has_set_port<std::remove_pointer_t<T>>::value;

template <typename, typename = std::void_t<>>
struct has_set_secure: std::false_type {};

template <typename T>
struct has_set_secure<T, std::void_t<decltype(std::declval<T&>().set_secure(std::declval<bool>()))>>: std::true_type {};

template <typename T>
constexpr bool has_set_secure_v = has_set_secure<std::remove_pointer_t<T>>::value;

template <typename TDataSource>
int TCommandFederatedQueryCreateConnectionGeneric<TDataSource>::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    auto cluster = TDataSource::GetCluster(request.mutable_content()->mutable_setting());

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        cluster->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        cluster->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Host || Port) {
            throw TMisuseException() << "Used database id and host with port";
        }
        cluster->set_database_id(DatabaseId);
    } else {
        if constexpr (has_set_host_v<decltype(cluster)>) {
            cluster->set_host(Host);
        }
        if constexpr (has_set_port_v<decltype(cluster)>) {
            cluster->set_port(Port);
        }
    }

    cluster->set_login(Login);
    cluster->set_password(Password);
    if constexpr (has_set_secure_v<decltype(cluster)>) {
        cluster->set_secure(Secure);
    }

    cluster->set_database_name(DatabaseName);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryCreateConnectionDataStreams::TCommandFederatedQueryCreateConnectionDataStreams()
    : TYdbCommand("datastreams", {}, "Create new connection to DataStreams that can be used in queries")
{}

void TCommandFederatedQueryCreateConnectionDataStreams::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access data streams").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access data streams").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("token-auth", "OAuth2 token to be used to access data streams").StoreResult(&TokenAuth);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
    config.Opts->AddLongOption("shared-reading", "Enable Shared Reading").NoArgument().StoreTrue(&SharedReading);
}

void TCommandFederatedQueryCreateConnectionDataStreams::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateConnectionDataStreams::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::DataStreams* const data_stream = request.mutable_content()->mutable_setting()->mutable_data_streams();

    if (TokenAuth) {
        if (CurrentIAM || ServiceAccount) {
            throw TMisuseException() << "Use either service account auth, or token auth or current IAM auth";
        }
        data_stream->mutable_auth()->mutable_token()->set_token(TokenAuth);
    } else if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        data_stream->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        data_stream->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        data_stream->set_database_id(DatabaseId);
    } else {
        data_stream->set_database(Database);
        data_stream->set_endpoint(Endpoint);
    }

    data_stream->set_secure(Secure);

    if (SharedReading) {
        data_stream->set_shared_reading(SharedReading);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryCreateConnectionObjectStorage::TCommandFederatedQueryCreateConnectionObjectStorage()
    : TYdbCommand("objectstorage", {}, "Create connection to ObjectStorage that can be used in queries")
{}

void TCommandFederatedQueryCreateConnectionObjectStorage::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access object storage bucket").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access object storage bucket").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("bucket", "Object storage bucket name").StoreResult(&Bucket);
}

void TCommandFederatedQueryCreateConnectionObjectStorage::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateConnectionObjectStorage::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::ObjectStorageConnection* const object_storage = request.mutable_content()->mutable_setting()->mutable_object_storage();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        object_storage->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        object_storage->mutable_auth()->mutable_current_iam();
    } else {
        object_storage->mutable_auth()->mutable_none();
    }

    object_storage->set_bucket(Bucket);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryCreateConnectionMonitoring::TCommandFederatedQueryCreateConnectionMonitoring()
    : TYdbCommand("monitoring", {}, "Create new connection to Monitoring that can be used in queries")
{}

void TCommandFederatedQueryCreateConnectionMonitoring::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access monitoring").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access monitoring").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("project", "Monitoring project name").StoreResult(&Project);
    config.Opts->AddLongOption("cluster", "Monitoring cluster name").StoreResult(&Cluster);
    config.Opts->AddLongOption("token-auth", "OAuth2 token to be used to access monitoring").StoreResult(&TokenAuth);
}

void TCommandFederatedQueryCreateConnectionMonitoring::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateConnectionMonitoring::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateConnectionRequest request;

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::Monitoring* const monitoring = request.mutable_content()->mutable_setting()->mutable_monitoring();

    if (TokenAuth) {
        if (CurrentIAM || ServiceAccount) {
            throw TMisuseException() << "Use either service account auth, or token auth or current IAM auth";
        }
        monitoring->mutable_auth()->mutable_token()->set_token(TokenAuth);
    } else if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        monitoring->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        monitoring->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    monitoring->set_project(Project);
    monitoring->set_cluster(Cluster);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateConnection(request, CreateFqSettings<NYdb::NFq::TCreateConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryTestConnection::TCommandFederatedQueryTestConnection()
    : TClientCommandTree("test", {}, "Test a connection that can be used in queries")
{
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionYdb>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionClickHouse>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionDataStreams>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionObjectStorage>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionMonitoring>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionPostgreSQL>());
    AddCommand(std::make_unique<TCommandFederatedQueryTestConnectionGreenplum>());
}

TCommandFederatedQueryTestConnectionYdb::TCommandFederatedQueryTestConnectionYdb()
    : TYdbCommand("ydb", {}, "Test a connection to YDB that can be used in queries")
{}

void TCommandFederatedQueryTestConnectionYdb::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("sa", "Service account to be used to access YDB").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access YDB").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

void TCommandFederatedQueryTestConnectionYdb::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryTestConnectionYdb::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::TestConnectionRequest request;

    ::FederatedQuery::YdbDatabase* const ydb = request.mutable_setting()->mutable_ydb_database();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        ydb->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        ydb->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        ydb->set_database_id(DatabaseId);
    } else {
        ydb->set_database(Database);
        ydb->set_endpoint(Endpoint);
    }

    ydb->set_secure(Secure);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.TestConnection(request, CreateFqSettings<NYdb::NFq::TTestConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

template <typename TDataSource>
TCommandFederatedQueryTestConnectionGeneric<TDataSource>::TCommandFederatedQueryTestConnectionGeneric()
    : TYdbCommand(
        LowerCaseName(TDataSource::Name),
        {},
        TStringBuilder() << "Test a connection to " << TDataSource::Name << " that can be used in queries")
{}

template <typename TDataSource>
void TCommandFederatedQueryTestConnectionGeneric<TDataSource>::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("sa", TStringBuilder() << "Service account to be used to access " << TDataSource::Name).StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", TStringBuilder() << "Current IAM token to be used to access " << TDataSource::Name).NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("db-name", "Database Name").StoreResult(&DatabaseName);
    config.Opts->AddLongOption("login", "Login").StoreResult(&Login);
    config.Opts->AddLongOption("pwd", "Password").StoreResult(&Password);
    config.Opts->AddLongOption("host", "Host").StoreResult(&Host);
    config.Opts->AddLongOption("port", "Port").StoreResult(&Port);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

template <typename TDataSource>
void TCommandFederatedQueryTestConnectionGeneric<TDataSource>::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

template <typename TDataSource>
int TCommandFederatedQueryTestConnectionGeneric<TDataSource>::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::TestConnectionRequest request;

    auto cluster = TDataSource::GetCluster(request.mutable_setting());

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        cluster->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        cluster->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Host || Port) {
            throw TMisuseException() << "Used database id and host with port";
        }
        cluster->set_database_id(DatabaseId);
    } else {
        if constexpr (has_set_host_v<decltype(cluster)>) {
            cluster->set_host(Host);
        }
        if constexpr (has_set_port_v<decltype(cluster)>) {
            cluster->set_port(Port);
        }
    }

    cluster->set_login(Login);
    cluster->set_password(Password);
    if constexpr (has_set_secure_v<decltype(cluster)>) {
        cluster->set_secure(Secure);
    }

    cluster->set_database_name(DatabaseName);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.TestConnection(request, CreateFqSettings<NYdb::NFq::TTestConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryTestConnectionDataStreams::TCommandFederatedQueryTestConnectionDataStreams()
    : TYdbCommand("datastreams", {}, "Test a connection to DataStreams that can be used in queries")
{}

void TCommandFederatedQueryTestConnectionDataStreams::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("sa", "Service account to be used to access data streams").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access data streams").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

void TCommandFederatedQueryTestConnectionDataStreams::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryTestConnectionDataStreams::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::TestConnectionRequest request;

    ::FederatedQuery::DataStreams* const data_stream = request.mutable_setting()->mutable_data_streams();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        data_stream->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        data_stream->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        data_stream->set_database_id(DatabaseId);
    } else {
        data_stream->set_database(Database);
        data_stream->set_endpoint(Endpoint);
    }

    data_stream->set_secure(Secure);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.TestConnection(request, CreateFqSettings<NYdb::NFq::TTestConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryTestConnectionObjectStorage::TCommandFederatedQueryTestConnectionObjectStorage()
    : TYdbCommand("objectstorage", {}, "Test a connection to ObjectStorage that can be used in queries")
{}

void TCommandFederatedQueryTestConnectionObjectStorage::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("sa", "Service account to be used to access object storage bucket").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access object storage bucket").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("bucket", "Object storage bucket name").StoreResult(&Bucket);
}

void TCommandFederatedQueryTestConnectionObjectStorage::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryTestConnectionObjectStorage::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::TestConnectionRequest request;

    ::FederatedQuery::ObjectStorageConnection* const object_storage = request.mutable_setting()->mutable_object_storage();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        object_storage->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        object_storage->mutable_auth()->mutable_current_iam();
    } else {
        object_storage->mutable_auth()->mutable_none();
    }

    object_storage->set_bucket(Bucket);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.TestConnection(request, CreateFqSettings<NYdb::NFq::TTestConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryTestConnectionMonitoring::TCommandFederatedQueryTestConnectionMonitoring()
    : TYdbCommand("monitoring", {}, "Test a connection to Monitoring that can be used in queries")
{}

void TCommandFederatedQueryTestConnectionMonitoring::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("sa", "Service account to be used to access monitoring").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access monitoring").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("project", "Monitoring project name").StoreResult(&Project);
    config.Opts->AddLongOption("cluster", "Monitoring cluster name").StoreResult(&Cluster);
}

void TCommandFederatedQueryTestConnectionMonitoring::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryTestConnectionMonitoring::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::TestConnectionRequest request;

    ::FederatedQuery::Monitoring* const monitoring = request.mutable_setting()->mutable_monitoring();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        monitoring->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        monitoring->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    monitoring->set_project(Project);
    monitoring->set_cluster(Cluster);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.TestConnection(request, CreateFqSettings<NYdb::NFq::TTestConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryConnectionListBindings::TCommandFederatedQueryConnectionListBindings()
    : TYdbCommand("list", {}, "List available bindings for specified connection")
{}

void TCommandFederatedQueryConnectionListBindings::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("id", "Connection identifier").Required().StoreResult(&ConnectionId);
    config.Opts->AddLongOption("name", "Filter by substring name").StoreResult(&Name);
    config.Opts->AddLongOption("created-by-me", "Only created by me").NoArgument().StoreTrue(&CreatedByMe);
}

void TCommandFederatedQueryConnectionListBindings::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryConnectionListBindings::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ListBindingsRequest request;
    request.set_page_token(PageToken);
    request.set_limit(Limit);
    if (ConnectionId) {
        request.mutable_filter()->set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_filter()->set_name(Name);
    }

    if (CreatedByMe) {
        request.mutable_filter()->set_created_by_me(CreatedByMe);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListBindings(request, CreateFqSettings<NYdb::NFq::TListBindingsSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryListConnections::TCommandFederatedQueryListConnections()
    : TYdbCommand("list", {}, "List available connections")
{}

void TCommandFederatedQueryListConnections::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("name", "Filter by substring name").StoreResult(&Name);
    config.Opts->AddLongOption("created-by-me", "Only created by me").NoArgument().StoreTrue(&CreatedByMe);
}

void TCommandFederatedQueryListConnections::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryListConnections::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ListConnectionsRequest request;
    request.set_page_token(PageToken);
    request.set_limit(Limit);

    if (Name) {
        request.mutable_filter()->set_name(Name);
    }

    if (CreatedByMe) {
        request.mutable_filter()->set_created_by_me(CreatedByMe);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListConnections(request, CreateFqSettings<NYdb::NFq::TListConnectionsSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDescribeConnection::TCommandFederatedQueryDescribeConnection()
    : TYdbCommand("describe", {}, "Get detailed information about connection")
{}

void TCommandFederatedQueryDescribeConnection::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
}

void TCommandFederatedQueryDescribeConnection::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDescribeConnection::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::DescribeConnectionRequest request;
    request.set_connection_id(ConnectionId);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DescribeConnection(request, CreateFqSettings<NYdb::NFq::TDescribeConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyConnection::TCommandFederatedQueryModifyConnection()
    : TClientCommandTree("modify", {}, "Change connection settings")
{
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionYdb>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionClickHouse>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionDataStreams>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionObjectStorage>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionMonitoring>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionPostgreSQL>());
    AddCommand(std::make_unique<TCommandFederatedQueryModifyConnectionGreenplum>());
}

TCommandFederatedQueryModifyConnectionYdb::TCommandFederatedQueryModifyConnectionYdb()
    : TYdbCommand("ydb", {}, "Change YDB connection settings")
{}

void TCommandFederatedQueryModifyConnectionYdb::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access YDB").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access YDB").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

void TCommandFederatedQueryModifyConnectionYdb::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyConnectionYdb::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyConnectionRequest request;

    if (ConnectionId) {
        request.set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::YdbDatabase* const ydb = request.mutable_content()->mutable_setting()->mutable_ydb_database();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        ydb->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        ydb->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        ydb->set_database_id(DatabaseId);
    } else {
        ydb->set_database(Database);
        ydb->set_endpoint(Endpoint);
    }

    ydb->set_secure(Secure);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyConnection(request, CreateFqSettings<NYdb::NFq::TModifyConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

template <typename TDataSource>
TCommandFederatedQueryModifyConnectionGeneric<TDataSource>::TCommandFederatedQueryModifyConnectionGeneric()
    : TYdbCommand(
        LowerCaseName(TDataSource::Name),
        {},
        TStringBuilder() << "Change " << TDataSource::Name << " settings")
{}

template <typename TDataSource>
void TCommandFederatedQueryModifyConnectionGeneric<TDataSource>::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", TStringBuilder() << "Service account to be used to access " << TDataSource::Name).StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", TStringBuilder() << "Current IAM token to be used to access " << TDataSource::Name).NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("db-name", "Database Name").StoreResult(&DatabaseName);
    config.Opts->AddLongOption("login", "Login").StoreResult(&Login);
    config.Opts->AddLongOption("pwd", "Password").StoreResult(&Password);
    config.Opts->AddLongOption("host", "Host").StoreResult(&Host);
    config.Opts->AddLongOption("port", "Port").StoreResult(&Port);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

template <typename TDataSource>
void TCommandFederatedQueryModifyConnectionGeneric<TDataSource>::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

template <typename TDataSource>
int TCommandFederatedQueryModifyConnectionGeneric<TDataSource>::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyConnectionRequest request;

    if (ConnectionId) {
        request.set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    auto cluster = TDataSource::GetCluster(request.mutable_content()->mutable_setting());

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        cluster->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        cluster->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Host || Port) {
            throw TMisuseException() << "Used database id and host with port";
        }
        cluster->set_database_id(DatabaseId);
    } else {
        if constexpr (has_set_host_v<decltype(cluster)>) {
            cluster->set_host(Host);
        }
        if constexpr (has_set_port_v<decltype(cluster)>) {
            cluster->set_port(Port);
        }
    }

    cluster->set_login(Login);
    cluster->set_password(Password);
    if constexpr (has_set_secure_v<decltype(cluster)>) {
        cluster->set_secure(Secure);
    }

    cluster->set_database_name(DatabaseName);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyConnection(request, CreateFqSettings<NYdb::NFq::TModifyConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyConnectionDataStreams::TCommandFederatedQueryModifyConnectionDataStreams()
    : TYdbCommand("datastreams", {}, "Change DataStreams connection settings")
{}

void TCommandFederatedQueryModifyConnectionDataStreams::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access data streams").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access data streams").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("db-id", "Database Id").StoreResult(&DatabaseId);
    config.Opts->AddLongOption("endpoint", "Endpoint").StoreResult(&Endpoint);
    config.Opts->AddLongOption("database", "Database").StoreResult(&Database);
    config.Opts->AddLongOption("secure", "Secure").NoArgument().StoreTrue(&Secure);
}

void TCommandFederatedQueryModifyConnectionDataStreams::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyConnectionDataStreams::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyConnectionRequest request;

    if (ConnectionId) {
        request.set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::DataStreams* const data_stream = request.mutable_content()->mutable_setting()->mutable_data_streams();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        data_stream->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        data_stream->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    if (DatabaseId) {
        if (Database || Endpoint) {
            throw TMisuseException() << "Used database id and database with endpoint";
        }
        data_stream->set_database_id(DatabaseId);
    } else {
        data_stream->set_database(Database);
        data_stream->set_endpoint(Endpoint);
    }

    data_stream->set_secure(Secure);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyConnection(request, CreateFqSettings<NYdb::NFq::TModifyConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyConnectionObjectStorage::TCommandFederatedQueryModifyConnectionObjectStorage()
    : TYdbCommand("objectstorage", {}, "Change ObjectStorage connection settings")
{}

void TCommandFederatedQueryModifyConnectionObjectStorage::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access object storage bucket").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access object storage bucket").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("bucket", "Object storage bucket name").StoreResult(&Bucket);
}

void TCommandFederatedQueryModifyConnectionObjectStorage::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyConnectionObjectStorage::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyConnectionRequest request;

    if (ConnectionId) {
        request.set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::ObjectStorageConnection* const object_storage = request.mutable_content()->mutable_setting()->mutable_object_storage();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        object_storage->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        object_storage->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    object_storage->set_bucket(Bucket);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyConnection(request, CreateFqSettings<NYdb::NFq::TModifyConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyConnectionMonitoring::TCommandFederatedQueryModifyConnectionMonitoring()
    : TYdbCommand("monitoring", {}, "Change Monitoring connection settings")
{}

void TCommandFederatedQueryModifyConnectionMonitoring::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption('n', "name", "Connection name").StoreResult(&Name);
    config.Opts->AddLongOption('d', "description", "Connection description").StoreResult(&Description);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption("sa", "Service account to be used to access monitoring").StoreResult(&ServiceAccount);
    config.Opts->AddLongOption("current-iam", "Current IAM token to be used to access monitoring").NoArgument().StoreTrue(&CurrentIAM);
    config.Opts->AddLongOption("project", "Monitoring project name").StoreResult(&Project);
    config.Opts->AddLongOption("cluster", "Monitoring cluster name").StoreResult(&Cluster);
}

void TCommandFederatedQueryModifyConnectionMonitoring::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyConnectionMonitoring::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyConnectionRequest request;

    if (ConnectionId) {
        request.set_connection_id(ConnectionId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility) {
        Visibility = "PRIVATE";
    }

    FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
    if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
        throw TMisuseException() << "Unable to parse visibility " << Visibility << ". Use either SCOPE or PRIVATE";
    }
    request.mutable_content()->mutable_acl()->set_visibility(visibility);

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    ::FederatedQuery::Monitoring* const monitoring = request.mutable_content()->mutable_setting()->mutable_monitoring();

    if (ServiceAccount) {
        if (CurrentIAM) {
            throw TMisuseException() << "Use either service account auth or current IAM auth, not both";
        }
        monitoring->mutable_auth()->mutable_service_account()->set_id(ServiceAccount);
    } else if (CurrentIAM) {
        monitoring->mutable_auth()->mutable_current_iam();
    } else {
        throw TMisuseException() << "Either service account or current IAM authentication should be used";
    }

    monitoring->set_project(Project);
    monitoring->set_cluster(Cluster);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyConnection(request, CreateFqSettings<NYdb::NFq::TModifyConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDeleteConnection::TCommandFederatedQueryDeleteConnection()
    : TYdbCommand("delete", {}, "Delete specified connection")
{}

void TCommandFederatedQueryDeleteConnection::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Connection identifier").StoreResult(&ConnectionId);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
}

void TCommandFederatedQueryDeleteConnection::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDeleteConnection::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::DeleteConnectionRequest request;
    request.set_connection_id(ConnectionId);
    request.set_previous_revision(PreviousRevision);
    request.set_idempotency_key(IdempotencyKey);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DeleteConnection(request, CreateFqSettings<NYdb::NFq::TDeleteConnectionSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryCreateBinding::TCommandFederatedQueryCreateBinding()
    : TYdbCommand("create", {}, "Create new binding that can be used in queries")
{}

void TCommandFederatedQueryCreateBinding::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('n', "name", "Binding name").StoreResult(&Name);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption('j', "json", "Json file").StoreResult(&JsonFile);
}

void TCommandFederatedQueryCreateBinding::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryCreateBinding::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::CreateBindingRequest request;

    if (JsonFile) {
        auto jsonText = ReadFromFile(JsonFile, "json");
        NProtobufJson::Json2Proto(jsonText, request, Json2ProtoConfig);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility && request.content().acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        Visibility = "PRIVATE";
    }

    if (Visibility) {
        FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
        if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
            throw TMisuseException() << "Unknown visibility";
        }
        request.mutable_content()->mutable_acl()->set_visibility(visibility);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.CreateBinding(request, CreateFqSettings<NYdb::NFq::TCreateBindingSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryListBindings::TCommandFederatedQueryListBindings()
    : TYdbCommand("list", {}, "List available bindings")
{}

void TCommandFederatedQueryListBindings::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("page-token", "Page token").StoreResult(&PageToken);
    config.Opts->AddLongOption('l', "limit", "Limit the size of the response").StoreResult(&Limit);
    config.Opts->AddLongOption("name", "Filter by substring name").StoreResult(&Name);
    config.Opts->AddLongOption("created-by-me", "Only created by me").NoArgument().StoreTrue(&CreatedByMe);
}

void TCommandFederatedQueryListBindings::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryListBindings::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ListBindingsRequest request;
    request.set_page_token(PageToken);
    request.set_limit(Limit);

    if (Name) {
        request.mutable_filter()->set_name(Name);
    }

    if (CreatedByMe) {
        request.mutable_filter()->set_created_by_me(CreatedByMe);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ListBindings(request, CreateFqSettings<NYdb::NFq::TListBindingsSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDescribeBinding::TCommandFederatedQueryDescribeBinding()
    : TYdbCommand("describe", {}, "Get detailed information about binding")
{}

void TCommandFederatedQueryDescribeBinding::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Binding identifier").StoreResult(&BindingId);
}

void TCommandFederatedQueryDescribeBinding::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDescribeBinding::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::DescribeBindingRequest request;
    request.set_binding_id(BindingId);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DescribeBinding(request, CreateFqSettings<NYdb::NFq::TDescribeBindingSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryModifyBinding::TCommandFederatedQueryModifyBinding()
    : TYdbCommand("modify", {}, "Change binding settings")
{}

void TCommandFederatedQueryModifyBinding::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Binding identifier").StoreResult(&BindingId);
    config.Opts->AddLongOption('n', "name", "Binding name").StoreResult(&Name);
    config.Opts->AddLongOption("visibility", "Visibility (SCOPE, PRIVATE) (default: \"PRIVATE\")")
        .StoreResult(&Visibility);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
    config.Opts->AddLongOption('j', "json", "Json file").StoreResult(&JsonFile);
}

void TCommandFederatedQueryModifyBinding::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryModifyBinding::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::ModifyBindingRequest request;

    if (JsonFile) {
        auto jsonText = ReadFromFile(JsonFile, "json");
        NProtobufJson::Json2Proto(jsonText, request, Json2ProtoConfig);
    }

    if (BindingId) {
        request.set_binding_id(BindingId);
    }

    if (Name) {
        request.mutable_content()->set_name(Name);
    }

    if (!Visibility && request.content().acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        Visibility = "PRIVATE";
    }

    if (Visibility) {
        FederatedQuery::Acl::Visibility visibility = FederatedQuery::Acl::VISIBILITY_UNSPECIFIED;
        if (!FederatedQuery::Acl::Visibility_Parse(Visibility, &visibility)) {
            throw TMisuseException() << "Unknown visibility";
        }
        request.mutable_content()->mutable_acl()->set_visibility(visibility);
    }

    if (PreviousRevision) {
        request.set_previous_revision(PreviousRevision);
    }

    if (IdempotencyKey) {
        request.set_idempotency_key(IdempotencyKey);
    }

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.ModifyBinding(request, CreateFqSettings<NYdb::NFq::TModifyBindingSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

TCommandFederatedQueryDeleteBinding::TCommandFederatedQueryDeleteBinding()
    : TYdbCommand("delete", {}, "Delete specified binding")
{}

void TCommandFederatedQueryDeleteBinding::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("id", "Binding identifier").StoreResult(&BindingId);
    config.Opts->AddLongOption("revision", "Previous revision").StoreResult(&PreviousRevision);
    config.Opts->AddLongOption("idempotency-key", "Idempotency key").StoreResult(&IdempotencyKey);
}

void TCommandFederatedQueryDeleteBinding::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandFederatedQueryDeleteBinding::Run(TConfig& config) {
    NFq::TClient client(CreateDriver(config));
    FederatedQuery::DeleteBindingRequest request;
    request.set_binding_id(BindingId);
    request.set_previous_revision(PreviousRevision);
    request.set_idempotency_key(IdempotencyKey);

    PrintRequestIfVerbose(config, request);

    Cout << "Response:" << Endl;
    ProcessProtoResult(client.DeleteBinding(request, CreateFqSettings<NYdb::NFq::TDeleteBindingSettings>(config.YScope)).ExtractValueSync());

    return EXIT_SUCCESS;
}

} //NConsoleClient
} // NYdb
