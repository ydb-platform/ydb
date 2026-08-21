#include "ydb_service_session.h"

#include <ydb/public/lib/ydb_cli/common/duration.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/utf8_utils.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/system/shellcommand.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr size_t MaxPrettyQueryLength = 200;

std::optional<std::string> GetOptionalUtf8(TResultSetParser& parser, TStringBuf columnName) {
    const ssize_t index = parser.ColumnIndex(std::string(columnName.data(), columnName.size()));
    return index >= 0 ? parser.ColumnParser(index).GetOptionalUtf8() : std::nullopt;
}

std::optional<ui32> GetOptionalUint32(TResultSetParser& parser, TStringBuf columnName) {
    const ssize_t index = parser.ColumnIndex(std::string(columnName.data(), columnName.size()));
    return index >= 0 ? parser.ColumnParser(index).GetOptionalUint32() : std::nullopt;
}

std::optional<TInstant> GetOptionalTimestamp(TResultSetParser& parser, TStringBuf columnName) {
    const ssize_t index = parser.ColumnIndex(std::string(columnName.data(), columnName.size()));
    return index >= 0 ? parser.ColumnParser(index).GetOptionalTimestamp() : std::nullopt;
}

TString OptionalString(const std::optional<std::string>& value) {
    return value && !value->empty() ? SanitizeUtf8ForTerminal(*value) : TString("-");
}

TString ShellQuotedSessionId(const std::optional<std::string>& sessionId) {
    if (!sessionId || sessionId->empty()) {
        return "-";
    }

    TString quoted;
    ShellQuoteArg(quoted, SanitizeUtf8ForTerminal(*sessionId));
    return quoted;
}

TString FormatAge(const std::optional<TInstant>& timestamp, TInstant now) {
    if (!timestamp) {
        return "-";
    }

    const TDuration age = now >= *timestamp ? now - *timestamp : TDuration::Zero();
    return TStringBuilder() << HumanReadable(age);
}

TString FormatNodeId(const std::optional<ui32>& nodeId) {
    return nodeId ? ToString(*nodeId) : TString("-");
}

TString CompactQuery(const std::optional<std::string>& query) {
    if (!query || query->empty()) {
        return "-";
    }

    TString compact = CompactUtf8ForTerminal(*query, MaxPrettyQueryLength);
    return compact.empty() ? TString("-") : compact;
}

void PrintListPretty(const TResultSet& resultSet) {
    TPrettyTable table({
        "SESSION_ID",
        "STATE",
        "QUERY_AGE",
        "SESSION_AGE",
        "USER",
        "APPLICATION",
        "POOL",
        "NODE",
        "QUERY",
    });

    const TInstant now = TInstant::Now();
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        table.AddRow()
            .Column(0, ShellQuotedSessionId(GetOptionalUtf8(parser, "SessionId")))
            .Column(1, OptionalString(GetOptionalUtf8(parser, "State")))
            .Column(2, FormatAge(GetOptionalTimestamp(parser, "QueryStartAt"), now))
            .Column(3, FormatAge(GetOptionalTimestamp(parser, "SessionStartAt"), now))
            .Column(4, OptionalString(GetOptionalUtf8(parser, "UserSID")))
            .Column(5, OptionalString(GetOptionalUtf8(parser, "ApplicationName")))
            .Column(6, OptionalString(GetOptionalUtf8(parser, "WmPoolId")))
            .Column(7, FormatNodeId(GetOptionalUint32(parser, "NodeId")))
            .Column(8, CompactQuery(GetOptionalUtf8(parser, "Query")));
    }

    Cout << table;
}

void PrintSessionPretty(const TResultSet& resultSet) {
    TResultSetParser parser(resultSet);
    if (!parser.TryNextRow()) {
        return;
    }

    TPrettyTable table({"FIELD", "VALUE"}, TPrettyTableConfig().WithoutRowDelimiters());
    const auto& columns = resultSet.GetColumnsMeta();
    for (size_t index = 0; index < columns.size(); ++index) {
        table.AddRow()
            .Column(0, columns[index].Name)
            .Column(1, FormatValueJson(parser.GetValue(index), EBinaryStringEncoding::Unicode));
    }

    Cout << table;
}

void PrintListJson(const TResultSet& resultSet) {
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE);
    writer.BeginObject();
    writer.WriteKey("sessions").BeginList();

    TResultSetParser parser(resultSet);
    const auto& columns = resultSet.GetColumnsMeta();
    while (parser.TryNextRow()) {
        FormatResultRowJson(parser, columns, writer, EBinaryStringEncoding::Unicode);
    }

    writer.EndList();
    writer.EndObject();
    Cout << writer.Str() << Endl;
}

void PrintSessionJson(const TResultSet& resultSet) {
    TResultSetParser parser(resultSet);
    if (!parser.TryNextRow()) {
        return;
    }

    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE);
    FormatResultRowJson(parser, resultSet.GetColumnsMeta(), writer, EBinaryStringEncoding::Unicode);
    Cout << writer.Str() << Endl;
}

void PrintMachineReadable(const TResultSet& resultSet, EDataFormat format) {
    TResultSetPrinter printer(TResultSetPrinter::TSettings()
        .SetFormat(format)
        .SetCsvWithHeader(true));
    printer.Print(resultSet);
}

const TResultSet& GetSingleResultSet(const NQuery::TExecuteQueryResult& result) {
    if (result.GetResultSets().size() != 1) {
        throw yexception() << "Expected exactly one result set, got " << result.GetResultSets().size();
    }
    return result.GetResultSets().front();
}

} // anonymous namespace

TCommandSession::TCommandSession()
    : TClientCommandTree("session", {}, "Database session operations")
{
    AddCommand(std::make_unique<TCommandListSessions>());
    AddCommand(std::make_unique<TCommandGetSession>());
    AddCommand(std::make_unique<TCommandTerminateSession>());
}

TCommandListSessions::TCommandListSessions()
    : TYdbSimpleCommand("list", {}, "List database sessions")
{
}

void TCommandListSessions::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("state", "Filter by session state")
        .RequiredArgument("STATE")
        .CompletionArgHelp("Session state")
        .ChoicesWithCompletion({
            {"idle", "Session is waiting for a request"},
            {"executing", "Session is processing a request"},
        })
        .StoreResult(&State);
    config.Opts->AddLongOption("user", "Filter by user SID")
        .RequiredArgument("SID")
        .StoreResult(&User);
    config.Opts->AddLongOption("application", "Filter by application name")
        .RequiredArgument("NAME")
        .StoreResult(&Application);
    config.Opts->AddLongOption("resource-pool", "Filter by workload manager resource pool")
        .RequiredArgument("NAME")
        .StoreResult(&ResourcePool);
    config.Opts->AddLongOption("node-id", "Filter by node ID")
        .RequiredArgument("ID")
        .StoreResult(&NodeId);
    config.Opts->AddLongOption("older-than", "Show sessions whose age is at least this duration (for example, 30s or 5m)")
        .RequiredArgument("DURATION")
        .StoreMappedResult(&OlderThan, ParseDuration);
    config.Opts->AddLongOption("query-running-for", "Show sessions with a query running for at least this duration")
        .RequiredArgument("DURATION")
        .StoreMappedResult(&QueryRunningFor, ParseDuration);
    config.Opts->AddLongOption("limit", "Maximum number of returned sessions (0 means unlimited)")
        .RequiredArgument("NUM")
        .DefaultValue(Limit)
        .StoreResult(&Limit);

    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
        EDataFormat::Csv,
        EDataFormat::Tsv,
    });
}

void TCommandListSessions::Parse(TConfig& config) {
    TYdbSimpleCommand::Parse(config);
    ParseOutputFormats();

    State.to_upper();
    // YQL uses MAX_TIMESTAMP as the exclusive bound for Interval too.
    if (OlderThan.Defined()
        && OlderThan.GetRef().MicroSeconds() >= NYql::NUdf::MAX_TIMESTAMP) {
        throw TMisuseException() << "The --older-than value is too large";
    }
    if (QueryRunningFor.Defined()
        && QueryRunningFor.GetRef().MicroSeconds() >= NYql::NUdf::MAX_TIMESTAMP) {
        throw TMisuseException() << "The --query-running-for value is too large";
    }
}

int TCommandListSessions::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);

    TStringBuilder query;
    query << "DECLARE $current_session_id AS Utf8;\n";
    if (State) {
        query << "DECLARE $state AS Utf8;\n";
    }
    if (User) {
        query << "DECLARE $user AS Utf8;\n";
    }
    if (Application) {
        query << "DECLARE $application AS Utf8;\n";
    }
    if (ResourcePool) {
        query << "DECLARE $resource_pool AS Utf8;\n";
    }
    if (NodeId.Defined()) {
        query << "DECLARE $node_id AS Uint32;\n";
    }
    if (OlderThan.Defined()) {
        query << "DECLARE $older_than AS Interval;\n";
    }
    if (QueryRunningFor.Defined()) {
        query << "DECLARE $query_running_for AS Interval;\n";
    }
    if (Limit) {
        query << "DECLARE $limit AS Uint64;\n";
    }

    query << "SELECT *\n"
          << "FROM `.sys/query_sessions`\n"
          << "WHERE SessionId != $current_session_id\n";
    if (State) {
        query << "  AND State = $state\n";
    }
    if (User) {
        query << "  AND UserSID = $user\n";
    }
    if (Application) {
        query << "  AND ApplicationName = $application\n";
    }
    if (ResourcePool) {
        query << "  AND WmPoolId = $resource_pool\n";
    }
    if (NodeId.Defined()) {
        query << "  AND NodeId = $node_id\n";
    }
    if (OlderThan.Defined()) {
        query << "  AND SessionStartAt <= CurrentUtcTimestamp() - $older_than\n";
    }
    if (QueryRunningFor.Defined()) {
        query << "  AND QueryStartAt <= CurrentUtcTimestamp() - $query_running_for\n";
    }
    query << "ORDER BY State, QueryStartAt, SessionStartAt, SessionId\n";
    if (Limit) {
        query << "LIMIT $limit\n";
    }

    const TString queryText = query;
    auto retrySettings = NQuery::TRetryOperationSettings().Idempotent(true);
    if (ClientTimeout) {
        retrySettings.MaxTimeout(ParseDurationMilliseconds(ClientTimeout));
    }
    auto result = queryClient.RetryQuery(
        [this, &queryText](NQuery::TSession session) {
            TParamsBuilder paramsBuilder;
            paramsBuilder.AddParam("$current_session_id").Utf8(session.GetId()).Build();
            if (State) {
                paramsBuilder.AddParam("$state").Utf8(State).Build();
            }
            if (User) {
                paramsBuilder.AddParam("$user").Utf8(User).Build();
            }
            if (Application) {
                paramsBuilder.AddParam("$application").Utf8(Application).Build();
            }
            if (ResourcePool) {
                paramsBuilder.AddParam("$resource_pool").Utf8(ResourcePool).Build();
            }
            if (NodeId.Defined()) {
                paramsBuilder.AddParam("$node_id").Uint32(NodeId.GetRef()).Build();
            }
            if (OlderThan.Defined()) {
                paramsBuilder.AddParam("$older_than")
                    .Interval(static_cast<i64>(OlderThan.GetRef().MicroSeconds()))
                    .Build();
            }
            if (QueryRunningFor.Defined()) {
                paramsBuilder.AddParam("$query_running_for")
                    .Interval(static_cast<i64>(QueryRunningFor.GetRef().MicroSeconds()))
                    .Build();
            }
            if (Limit) {
                paramsBuilder.AddParam("$limit").Uint64(Limit).Build();
            }

            return session.ExecuteQuery(
                queryText,
                NQuery::TTxControl::NoTx(),
                paramsBuilder.Build(),
                FillSettings(NQuery::TExecuteQuerySettings())
            );
        },
        retrySettings
    ).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const TResultSet& resultSet = GetSingleResultSet(result);
    if (resultSet.Truncated()) {
        Cerr << "Session list result was truncated; use more selective filters or a lower --limit value" << Endl;
        return EXIT_FAILURE;
    }
    if (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty) {
        PrintListPretty(resultSet);
    } else if (OutputFormat == EDataFormat::Json) {
        PrintListJson(resultSet);
    } else {
        PrintMachineReadable(resultSet, OutputFormat);
    }
    return EXIT_SUCCESS;
}

void TCommandWithSessionId::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<session-id>", "Session ID (quote it when entering it in a shell)");
}

void TCommandWithSessionId::Parse(TConfig& config) {
    TYdbSimpleCommand::Parse(config);

    SessionId = config.ParseResult->GetFreeArgs()[0];
    if (!SessionId) {
        throw TMisuseException() << "Session ID cannot be empty";
    }
    if (SessionId.StartsWith("ydb://session/")
        && SessionId.find("?id=") == TString::npos
        && SessionId.find("&id=") == TString::npos) {
        throw TMisuseException()
            << "Session ID seems incomplete. Quote the full value because '?' and '&' are interpreted by the shell";
    }
}

TCommandGetSession::TCommandGetSession()
    : TCommandWithSessionId("get", {}, "Get details of a database session")
{
}

void TCommandGetSession::Config(TConfig& config) {
    TCommandWithSessionId::Config(config);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
        EDataFormat::Csv,
        EDataFormat::Tsv,
    });
}

void TCommandGetSession::Parse(TConfig& config) {
    TCommandWithSessionId::Parse(config);
    ParseOutputFormats();
}

int TCommandGetSession::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);

    TStringBuilder query;
    query << "DECLARE $session_id AS Utf8;\n"
          << "SELECT *\n"
          << "FROM `.sys/query_sessions`\n"
          << "WHERE SessionId = $session_id\n";

    auto params = TParamsBuilder()
        .AddParam("$session_id").Utf8(SessionId).Build()
        .Build();
    auto result = queryClient.ExecuteQuery(
        query,
        NQuery::TTxControl::NoTx(),
        params,
        FillSettings(NQuery::TExecuteQuerySettings()
            .RetrySettings(NQuery::TRetryOperationSettings().Idempotent(true)))
    ).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const TResultSet& resultSet = GetSingleResultSet(result);
    if (resultSet.RowsCount() == 0) {
        Cerr << "Session not found or not visible: " << SessionId << Endl;
        return EXIT_FAILURE;
    }

    if (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty) {
        PrintSessionPretty(resultSet);
    } else if (OutputFormat == EDataFormat::Json) {
        PrintSessionJson(resultSet);
    } else {
        PrintMachineReadable(resultSet, OutputFormat);
    }
    return EXIT_SUCCESS;
}

TCommandTerminateSession::TCommandTerminateSession()
    : TCommandWithSessionId("terminate", {}, "Request termination of a database session")
{
}

int TCommandTerminateSession::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(
        queryClient.DeleteSession(
            SessionId,
            FillSettings(NQuery::TDeleteSessionSettings())
        ).GetValueSync()
    );

    Cout << "Termination requested for session " << SessionId << Endl;
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
