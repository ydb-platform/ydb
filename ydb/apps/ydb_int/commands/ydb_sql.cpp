#include "ydb_sql.h"

#include <library/cpp/json/json_reader.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/waiting_bar.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <util/generic/queue.h>
#include <google/protobuf/text_format.h>

namespace NYdb {
namespace NConsoleClient {

using namespace NKikimr::NOperationId;

namespace {

// Command names are kept in one place to make a future move into the open-source
// `ydb` binary easier (where `experimental` parent command does not exist).
constexpr TStringBuf SqlCommandName = "sql";
constexpr TStringBuf SqlOperationCommandName = "sql-operation";

// Wraps `arg` in single quotes if it contains shell-significant characters, so that
// copy-pasted hint commands work even when option values contain spaces or quotes.
TString ShellQuoteIfNeeded(TStringBuf arg) {
    if (arg.find_first_of(" \t\n\"'\\$`*?[]{}()|&;<>!#~") == TStringBuf::npos && !arg.empty()) {
        return TString(arg);
    }
    TStringBuilder result;
    result << '\'';
    for (char c : arg) {
        if (c == '\'') {
            result << "'\\''";
        } else {
            result << c;
        }
    }
    result << '\'';
    return result;
}

// Builds a command line prefix from the original argv (binary path + everything the user
// typed) up to the `sql` or `sql-operation` token, then appends `sql-operation <subCommand>`.
// Using InitialArgV/InitialArgC (not ArgV/ArgC, which get shifted as subcommands are parsed)
// keeps the hint identical to how the tool was actually invoked.
TString MakeSqlOperationCommand(const TClientCommand::TConfig& config, TStringBuf subCommand) {
    TStringBuilder result;
    result << ShellQuoteIfNeeded(config.InitialArgV[0]);
    for (int i = 1; i < config.InitialArgC; ++i) {
        TStringBuf arg(config.InitialArgV[i]);
        if (arg == SqlCommandName || arg == SqlOperationCommandName) {
            break;
        }
        result << ' ' << ShellQuoteIfNeeded(arg);
    }
    result << ' ' << SqlOperationCommandName << ' ' << subCommand;
    return result;
}

} // anonymous namespace

void TCommandExecuteSqlBase::DeclareScriptOptions(TClientCommand::TConfig& config) {
    config.Opts->AddLongOption('s', "script", "Script (query) text to execute").RequiredArgument("[String]")
        .StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with script (query) text").RequiredArgument("PATH")
        .StoreResult(&QueryFile);
}

void TCommandExecuteSqlBase::DeclareCommonInputOptions(TClientCommand::TConfig& config) {
    config.Opts->AddLongOption("stats", "Execution statistics collection mode [none, basic, full, profile]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption("syntax", "Query syntax [yql, pg]")
        .RequiredArgument("[String]").DefaultValue("yql").StoreResult(&Syntax)
        .Hidden();
    config.Opts->AddLongOption("results-ttl", "Amount of time to store script execution results on server "
        "(for async operations only). By default it is 86400s (24 hours).")
        .RequiredArgument("SECONDS").StoreResult(&ResultsTtl);

    AddParametersOption(config);

    AddDefaultParamFormats(config);

    AddBatchParametersOptions(config, "script");

    CheckExamples(config);
}

void TCommandExecuteSqlBase::DeclareCommonOutputOptions(TClientCommand::TConfig& config) {
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::JsonUnicode,
        EDataFormat::JsonUnicodeArray,
        EDataFormat::JsonBase64,
        EDataFormat::JsonBase64Array,
        EDataFormat::Csv,
        EDataFormat::Tsv,
        EDataFormat::Parquet,
    });
}

void TCommandExecuteSqlBase::ParseCommonInputOptions(TClientCommand::TConfig& config) {
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of script\" (\"--script\", \"-s\") "
            << "and \"Path to file with script text\" (\"--file\", \"-f\") were provided.";
    }
    if (QueryFile) {
        if (QueryFile == "-") {
            if (IsStdinInteractive()) {
                throw TMisuseException() << "Path to script file is \"-\", meaning that script text should be read "
                    "from stdin. This is only available in non-interactive mode";
            }
            if (ReadingSomethingFromStdin) {
                throw TMisuseException() << "Can't read both script file and parameters from stdin";
            }
            ReadingSomethingFromStdin = true;
            Query = Cin.ReadAll();
        } else {
            Query = ReadFromFile(QueryFile, "query");
        }
    }
    if (Query.empty()) {
        throw TMisuseException() << "Neither text of script (\"--script\", \"-s\") "
            << "nor path to file with script text (\"--file\", \"-f\") were provided.";
    }
    // Should be called after setting ReadingSomethingFromStdin
    ParseParameters(config);
}

void TCommandExecuteSqlBase::ParseCommonOutputOptions() {
    ParseOutputFormats();
}

int TCommandExecuteSqlBase::ExecuteScriptAsync(TClientCommand::TConfig& config, TDriver& driver, NQuery::TQueryClient& queryClient) {
    NQuery::TExecuteScriptSettings settings;

    // Execute query
    settings.ExecMode(NQuery::EExecMode::Execute);
    settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, NQuery::EStatsMode::None));

    if (Syntax == "yql") {
        settings.Syntax(NQuery::ESyntax::YqlV1);
    } else if (Syntax == "pg") {
        settings.Syntax(NQuery::ESyntax::Pg);
    } else {
        throw TMisuseException() << "Unknown syntax option \"" << Syntax << "\"";
    }
    settings.ResultsTtl(ResultsTtl);

    if (!Parameters.empty() || InputParamStream) {
        // Execute query with parameters
        if (InputFramingFormat != EFramingFormat::Default && InputFramingFormat != EFramingFormat::NoFraming) {
            throw TMisuseException() << "Can't execute several async scripts (with \""
                << InputFramingFormat << "\" framing format).";
        }
        THolder<TParamsBuilder> paramBuilder;
        if (GetNextParams(driver, Query, paramBuilder, config.IsVerbose())) {
            // Execute script with parameters
            Operation = queryClient.ExecuteScript(
                Query,
                paramBuilder->Build(),
                settings
            ).GetValueSync();
        } else {
            Cerr << "No parameters provided." << Endl;
            return EXIT_FAILURE;
        }
    } else {
        // Execute script without parameters
        Operation = queryClient.ExecuteScript(
            Query,
            settings
        ).GetValueSync();
    }
    NStatusHelpers::ThrowOnErrorOrPrintIssues(Operation.value().Status());
    if (AsyncWait) {
        Cerr << "Operation \"" << Operation->Id().ToString() << "\" started." << Endl;
        NOperation::TOperationClient operationClient(driver);
        return WaitAndPrintResults(queryClient, operationClient, Operation->Id());
    } else {
        return PrintOperationInfo(config);
    }
}

int TCommandExecuteSqlBase::PrintOperationInfo(const TClientCommand::TConfig& config) {
    Y_ENSURE(Operation.has_value());
    Cout << "Operation info:" << Endl << Operation->ToString() << Endl;
    std::string quotedId = "\"" + Operation->Id().ToString() + "\"";
    Cerr << "To wait for completion and fetch results: " << MakeSqlOperationCommand(config, "fetch") << " " << quotedId << Endl
        << "To get current execution status: " << MakeSqlOperationCommand(config, "get") << " " << quotedId << Endl
        << "To wait for completion only: " << MakeSqlOperationCommand(config, "wait") << " " << quotedId << Endl
        << "To cancel operation: " << MakeSqlOperationCommand(config, "cancel") << " " << quotedId << Endl
        << "To forget operation: " << MakeSqlOperationCommand(config, "forget") << " " << quotedId << Endl;
    return EXIT_SUCCESS;
}

int TCommandExecuteSqlBase::WaitAndPrintResults(
        NQuery::TQueryClient& queryClient,
        NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId) {
    if (!WaitForCompletion(operationClient, operationId)) {
        return EXIT_FAILURE;
    }
    return PrintScriptResults(queryClient);
}

bool TCommandExecuteSqlBase::WaitForCompletion(NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId) {
    SetInterruptHandlers();
    bool firstGetOperation = true;
    TWaitingBar waitingBar("Waiting for script execution to finish... ");
    while (!IsInterrupted()) {
        NQuery::TScriptExecutionOperation execScriptOperation = (firstGetOperation && Operation)
            ? Operation.value()
            : operationClient
                .Get<NQuery::TScriptExecutionOperation>(operationId)
                .GetValueSync();
        firstGetOperation = false;

        // Surface any failure status from polling immediately, otherwise the loop
        // would silently retry forever on terminal server-side errors. Use ThrowOnError
        // (not ThrowOnErrorOrPrintIssues) here so that transient in-progress issues are not
        // re-printed each poll iteration; issues will still be reported via the exception.
        NStatusHelpers::ThrowOnError(execScriptOperation.Status());
        if (!execScriptOperation.Ready()) {
            waitingBar.Render();
            Sleep(TDuration::Seconds(1));
            continue;
        }
        Operation = execScriptOperation;
        waitingBar.Finish(/* cleanup */ true);
        // Print any final-status issues once, now that the operation is ready.
        NStatusHelpers::ThrowOnErrorOrPrintIssues(execScriptOperation.Status());
        return true;
    }

    waitingBar.Finish(/* cleanup */ false);
    Cerr << "<INTERRUPTED>" << Endl;
    return false;
}

int TCommandExecuteSqlBase::PrintScriptResults(NQuery::TQueryClient& queryClient) {
    Y_ENSURE(Operation.has_value());
    TResultSetPrinter printer(OutputFormat, &IsInterrupted);

    for (size_t resultSetIndex = 0; resultSetIndex < Operation->Metadata().ResultSetsMeta.size(); ++resultSetIndex) {
        // Stored by value, not by pointer: the underlying response is destroyed at the end of the
        // iteration, so a pointer into it would dangle on the next loop iteration.
        std::string nextFetchToken;
        while (!IsInterrupted()) {
            NYdb::NQuery::TFetchScriptResultsSettings settings;
            if (!nextFetchToken.empty()) {
                settings.FetchToken(nextFetchToken);
            }

            // TODO: fetch with retries
            auto asyncResult = queryClient.FetchScriptResults(Operation->Id(), resultSetIndex, settings);
            auto result = asyncResult.GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

            if (result.HasResultSet()) {
                printer.Print(result.ExtractResultSet());
            }

            if (result.GetNextFetchToken().empty()) {
                break;
            }
            nextFetchToken = result.GetNextFetchToken();
        }
    }
    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

TCommandSqlExperimental::TCommandSqlExperimental()
    : TYdbCommand("sql", {}, "Execute SQL script (query)")
    , TCommandExecuteSqlBase()
{}

void TCommandSqlExperimental::Config(TConfig& config) {
    TClientCommand::Config(config);
    DeclareScriptOptions(config);
    config.Opts->AddLongOption("explain", "Execute explain request for the query. Shows query logical plan. "
            "The query is not actually executed, thus does not affect the database.")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query execution plan. "
            "Query results are ignored.\n"
            "Important note: The query is actually executed, so any changes will be applied to the database.")
        .StoreTrue(&ExplainAnalyzeMode);
    config.Opts->AddLongOption("async", TStringBuilder() << "Execute script (query) asynchronously. "
            "Operation will be started on server and its Id will be printed. "
            "To get operation status use \"" << MakeSqlOperationCommand(config, "get") << "\" command. "
            "To fetch query results use \"" << MakeSqlOperationCommand(config, "fetch") << "\" command.\n"
            "Note: query results will be stored on server and thus consume storage resources. "
            "Use --results-ttl option to set how long results should be stored for")
        .StoreTrue(&RunAsync);
    config.Opts->AddLongOption("async-wait",
            "Execute script (query) asynchronously and wait for results (using polling).\n"
            "The difference between using and not-using this option:\n"
            "  - If YDB CLI loses connection to the server when running a query without this option, "
            "the stream breaks and command execution finishes with error. "
            "If this option is used, the polling process continues and query results may still be received after reconnect.\n"
            "  - Using this option will probably reduce performance due to artificial delays between polling requests.\n"
            "Note: query results will be stored on server and thus consume storage resources. "
            "Use --results-ttl option to set how long results should be stored for")
        .StoreTrue(&AsyncWait);
    DeclareCommonInputOptions(config);
    DeclareCommonOutputOptions(config);

    config.SetFreeArgsNum(0);
}

void TCommandSqlExperimental::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseCommonInputOptions(config);
    ParseCommonOutputOptions();
    if (ExplainMode && ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExplainAnalyzeMode && !CollectStatsMode.empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain-analyze mode. "
            "Relevant for execution mode only.";
    }
    if (ExplainMode && !CollectStatsMode.empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain mode. "
            "Relevant for execution mode only.";
    }
    if ((ExplainAnalyzeMode || ExplainMode) && (AsyncWait || RunAsync)) {
        throw TMisuseException() << "Explain and explain-analyze modes can't be run in async mode";
    }
}

int TCommandSqlExperimental::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    if (RunAsync || AsyncWait) {
        return ExecuteScriptAsync(config, driver, client);
    } else {
        // Single stream execution
        return StreamExecuteQuery(client);
    }
}

int TCommandSqlExperimental::StreamExecuteQuery(NQuery::TQueryClient& client) {
    NQuery::TExecuteQuerySettings settings;

    if (ExplainMode) {
        // Execute explain request for the query
        settings.ExecMode(NQuery::EExecMode::Explain);
    } else {
        // Execute query
        settings.ExecMode(NQuery::EExecMode::Execute);
        auto defaultStatsMode = ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
        settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode));
    }
    if (Syntax == "yql") {
        settings.Syntax(NQuery::ESyntax::YqlV1);
    } else if (Syntax == "pg") {
        settings.Syntax(NQuery::ESyntax::Pg);
    } else {
        throw TMisuseException() << "Unknown syntax option \"" << Syntax << "\"";
    }
    // Execute query without parameters
    auto asyncResult = client.StreamExecuteQuery(
        Query,
        NQuery::TTxControl::NoTx(),
        settings
    );

    auto result = asyncResult.GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return PrintResponse(result);
}

int TCommandSqlExperimental::PrintResponse(NQuery::TExecuteQueryIterator& result) {
    std::optional<TString> stats;
    std::optional<TString> plan;
    SetInterruptHandlers();
    {
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (streamPart.EOS()) {
                    break;
                }
                NStatusHelpers::ThrowOnErrorOrPrintIssues(streamPart);
            }

            if (streamPart.HasResultSet() && !ExplainAnalyzeMode) {
                printer.Print(streamPart.GetResultSet());
            }

            if (streamPart.GetStats().has_value()) {
                const auto& queryStats = *streamPart.GetStats();
                stats = queryStats.ToString();

                if (queryStats.GetPlan()) {
                    plan = queryStats.GetPlan();
                }
            }
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (stats && !ExplainMode && !ExplainAnalyzeMode) {
        Cout << Endl << "Statistics:" << Endl << *stats;
    }

    if (plan) {
        if (!ExplainMode && !ExplainAnalyzeMode
                && (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty)) {
            Cout << Endl << "Execution plan:" << Endl;
        }
        // TODO: get rid of pretty-table format, refactor TQueryPrinter to reflect that
        EDataFormat format = (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty)
            && (ExplainMode || ExplainAnalyzeMode)
            ? EDataFormat::PrettyTable : OutputFormat;
        TQueryPlanPrinter queryPlanPrinter(format, /* show actual costs */ !ExplainMode);
        queryPlanPrinter.Print(*plan);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

void TCommandSqlExperimental::SetScript(TString&& script) {
    Query = std::move(script);
}

void TCommandSqlExperimental::SetCollectStatsMode(TString&& collectStatsMode) {
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandSqlExperimental::SetSyntax(TString&& syntax) {
    Syntax = std::move(syntax);
}

TCommandSqlOperation::TCommandSqlOperation()
    : TClientCommandTree("sql-operation", {}, "Async script execution operations")
{
    AddCommand(std::make_unique<TCommandSqlOperationExecute>());
    AddCommand(std::make_unique<TCommandSqlOperationFetch>());
    AddCommand(std::make_unique<TCommandSqlOperationGet>());
    AddCommand(std::make_unique<TCommandSqlOperationWait>());
    AddCommand(std::make_unique<TCommandSqlOperationCancel>());
    AddCommand(std::make_unique<TCommandSqlOperationForget>());
    AddCommand(std::make_unique<TCommandSqlOperationList>());
}

TCommandSqlOperationExecute::TCommandSqlOperationExecute()
    : TYdbCommand("execute", {{"exec"}}, "Execute SQL script asynchronously")
    , TCommandExecuteSqlBase()
{}

void TCommandSqlOperationExecute::Config(TConfig& config) {
    TYdbCommand::Config(config);
    DeclareScriptOptions(config);
    config.Opts->AddLongOption("wait",
            "Wait for script execution completion (using polling) and print results")
        .StoreTrue(&AsyncWait);
    DeclareCommonInputOptions(config);
    DeclareCommonOutputOptions(config);

    config.SetFreeArgsNum(0);
}

void TCommandSqlOperationExecute::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseCommonInputOptions(config);
    ParseCommonOutputOptions();
}

int TCommandSqlOperationExecute::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    return ExecuteScriptAsync(config, driver, client);
}

void TCommandWithScriptExecutionOperationId::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<operation_id>", "ID of existing operation of type SCRIPT_EXECUTION");
}

void TCommandWithScriptExecutionOperationId::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    try {
        OperationId = TOperationId(config.ParseResult->GetFreeArgs()[0]);
    } catch (const yexception& ex) {
        throw TMisuseException() << "Invalid operation ID";
    }
    if (OperationId.GetKind() != TOperationId::EKind::SCRIPT_EXECUTION) {
        throw TMisuseException() << "Invalid operation kind: expected SCRIPT_EXECUTION";
    }
}

TCommandSqlOperationFetch::TCommandSqlOperationFetch()
    : TCommandWithScriptExecutionOperationId("fetch", {}, "Fetch results of async script execution by operation id")
    , TCommandExecuteSqlBase()
{}

void TCommandSqlOperationFetch::Config(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Config(config);

    DeclareCommonOutputOptions(config);
}

void TCommandSqlOperationFetch::Parse(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Parse(config);
    ParseCommonOutputOptions();
}

int TCommandSqlOperationFetch::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);
    NOperation::TOperationClient operationClient(driver);
    return WaitAndPrintResults(queryClient, operationClient, OperationId);
}

TCommandSqlOperationGet::TCommandSqlOperationGet()
    : TCommandWithScriptExecutionOperationId("get", {}, "Get and print current status of async script execution by operation id")
{}

void TCommandSqlOperationGet::Config(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Config(config);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::ProtoJsonBase64,
    });
}

void TCommandSqlOperationGet::Parse(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Parse(config);
    ParseOutputFormats();
}

namespace {
    IOutputStream& operator<<(IOutputStream& out, NQuery::ESyntax syntax) {
        switch (syntax) {
            case NQuery::ESyntax::Pg:
                return out << "PostgreSQL";
            case NQuery::ESyntax::YqlV1:
                return out << "YQL";
            default:
                return out << "Unknown";
        }
    }
}

int TCommandSqlOperationGet::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NQuery::TScriptExecutionOperation operation = client
        .Get<NQuery::TScriptExecutionOperation>(OperationId)
        .GetValueSync();
    std::string quotedId = "\"" + operation.Id().ToString() + "\"";
    auto printOperationInfo = [&]() {
        PrintOperation(operation, OutputFormat);
        // Print extra human-readable fields only in Pretty format, otherwise machine-readable
        // output (e.g. proto-json-base64) would be corrupted by these free-form lines.
        if (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty) {
            const auto& metadata = operation.Metadata();
            Cout << "Script syntax: " << metadata.ScriptContent.Syntax << Endl;
            Cout << "Script text: \"" << metadata.ScriptContent.Text << "\"" << Endl << Endl;
        }
    };
    switch (operation.Status().GetStatus()) {
        case EStatus::SUCCESS:
            printOperationInfo();
            if (operation.Ready()) {
                Cerr << "To fetch results: " << MakeSqlOperationCommand(config, "fetch") << " " << quotedId << Endl;
            } else {
                Cerr << "To wait for completion and fetch results: " << MakeSqlOperationCommand(config, "fetch") << " " << quotedId << Endl
                    << "To wait for completion only: " << MakeSqlOperationCommand(config, "wait") << " " << quotedId << Endl
                    << "To cancel operation: " << MakeSqlOperationCommand(config, "cancel") << " " << quotedId << Endl;
            }
            Cerr << "To forget operation: " << MakeSqlOperationCommand(config, "forget") << " " << quotedId << Endl;
            return EXIT_SUCCESS;
        case EStatus::CANCELLED:
            printOperationInfo();
            Cerr << "To forget operation: " << MakeSqlOperationCommand(config, "forget") << " " << quotedId << Endl;
            return EXIT_FAILURE;
        default:
            NStatusHelpers::ThrowOnErrorOrPrintIssues(operation.Status());
            return EXIT_FAILURE;
    }
}

TCommandSqlOperationWait::TCommandSqlOperationWait()
    : TCommandWithScriptExecutionOperationId("wait", {}, "Wait for async script execution completion by operation id")
    , TCommandExecuteSqlBase()
{}

int TCommandSqlOperationWait::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    if (WaitForCompletion(client, OperationId)) {
        std::string quotedId = "\"" + OperationId.ToString() + "\"";
        Cerr << "Operation completed." << Endl
            << "To fetch results: " << MakeSqlOperationCommand(config, "fetch") << " " << quotedId << Endl
            << "To forget operation: " << MakeSqlOperationCommand(config, "forget") << " " << quotedId << Endl;
        return EXIT_SUCCESS;
    } else {
        return EXIT_FAILURE;
    }
}

TCommandSqlOperationCancel::TCommandSqlOperationCancel()
    : TCommandWithScriptExecutionOperationId("cancel", {}, "Cancel async script execution by operation id")
{}

int TCommandSqlOperationCancel::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Cancel(OperationId).GetValueSync());
    Cerr << "Operation cancelled. To forget operation: " << MakeSqlOperationCommand(config, "forget")
         << " \"" << OperationId.ToString() << "\"" << Endl;
    return EXIT_SUCCESS;
}

TCommandSqlOperationForget::TCommandSqlOperationForget()
    : TCommandWithScriptExecutionOperationId("forget", {}, "Forget async script execution operation by id")
{}

int TCommandSqlOperationForget::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Forget(OperationId).GetValueSync());
    return EXIT_SUCCESS;
}

TCommandSqlOperationList::TCommandSqlOperationList()
    : TYdbCommand("list", {}, "List async script execution operations")
{}

void TCommandSqlOperationList::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption('s', "page-size", "Page size")
        .RequiredArgument("NUM").StoreResult(&PageSize);
    config.Opts->AddLongOption('t', "page-token", "Page token")
        .RequiredArgument("STRING").StoreResult(&PageToken);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });

    config.SetFreeArgsNum(0);
}

void TCommandSqlOperationList::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandSqlOperationList::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NOperation::TOperationsList<NQuery::TScriptExecutionOperation> operations = client.List<NQuery::TScriptExecutionOperation>(
        PageSize, PageToken)
        .GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(operations);
    PrintOperationsList(operations, OutputFormat);
    return EXIT_SUCCESS;
}


}
}
