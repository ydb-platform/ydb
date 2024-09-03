#include "ydb_sql.h"

#include <library/cpp/json/json_reader.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/waiting_bar.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <util/generic/queue.h>
#include <google/protobuf/text_format.h>

namespace NYdb {
namespace NConsoleClient {

using namespace NKikimr::NOperationId;

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
        .RequiredArgument("SECONDS").DefaultValue("86400").StoreResult(&ResultsTtl);
}

void TCommandExecuteSqlBase::DeclareCommonOutputOptions(TClientCommand::TConfig& config) {
    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
        EOutputFormat::Csv,
        EOutputFormat::Tsv,
        EOutputFormat::Parquet,
    });
}

void TCommandExecuteSqlBase::ParseCommonInputOptions() {
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of query\" (\"--query\", \"-q\") "
            << "and \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
    if (QueryFile) {
        Query = ReadFromFile(QueryFile, "query");
    }
}

void TCommandExecuteSqlBase::ParseCommonOutputOptions() {
    ParseFormats();
}

int TCommandExecuteSqlBase::ExecuteScriptAsync(TDriver& driver, NQuery::TQueryClient& queryClient) {
    NQuery::TExecuteScriptSettings settings;

        // Execute query
    settings.ExecMode(NQuery::EExecMode::Execute);
    settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, NQuery::EStatsMode::None));

    if (ResultsTtl) {
        settings.ResultsTtl(TDuration::Seconds(FromString<ui64>(ResultsTtl)));
    }

    // Execute query without parameters
    Operation = queryClient.ExecuteScript(
        Query,
        settings
    ).GetValueSync();
    ThrowOnError(Operation.value().Status());
    if (AsyncWait) {
        Cerr << "Operation \"" << ProtoToString(Operation->Id()) << "\" started." << Endl;
        NOperation::TOperationClient operationClient(driver);
        return WaitAndPrintResults(queryClient, operationClient, Operation->Id());
    } else {
        return PrintOperationInfo();
    }
}

int TCommandExecuteSqlBase::PrintOperationInfo() {
    Y_ENSURE(Operation.has_value());
    Cout << "Operation info:" << Endl << Operation->ToString() << Endl;
    std::string quotedId = "\"" + ProtoToString(Operation->Id()) + "\"";
    Cerr << "To wait for completion and fetch results: ydb sql-async fetch " << quotedId << Endl
        << "To get current execution status: ydb sql-async get " << quotedId << Endl
        << "To wait for completion only: ydb sql-async wait " << quotedId << Endl
        << "To cancel operation: ydb sql-async cancel " << quotedId << Endl
        << "To forget operation: ydb sql-async forget " << quotedId << Endl;
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

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
            return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

bool TCommandExecuteSqlBase::WaitForCompletion(NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId) {
    SetInterruptHandlers();
    // Throw exception on getting operation info if it is the first getOperation, then retry
    bool firstGetOperation = true;
    TWaitingBar waitingBar("Waiting for script execution to finish... ");
    while (!IsInterrupted()) {
        NQuery::TScriptExecutionOperation execScriptOperation = (firstGetOperation && Operation)
            ? Operation.value()
            : operationClient
                .Get<NQuery::TScriptExecutionOperation>(operationId)
                .GetValueSync();
        if (firstGetOperation) {
            ThrowOnError(execScriptOperation.Status());
            firstGetOperation = false;
        }
        if (!execScriptOperation.Status().IsSuccess() || !execScriptOperation.Ready()) {
            waitingBar.Render();
            Sleep(TDuration::Seconds(1));
            continue;
        }
        Operation = execScriptOperation;
        waitingBar.Finish(true);
        break;
    }
    waitingBar.Finish(false);

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
            return false;
    }
    return true;
}

int TCommandExecuteSqlBase::PrintScriptResults(NQuery::TQueryClient& queryClient) {
    Y_ENSURE(Operation.has_value());
    TResultSetPrinter printer(OutputFormat, &IsInterrupted);

    for (size_t resultSetIndex = 0; resultSetIndex < Operation->Metadata().ResultSetsMeta.size(); ++resultSetIndex) {
        TString const* nextFetchToken = nullptr;
        while (!IsInterrupted()) {
            NYdb::NQuery::TFetchScriptResultsSettings settings;
            if (nextFetchToken != nullptr) {
                settings.FetchToken(*nextFetchToken);
            }

            // TODO: fetch with retries
            auto asyncResult = queryClient.FetchScriptResults(Operation->Id(), resultSetIndex, settings);
            auto result = asyncResult.GetValueSync();

            if (result.HasResultSet()) {
                printer.Print(result.ExtractResultSet());
            }

            if (result.GetNextFetchToken()) {
                nextFetchToken = &result.GetNextFetchToken();
            } else {
                break;
            }
        }
    }
    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
            return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

TCommandSql::TCommandSql()
    : TYdbCommand("sql", {}, "Execute SQL script (query)")
{}

void TCommandSql::Config(TConfig& config) {
    TClientCommand::Config(config);
    DeclareScriptOptions(config);
    config.Opts->AddLongOption("explain", "Execute explain request for the query. Shows query logical plan. "
            "The query is not actually executed, thus does not affect the database.")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query execution plan. "
            "Query results are ignored.\n"
            "Important note: The query is actually executed, so any changes will be applied to the database.")
        .StoreTrue(&ExplainAnalyzeMode);
    config.Opts->AddLongOption("async", "Execute script (query) asynchronously. "
            "Operation will be started on server and its Id will be printed. "
            "To get operation status use \"ydb sql-async get\" command. "
            "To fetch query results use \"ydb sql-async fetch\" command.\n"
            "Note: query results will be stored on server and thus consume storage resources. "
            "Use --results-ttl option to set how long results should be stored for")
        .StoreTrue(&RunAsync);
    config.Opts->AddLongOption("async-wait",
            "Execute script (query) asynchronously and wait for results (using polling).\n"
            "The difference between using and not-using this option:\n"
            "  - If YDB CLI loses connection to the server when running a query without this option, "
            "the stream breaks and command execution finishes with error. "
            "If this option is used, the polling process continues and query results may still be received after reconnect.\n"
            "  - Using this option will probably reduce performance due to artifitial delays between polling requests.\n"
            "Note: query results will be stored on server and thus consume storage resources. "
            "Use --results-ttl option to set how long results should be stored for")
        .StoreTrue(&AsyncWait);
    DeclareCommonInputOptions(config);
    DeclareCommonOutputOptions(config);

    config.SetFreeArgsNum(0);
}

void TCommandSql::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseCommonInputOptions();
    ParseCommonOutputOptions();
    if (ExplainMode && ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExplainAnalyzeMode && !CollectStatsMode.Empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain-analyze mode. "
            "Relevant for execution mode only.";
    }
    if (ExplainMode && !CollectStatsMode.Empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain mode"
            "Relevant for execution mode only.";
    }
    if ((ExplainAnalyzeMode || ExplainMode) && (AsyncWait || RunAsync)) {
        throw TMisuseException() << "Analyze modes can't be run in async mode";
    }
}

int TCommandSql::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    if (RunAsync || AsyncWait) {
        return ExecuteScriptAsync(driver, client);
    } else {
        // Single stream execution
        return StreamExecuteQuery(client);
    }
}

int TCommandSql::StreamExecuteQuery(NQuery::TQueryClient& client) {
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
    ThrowOnError(result);
    return PrintResponse(result);
}

int TCommandSql::PrintResponse(NQuery::TExecuteQueryIterator& result) {
    TMaybe<TString> stats;
    TMaybe<TString> plan;
    SetInterruptHandlers();
    {
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (streamPart.EOS()) {
                    break;
                }
                ThrowOnError(streamPart);
            }

            if (streamPart.HasResultSet() && !ExplainAnalyzeMode) {
                printer.Print(streamPart.GetResultSet());
            }

            if (!streamPart.GetStats().Empty()) {
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
                && (OutputFormat == EOutputFormat::Default || OutputFormat == EOutputFormat::Pretty)) {
            Cout << Endl << "Execution plan:" << Endl;
        }
        // TODO: get rid of pretty-table format, refactor TQueryPrinter to reflect that
        EOutputFormat format = (OutputFormat == EOutputFormat::Default || OutputFormat == EOutputFormat::Pretty)
            && (ExplainMode || ExplainAnalyzeMode)
            ? EOutputFormat::PrettyTable : OutputFormat;
        TQueryPlanPrinter queryPlanPrinter(format, /* show actual costs */ !ExplainMode);
        queryPlanPrinter.Print(*plan);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

void TCommandSql::SetScript(TString&& script) {
    Query = std::move(script);
}

void TCommandSql::SetCollectStatsMode(TString&& collectStatsMode) {
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandSql::SetSyntax(TString&& syntax) {
    Syntax = std::move(syntax);
}

TCommandSqlAsync::TCommandSqlAsync()
    : TClientCommandTree("sql-async", {}, "Async script execution operations")
{
    AddCommand(std::make_unique<TCommandSqlAsyncExecute>());
    AddCommand(std::make_unique<TCommandSqlAsyncFetch>());
    AddCommand(std::make_unique<TCommandSqlAsyncGet>());
    AddCommand(std::make_unique<TCommandSqlAsyncWait>());
    AddCommand(std::make_unique<TCommandSqlAsyncCancel>());
    AddCommand(std::make_unique<TCommandSqlAsyncForget>());
    AddCommand(std::make_unique<TCommandSqlAsyncList>());
}

TCommandSqlAsyncExecute::TCommandSqlAsyncExecute()
    : TYdbCommand("execute", {{"exec"}}, "Execute SQL script asynchronously")
{}

void TCommandSqlAsyncExecute::Config(TConfig& config) {
    TYdbCommand::Config(config);
    DeclareScriptOptions(config);
    config.Opts->AddLongOption("wait",
            "Wait for script execution completion (using polling) and print results")
        .StoreTrue(&AsyncWait);
    DeclareCommonInputOptions(config);
    DeclareCommonOutputOptions(config);

    config.SetFreeArgsNum(0);
}

void TCommandSqlAsyncExecute::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseCommonInputOptions();
    ParseCommonOutputOptions();
}

int TCommandSqlAsyncExecute::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    return ExecuteScriptAsync(driver, client);
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
    if (OperationId.GetKind() != TOperationId::EKind::TOperationId_EKind_SCRIPT_EXECUTION) {
        throw TMisuseException() << "Invalid operation kind: expected SCRIPT_EXECUTION";
    }
}

TCommandSqlAsyncFetch::TCommandSqlAsyncFetch()
    : TCommandWithScriptExecutionOperationId("fetch", {}, "Fetch results of async script execution by operation id")
{}

void TCommandSqlAsyncFetch::Config(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Config(config);

    DeclareCommonOutputOptions(config);
}

void TCommandSqlAsyncFetch::Parse(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Parse(config);
    ParseCommonOutputOptions();
}

int TCommandSqlAsyncFetch::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);
    NOperation::TOperationClient operationClient(driver);
    return WaitAndPrintResults(queryClient, operationClient, OperationId);
}

TCommandSqlAsyncGet::TCommandSqlAsyncGet()
    : TCommandWithScriptExecutionOperationId("get", {}, "Get and print current status of async script execution by operation id")
{}

void TCommandSqlAsyncGet::Config(TConfig& config) {
    TCommandWithScriptExecutionOperationId::Config(config);
    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::ProtoJsonBase64,
    });
}

namespace {
    IOutputStream& operator<<(IOutputStream& out, NQuery::ESyntax syntax) {
        switch (syntax) {
            case NQuery::ESyntax::Pg:
                return out << "PostgresQL";
            case NQuery::ESyntax::YqlV1:
                return out << "YQL";
            default:
                return out << "Unknown";
        }
    }
}

int TCommandSqlAsyncGet::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NQuery::TScriptExecutionOperation operation = client
        .Get<NQuery::TScriptExecutionOperation>(OperationId)
        .GetValueSync();
    std::string quotedId = "\"" + ProtoToString(operation.Id()) + "\"";
    auto printOperationInfo = [&]() {
        PrintOperation(operation, OutputFormat);
        const auto& metadata = operation.Metadata();
        Cout << "Script syntax: " << metadata.ScriptContent.Syntax << Endl;
        Cout << "Script text: \"" << metadata.ScriptContent.Text << "\"" << Endl;
    };
    switch (operation.Status().GetStatus()) {
        case EStatus::SUCCESS:
            printOperationInfo();
            if (operation.Ready()) {
                Cerr << "To fetch results: ydb sql-async fetch " << quotedId << Endl;
            } else {
                Cerr << "To wait for completion and fetch results: ydb sql-async fetch " << quotedId << Endl
                    << "To wait for completion only: ydb sql-async wait " << quotedId << Endl
                    << "To cancel operation: ydb sql-async cancel " << quotedId << Endl;
            }
            Cerr << "To forget operation: ydb sql-async forget " << quotedId << Endl;
            return EXIT_SUCCESS;
        case EStatus::CANCELLED:
            printOperationInfo();
            Cerr << "To forget operation: ydb sql-async forget " << quotedId << Endl;
            return EXIT_FAILURE;
        default:
            ThrowOnError(operation.Status());
            return EXIT_FAILURE;
    }
}

TCommandSqlAsyncWait::TCommandSqlAsyncWait()
    : TCommandWithScriptExecutionOperationId("wait", {}, "Wait for async script execution completion by operation id")
{}

int TCommandSqlAsyncWait::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    if (WaitForCompletion(client, OperationId)) {
        std::string quotedId = "\"" + ProtoToString(OperationId) + "\"";
        Cerr << "Operation completed." << Endl
            << "To fetch results: ydb sql-async fetch " << quotedId << Endl
            << "To forget operation: ydb sql-async forget " << quotedId << Endl;
        return EXIT_SUCCESS;
    } else {
        return EXIT_FAILURE;
    }
}

TCommandSqlAsyncCancel::TCommandSqlAsyncCancel()
    : TCommandWithScriptExecutionOperationId("cancel", {}, "Cancel async script execution by operation id")
{}

int TCommandSqlAsyncCancel::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    ThrowOnError(client.Cancel(OperationId).GetValueSync());
    Cerr << "Operation cancelled. To forget operation: ydb sql-async forget \""
        << ProtoToString(OperationId) << "\"" << Endl;
    return EXIT_SUCCESS;
}

TCommandSqlAsyncForget::TCommandSqlAsyncForget()
    : TCommandWithScriptExecutionOperationId("forget", {}, "Forget async script execution operation by id")
{}

int TCommandSqlAsyncForget::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    ThrowOnError(client.Forget(OperationId).GetValueSync());
    return EXIT_SUCCESS;
}

TCommandSqlAsyncList::TCommandSqlAsyncList()
    : TYdbCommand("list", {}, "List async script execution operations")
{}

void TCommandSqlAsyncList::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption('s', "page-size", "Page size")
        .RequiredArgument("NUM").StoreResult(&PageSize);
    config.Opts->AddLongOption('t', "page-token", "Page token")
        .RequiredArgument("STRING").StoreResult(&PageToken);
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });

    config.SetFreeArgsNum(0);
}

void TCommandSqlAsyncList::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseFormats();
}

int TCommandSqlAsyncList::Run(TConfig& config) {
    NOperation::TOperationClient client(CreateDriver(config));
    NOperation::TOperationsList<NQuery::TScriptExecutionOperation> operations = client.List<NQuery::TScriptExecutionOperation>(
        PageSize, PageToken)
        .GetValueSync();
    ThrowOnError(operations);
    PrintOperationsList(operations, OutputFormat);
    return EXIT_SUCCESS;
}


}
}
