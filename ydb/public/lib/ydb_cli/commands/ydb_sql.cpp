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

    config.SetFreeArgsNum(0);
}

void TCommandExecuteSqlBase::DeclareCommonInputOptions(TClientCommand::TConfig& config) {
    config.Opts->AddLongOption("stats", "Execution statistics collection mode [none, basic, full, profile]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption("syntax", "Query syntax [yql, pg]")
        .RequiredArgument("[String]").DefaultValue("yql").StoreResult(&Syntax)
        .Hidden();
    config.Opts->AddLongOption("results-ttl", "Amount of time to store query results on server. "
            "By default it is 86400s (24 hours).").RequiredArgument("SECONDS")
        .StoreResult(&ResultsTtl);
    config.SetFreeArgsNum(0);
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

    // Execute query without parameters
    auto asyncResult = queryClient.ExecuteScript(
        Query,
        settings
    );

    auto result = asyncResult.GetValueSync();
    ThrowOnError(result);
    if (AsyncWait) {
        NOperation::TOperationClient operationClient(driver);
        return WaitForResultAndPrintResponse(queryClient, operationClient, result.Id(), result);
    } else {
        return PrintResponse(result);
    }
}

int TCommandExecuteSqlBase::PrintResponse(const NQuery::TScriptExecutionOperation& result) {
    // TODO: replace id with actual id
    Cout << "Operation info:" << Endl << result.ToString() << Endl
        << "To wait for completion type \"ydb sql-async wait <id>\"" << Endl
        << "To fetch results type \"ydb sql-async fetch <id>\"";
    return EXIT_SUCCESS;
}

int TCommandExecuteSqlBase::WaitForResultAndPrintResponse(
        NQuery::TQueryClient& queryClient,
        NOperation::TOperationClient& operationClient,
        const TOperation::TOperationId& operationId,
        const NQuery::TScriptExecutionOperation& operation) {
    SetInterruptHandlers();
    // Throw exception on getting operation info if it is the first getOperation, then retry
    bool firstGetOperation = true;
    TWaitingBar waitingBar("Waiting for qeury execution to finish... ");
    int fakeCounter = 0;
    while (!IsInterrupted()) {
        NQuery::TScriptExecutionOperation execScriptOperation = firstGetOperation
            ? operation
            : operationClient
                .Get<NQuery::TScriptExecutionOperation>(operationId)
                .GetValueSync();
        if (firstGetOperation) {
            ThrowOnError(execScriptOperation.Status());
            firstGetOperation = false;
        }
        if (!execScriptOperation.Status().IsSuccess() || !execScriptOperation.Ready() || ++fakeCounter <= 5) {
            waitingBar.Render();
            Sleep(TDuration::Seconds(1));
            continue;
        }
        waitingBar.Finish(true);
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        for (size_t resultSetIndex = 0; resultSetIndex < execScriptOperation.Metadata().ResultSetsMeta.size(); ++resultSetIndex) {
            TString const* nextFetchToken = nullptr;
            while (!IsInterrupted()) {
                NYdb::NQuery::TFetchScriptResultsSettings settings;
                if (nextFetchToken != nullptr) {
                    settings.FetchToken(*nextFetchToken);
                }

                // TODO: fetch with retries
                auto asyncResult = queryClient.FetchScriptResults(operationId, resultSetIndex, settings);
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
        break;
    }
    waitingBar.Finish(false);

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
            return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

TCommandSql::TCommandSql()
    : TYdbCommand("sql", {}, "Execute SQL query")
{}

void TCommandSql::Config(TConfig& config) {
    TYdbCommand::Config(config);
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
            "To get operation status use \"ydb operation get\" command. "
            "To fetch query results use \"--fetch <operation_id>\" option of this command.\n"
            "Note: query results will be stored on server and thus consume storage resources.")
        .StoreTrue(&RunAsync);
    config.Opts->AddLongOption("async-wait",
            "Execute script (query) asynchronously and wait for results (using polling).\n"
            "The difference between using and not-using this option:\n"
            "  - If YDB CLI loses connection to the server when running a query without this option, "
            "the stream breaks and command execution finishes with error. "
            "If this option is used, the polling process continues and query results may still be received after reconnect.\n"
            "  - Using this option will probably reduce performance due to artifitial delays between polling requests.\n"
            "Note: query results will be stored on server and thus consume storage resources.")
        .StoreTrue(&AsyncWait);
    DeclareCommonInputOptions(config);
    DeclareCommonOutputOptions(config);
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
        return ExecuteQueryStream(client);
    }
}

int TCommandSql::ExecuteQueryStream(NQuery::TQueryClient& client) {
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
        throw TMisuseException() << "Invalid operation kind: expected SCRIPT_EXECUTION, but got "
            << OperationId.GetKind() << ".";
    }
}

NQuery::TScriptExecutionOperation TCommandWithScriptExecutionOperationId::GetOperation(TDriver& driver) {
    NOperation::TOperationClient operationClient(driver);
    NQuery::TScriptExecutionOperation execScriptOperation = operationClient
        .Get<NQuery::TScriptExecutionOperation>(OperationId)
        .GetValueSync();
    return execScriptOperation;
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
    try {
        OperationId = TOperationId(config.ParseResult->GetFreeArgs()[0]);
    } catch (const yexception& ex) {
        throw TMisuseException() << "Invalid operation ID";
    }
}

int TCommandSqlAsyncFetch::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient queryClient(driver);
    NOperation::TOperationClient operationClient(driver);
    auto operation = GetOperation(driver);
    return WaitForResultAndPrintResponse(queryClient, operationClient, OperationId, operation);
}



}
}
