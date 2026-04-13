#include "query_utils.h"
#include "common.h"
#include "progress_indication.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>
#include <util/string/escape.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

namespace NYdb::NConsoleClient {

TExplainGenericQuery::TExplainGenericQuery(const TDriver& driver)
    : Client(driver)
{}

TExplainGenericQuery::TResult TExplainGenericQuery::Explain(const TString& query, std::optional<TDuration> timeout, bool analyze) {
    NQuery::TExecuteQuerySettings settings;
    settings.ClientTimeout(timeout.value_or(TDuration()));

    if (analyze) {
        settings.StatsMode(NQuery::EStatsMode::Full);
    } else {
        settings.ExecMode(NQuery::EExecMode::Explain);
    }

    auto result = Client.StreamExecuteQuery(
        query,
        NQuery::TTxControl::BeginTx().CommitTx(),
        settings
    ).ExtractValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    SetInterruptHandlers();
    TResult explainResult;
    while (!IsInterrupted()) {
        auto tablePart = result.ReadNext().GetValueSync();
        if (ThrowOnErrorAndCheckEOS(tablePart)) {
            break;
        }
        if (tablePart.GetStats()) {
            auto proto = NYdb::TProtoAccessor::GetProto(*tablePart.GetStats());
            explainResult.PlanJson = proto.query_plan();
            explainResult.Ast = proto.query_ast();
        }
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
    }

    return explainResult;
}

TExecuteGenericQuery::TExecuteGenericQuery(const TDriver& driver)
    : Driver(driver)
    , Client(driver)
{}

int TExecuteGenericQuery::Execute(const TString& query, const TSettings& execSettings) {
    SetInterruptHandlers();

    auto asyncResult = StartQuery(query, execSettings);
    if (!WaitInterruptable(asyncResult)) {
        return EXIT_FAILURE;
    }

    auto result = asyncResult.GetValue();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return PrintResponse(result, query, execSettings);
}

void TExecuteGenericQuery::OnResultPart(ui64 resultSetIndex, const TResultSet& resultSet) {
    Y_UNUSED(resultSetIndex, resultSet);
}

NQuery::TAsyncExecuteQueryIterator TExecuteGenericQuery::StartQuery(const TString& query, const TSettings& execSettings) {
    if (execSettings.Parameters) {
        return Client.StreamExecuteQuery(
            query,
            NQuery::TTxControl::NoTx(),
            *execSettings.Parameters,
            execSettings.Settings
        );
    }

    return Client.StreamExecuteQuery(
        query,
        NQuery::TTxControl::NoTx(),
        execSettings.Settings
    );
}

int TExecuteGenericQuery::PrintResponse(NQuery::TExecuteQueryIterator& result, const TString& query, const TSettings& execSettings) {
    Y_DEFER {
        if (execSettings.AddIndent) {
            Cout << Endl;
        }
    };

    std::optional<std::string> stats;
    std::optional<std::string> plan;
    std::optional<std::string> ast;
    std::optional<std::string> meta;
    {
        bool printedSomething = false;
        ui64 currentResultSet = 0;
        TResultSetPrinter printer(execSettings.OutputFormat, &IsInterrupted);

        TProgressIndication progressIndication;
        TMaybe<NQuery::TExecStats> execStats;

        while (!IsInterrupted()) {
            auto asyncStreamPart = result.ReadNext();
            if (!WaitInterruptable(asyncStreamPart)) {
                return EXIT_FAILURE;
            }
            auto streamPart = asyncStreamPart.ExtractValue();
            if (ThrowOnErrorAndCheckEOS(streamPart)) {
                break;
            }

            if (streamPart.HasStats()) {
                execStats = streamPart.ExtractStats();

                const auto& protoStats = TProtoAccessor::GetProto(execStats.GetRef());
                for (const auto& queryPhase : protoStats.query_phases()) {
                    for (const auto& tableAccessStats : queryPhase.table_access()) {
                        progressIndication.UpdateProgress({tableAccessStats.reads().rows(), tableAccessStats.reads().bytes()});
                    }
                }
                progressIndication.SetDurationUs(protoStats.total_duration_us());

                progressIndication.Render();
            }

            if (streamPart.HasResultSet() && !execSettings.ExplainAnalyzeMode) {
                if (!printedSomething) {
                    if (execSettings.AddIndent) {
                        Cout << Endl;
                    }
                    printedSomething = true;
                }

                if (streamPart.GetResultSetIndex() != currentResultSet) {
                    currentResultSet = streamPart.GetResultSetIndex();
                    printer.Reset();
                }

                progressIndication.Finish();
                OnResultPart(streamPart.GetResultSetIndex(), streamPart.GetResultSet());
                printer.Print(streamPart.GetResultSet());
            }
        }

        if (execStats) {
            stats = execStats->ToString();
            plan = execStats->GetPlan();
            ast = execStats->GetAst();
            meta = execStats->GetMeta();
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (execSettings.ExplainAst) {
        Cout << "Query AST:" << Endl << ast << Endl;

        if (IsInterrupted()) {
            Cerr << "<INTERRUPTED>" << Endl;
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    if (stats && !execSettings.ExplainMode && !execSettings.ExplainAnalyzeMode) {
        Cout << Endl << "Statistics:" << Endl << *stats;
    }

    if (plan) {
        if (!execSettings.ExplainMode && !execSettings.ExplainAnalyzeMode
                && (execSettings.OutputFormat == EDataFormat::Default || execSettings.OutputFormat == EDataFormat::Pretty)) {
            Cout << Endl << "Execution plan:" << Endl;
        }
        // TODO: get rid of pretty-table format, refactor TQueryPrinter to reflect that
        EDataFormat format = (execSettings.OutputFormat == EDataFormat::Default || execSettings.OutputFormat == EDataFormat::Pretty)
            && (execSettings.ExplainMode || execSettings.ExplainAnalyzeMode)
            ? EDataFormat::PrettyTable : execSettings.OutputFormat;
        TQueryPlanPrinter queryPlanPrinter(format, /* show actual costs */ !execSettings.ExplainMode);
        queryPlanPrinter.Print(TString{*plan});
    }

    if (!execSettings.DiagnosticsFile.empty()) {
        TFileOutput file(execSettings.DiagnosticsFile);

        NJson::TJsonValue diagnosticsJson(NJson::JSON_MAP);

        if (stats) {
            diagnosticsJson.InsertValue("stats", *stats);
        }
        if (ast) {
            diagnosticsJson.InsertValue("ast", *ast);
        }
        if (plan) {
            NJson::TJsonValue planJson;
            NJson::ReadJsonTree(*plan, &planJson, true);
            diagnosticsJson.InsertValue("plan", planJson);
        }
        if (meta) {
            NJson::TJsonValue metaJson;
            NJson::ReadJsonTree(*meta, &metaJson, true);
            metaJson.InsertValue("query_text", EscapeC(query));
            diagnosticsJson.InsertValue("meta", metaJson);
        }
        file << NJson::PrettifyJson(NJson::WriteJson(diagnosticsJson, true), false);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
