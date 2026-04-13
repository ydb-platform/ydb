#include "exec_query_tool.h"
#include "tool_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/generic/scope.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TQueryRunner final : public TExecuteGenericQuery {
    using TBase = TExecuteGenericQuery;

public:
    explicit TQueryRunner(const TDriver& driver)
        : TBase(driver)
    {}

    NJson::TJsonValue ExtractResults() {
        NJson::TJsonValue result;
        std::swap(result, ResultSets);
        InittedResultSets.clear();
        return result;
    }

protected:
    void OnResultPart(ui64 resultSetIndex, const TResultSet& resultSet) final {
        auto& result = ResultSets[resultSetIndex];

        if (InittedResultSets.emplace(resultSetIndex).second) {
            const auto& columnMeta = resultSet.GetColumnsMeta();
            auto& columns = result["columns"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            for (const auto& column : columnMeta) {
                auto& item = columns.emplace_back();
                item["name"] = column.Name;
                item["type"] = column.Type.ToString();
            }
        }

        auto& rows = result["rows"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            try {
                TJsonParser row;
                Y_VALIDATE(row.Parse(FormatResultRowJson(parser, resultSet.GetColumnsMeta(), EBinaryStringEncoding::Unicode)), "Invalid serialized JSON row value");
                rows.emplace_back(row.GetValue());
            } catch (const std::exception& e) {
                YDB_CLI_LOG(Warning, "Error parsing result #" << resultSetIndex << " row #" << rows.size() << ": " << e.what());
                rows.emplace_back(TStringBuilder() << "Row conversion to JSON format failed: " << e.what() << ". Try to simplify result column types");
            }
        }
    }

private:
    NJson::TJsonValue ResultSets;
    std::unordered_set<ui64> InittedResultSets;
};

class TExecQueryTool final : public TToolBase, public TInterruptableCommand {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Execute query in Yandex Data Base (YDB) on YQL (SQL dialect). Use cases:
- Execute data query to fetch or modify data in database tables
- Execute DDL query to create new scheme entities e. g. tablas, topics e. t. c.

IMPORTANT:
- NEVER guess column names, types or keys. If you do not know the exact schema of a table, use the `describe` tool FIRST.
- NEVER guess data values for filtering. Do not assume specific values exist in the table.
  Instead, query the distinct values first (e.g., `SELECT DISTINCT column_name FROM my_table LIMIT 20`) to verify the actual values in the database.
- To get the schema of a table (columns, types, etc.), use the `describe` tool instead of this one.
- If path to table contains '/' or '@', wrap it into back ticks, for example `path/to/table`. Add back ticks only if they are really needed, for example table some_table do not need backticks.

Returns list of result sets for query, each contains list of rows and column metadata.
For example if there exists table 'my_table' with string column 'Data' and we execute query:
```
$filtered = SELECT * FROM my_table WHERE Data IS NOT NULL;
SELECT Data || "-first" FROM $filtered;
SELECT Data || "-second" FROM $filtered;
```
Tool will return:
[
    {
        "rows": [
            {"Data": "A-first"},
            {"Data": "B-first"}
        ],
        "columns": [
            {"name": "Data", "type": "string"}
        ]
    },
    {
        "rows": [
            {"Data": "A-second"},
            {"Data": "B-second"}
        ],
        "columns": [
            {"name": "Data", "type": "string"}
        ]
    }
])";

    static constexpr char QUERY_PROPERTY[] = "query";

    enum class EAction {
        Approve,
        Reject,
        Edit,
        Abort,
    };

    static EAction RunFtxuiActionDialog() {
        std::vector<TString> options = {
            "Approve execution",
            "Edit query",
            "Skip query (don't execute, let agent retry)",
            "Abort operation",
        };

        auto result = RunFtxuiMenu("Approve query execution?", options);
        if (!result) {
            return EAction::Abort;
        }

        switch (*result) {
            case 0: return EAction::Approve;
            case 1: return EAction::Edit;
            case 2: return EAction::Reject;
            case 3: return EAction::Abort;
            default: return EAction::Abort;
        }
    }

public:
    explicit TExecQueryTool(const TExecQueryToolSettings& settings)
        : TBase(CreateParametersSchema(), DESCRIPTION)
        , YQLHighlighter(MakeYQLHighlighter(GetColorSchema()))
        , Prompt(settings.Prompt)
        , Database(settings.Database)
        , Driver(settings.Driver)
        , ExecuteRunner(settings.Driver)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Query = Strip(parser.GetKey(QUERY_PROPERTY).GetString());
        UserMessage = "";
        IsSkipped = false;
    }

    bool AskPermissions() final {
        TColors colors;
        try {
            colors.resize(Query.size(), replxx::Replxx::Color::DEFAULT);
            YQLHighlighter->Apply(Query, colors);
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Warning, "Error highlighting query: " << e.what());
            colors.assign(Query.size(), replxx::Replxx::Color::DEFAULT);
        }

        YDB_CLI_LOG(Notice, "Agent wnt to execute query:\n" << PrintYqlHighlightAnsiColors(Query, colors));

        PrintFtxuiMessage(PrintYqlHighlightFtxuiColors(Query, colors), "Agent wants to execute query", ftxui::Color::Green);
        Cout << Endl;

        const auto action = RunFtxuiActionDialog();

        if (action == EAction::Abort) {
            Cout << "<Interrupted by user>" << Endl;
            throw yexception() << "Interrupted by user";
        }

        if (action == EAction::Edit) {
            if (RequestQueryText()) {
                return true;
            }
            IsSkipped = true;
            return true;
        }

        if (action == EAction::Approve) {
            return true;
        }

        Cout << Endl;

        IsSkipped = true;
        return true;
    }

    TResponse DoExecute() final {
        if (IsSkipped) {
            NJson::TJsonValue jsonResult;
            jsonResult["status"] = "skipped";
            return TResponse::Success(jsonResult, "User explicitly skipped execution of this query. The query was NOT executed. (Please continue in the primary language of the conversation)");
        }

        Y_DEFER { ResetInterrupted(); };

        try {
            if (ExecuteRunner.Execute(Query, {.AddIndent = true}) != EXIT_SUCCESS) {
                YDB_CLI_LOG(Notice, "Query execution was interrupted by user");
                return TResponse::Error(TStringBuilder() << "Query execution was interrupted by user", UserMessage);
            }
        } catch (const std::exception& e) {
            Cout << Colors.Red() << "Query execution failed:\n" << Colors.OldColor() << e.what() << Endl;
            return TResponse::Error(TStringBuilder() << "Query execution failed with error:\n" << e.what(), UserMessage);
        }

        return TResponse::Success(ExecuteRunner.ExtractResults(), UserMessage);
    }

private:
    bool RequestQueryText() {
        Cout << Endl;

        const auto lineReader = CreateLineReader({
            .Driver = Driver,
            .Database = Database,
            .Prompt = TStringBuilder() << Prompt << Colors.Yellow() << "YQL" << Colors.OldColor() << "> ",
            .EnableSwitchMode = false,
            .ContinueAfterCancel = false,
        });

        auto response = lineReader->ReadLine(Query);
        lineReader->Finish(false);
        if (!response) {
            Cout << Endl << "<Interrupted by user>" << Endl << Endl;
            return false;
        }

        Y_VALIDATE(std::holds_alternative<ILineReader::TLine>(*response), "Unexpected response alternative");
        TString newText = std::move(std::get<ILineReader::TLine>(*std::move(response)).Data);
        UserMessage = TStringBuilder()
            << "Query modified by user to:\n" << newText << "\n"
            << "(Results correspond to this new query. IGNORE this change notification in your response and proceed directly to analyzing the results.)";

        Query = std::move(newText);
        return true;
    }

    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Type(TJsonSchemaBuilder::EType::Object)
            .Property(QUERY_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Query to execute on YQL (SQL dialect), for example 'SELECT * FROM my_table'")
                .Done()
            .Build();
    }

private:
    const IYQLHighlighter::TPtr YQLHighlighter;
    const TString Prompt;
    const TString Database;
    const TDriver Driver;
    TQueryRunner ExecuteRunner;

    TString Query;
    TString UserMessage;
    bool IsSkipped = false;
};

} // anonymous namespace

ITool::TPtr CreateExecQueryTool(const TExecQueryToolSettings& settings) {
    return std::make_shared<TExecQueryTool>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
