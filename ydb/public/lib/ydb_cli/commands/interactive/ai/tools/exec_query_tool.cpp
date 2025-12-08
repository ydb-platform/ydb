#include "exec_query_tool.h"
#include "tool_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/yql_highlighter.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/query_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TQueryRunner final : public TExecuteGenericQuery {
    using TBase = TExecuteGenericQuery;

public:
    TQueryRunner(const TDriver& driver, const TInteractiveLogger& log)
        : TBase(driver)
        , Log(log)
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
                Log.Warning() << "Error parsing result #" << resultSetIndex << " row #" << rows.size() << ": " << e.what() << Endl;
                rows.emplace_back(TStringBuilder() << "Row conversion to JSON format failed: " << e.what() << ". Try to simplify result column types");
            }
        }
    }

private:
    const TInteractiveLogger Log;
    NJson::TJsonValue ResultSets;
    std::unordered_set<ui64> InittedResultSets;
};

class TExecQueryTool final : public TToolBase {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Execute query in Yandex Data Base (YDB) on YQL (SQL dialect). Use cases:
- Execute data query to fetch or modify data in database tables
- Execute DDL query to create new scheme entities e. g. tablas, topics e. t. c.
- Get scheme entity info by running `SHOW CREATE ...`

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
    };

public:
    TExecQueryTool(const TExecQueryToolSettings& settings, const TInteractiveLogger& log)
        : TBase(CreateParametersSchema(), DESCRIPTION, log)
        , YQLHighlighter(MakeYQLHighlighter(TColorSchema::Monaco()))
        , Prompt(settings.Prompt)
        , Database(settings.Database)
        , Driver(settings.Driver)
        , ExecuteRunner(settings.Driver, Log)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        TJsonParser parser(parameters);
        Query = Strip(parser.GetKey(QUERY_PROPERTY).GetString());
        UserMessage = "";
    }

    bool AskPermissions() final {
        TColors colors;
        try {
            colors.resize(Query.size(), replxx::Replxx::Color::DEFAULT);
            YQLHighlighter->Apply(Query, colors);
        } catch (const std::exception& e) {
            Log.Warning() << "Error highlighting query: " << e.what() << Endl;
            colors.assign(Query.size(), replxx::Replxx::Color::DEFAULT);
        }

        Cout << Endl << Colors.BoldColor() << "Agent wants to execute query:\n" << Colors.OldColor() << Endl << PrintAnsiColors(Query, colors) << Endl << Endl;

        EAction action = EAction::Reject;
        TString prompt = "Approve query execution? Type \"y\" (yes), \"n\" (no) or \"e\" (edit): ";
        AskInputWithPrompt(prompt, [&](const TString& input) {
            const auto choice = to_lower(Strip(input));
            if (!IsIn({"y", "yes", "n", "no", "e", "edit"}, choice)) {
                prompt = "Please type \"y\" (yes), \"n\" (no) or \"e\" (edit): ";
                return false;
            }

            if (choice == "y" || choice == "yes") {
                action = EAction::Approve;
            } else if (choice == "n" || choice == "no") {
                action = EAction::Reject;
            } else {
                action = EAction::Edit;
            }

            return true;
        }, Log.IsVerbose(), /* exitOnError */ false);

        if (action == EAction::Edit) {
            return RequestQueryText();
        }

        Cout << Endl;
        return action == EAction::Approve;
    }

    TResponse DoExecute() final {
        try {
            if (ExecuteRunner.Execute(Query, {}) != EXIT_SUCCESS) {
                Log.Notice() << "Query execution was interrupted by user";
                return TResponse(TStringBuilder() << "Query execution was interrupted by user", UserMessage);
            }
        } catch (const std::exception& e) {
            Cout << Colors.Red() << "Query execution failed:\n" << Colors.OldColor() << e.what() << Endl;
            return TResponse(TStringBuilder() << "Query execution failed with error:\n" << e.what(), UserMessage);
        }

        return TResponse(ExecuteRunner.ExtractResults(), UserMessage);
    }

private:
    bool RequestQueryText() {
        Cout << Endl;

        const auto lineReader = CreateLineReader({.Driver = Driver, .Database = Database, .ContinueAfterCancel = false}, Log);
        lineReader->Setup({
            .Prompt = TStringBuilder() << Prompt << Colors.Yellow() << "YQL" << Colors.OldColor() << "> ",
            .EnableSwitchMode = false,
        });

        auto response = lineReader->ReadLine(Query);
        lineReader->Finish();
        if (!response) {
            return false;
        }

        Y_VALIDATE(std::holds_alternative<ILineReader::TLine>(*response), "Unexpected response alternative");
        TString newText = std::move(std::get<ILineReader::TLine>(*std::move(response)).Data);
        UserMessage = TStringBuilder()
            << "I decided to change query text manually to:\n" << newText << "\nPrevious query text:\n" << Query
            << "\n I already get query results (they connected to your tool call), so there is no need to execute query twice, you can continue task";

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
};

} // anonymous namespace

ITool::TPtr CreateExecQueryTool(const TExecQueryToolSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<TExecQueryTool>(settings, log);
}

} // namespace NYdb::NConsoleClient::NAi
